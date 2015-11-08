// Copyright (C) 2014-2015 Andrey Shevtsov

// This program is free software; you can redistribute it and/or
// modify it under the terms of the GNU General Public License
// as published by the Free Software Foundation; either version 2
// of the License, or (at your option) any later version.

// This program is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU General Public License for more details.

// You should have received a copy of the GNU General Public License
// along with this program; if not, write to the Free Software
// Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA  02110-1301, USA.

using SqlWristband;
using SqlWristband.Config;
using SqlWristband.Data;

namespace SqlWristband.Probes
{
    using NLog;
    using System;
    using System.Collections.Generic;
    using System.Data.SqlClient;
    using System.Linq;

    public static class DataTypeMappingSqlServer
    {
        // mapping of internal data types (DataType) to SQL Server data types
        private static Dictionary<DataType, string[]> _mapping = new Dictionary<DataType, string[]>()
        {
            { DataType.Double,   new string[] { "float" } },
            { DataType.Ansi,     new string[] { "char", "nchar", "varchar", "nvarchar" } },
            { DataType.Unicode,  new string[] { "char", "nchar", "varchar", "nvarchar" } },
            { DataType.Datetime, new string[] { "datetime", "datetime2", "date" } },
            { DataType.SmallInt, new string[] { "tinyint", "smallint" } }
        };

        // checks whether passed value is allowed for the specified datatype
        public static bool DoesBelong(string value, DataType type)
        {
            return _mapping[type].Contains(value);
        }
    }

    public class SqlServerProbe : ProbeBase
    {
        private const ushort DATA_ROWS_INCREMENT = 10; // number of rows to pre-allocate in ProbeResultingData

        public SqlServerProbe(Configuration config)
        {
            _cfg = config;
            _logger = LogManager.GetLogger("SqlServerProbe");
        }

        // returns schema name
        public static string SchemaName(int targetId)
        {
            return "tgt" + targetId.ToString("D9");
        }

        // returns name of dictionary table
        public static string DictTableName(int targetId, MetricGroup metricGroup)
        {
            return SchemaName(targetId) + "." + metricGroup.dictTableName;
        }

        // returns name of data table
        public static string DataTableName(int targetId, MetricGroup metricGroup)
        {
            return SchemaName(targetId) + "." + metricGroup.dataTableName;
        }

        /// <summary>Converts DateTime to string in SqlServer format</summary>
        /// <param name="dt">DateTime</param>
        /// <returns>string in SqlServer format (yyyy-MM-dd HH:mm:ss)</returns>
        public static string DateTimeToString(DateTime dt)
        {
            return dt.ToString("yyyy-MM-dd HH:mm:ss");
        }

        /// <summary>Converts string into SQL Server datetime format (YYYY-MM-DD HH:SS)</summary>
        /// <param name="dt">string in YYYYMMDDHHMM format</param>
        /// <returns>String in SQL Server datetime format (YYYY-MM-DD HH:SS)</returns>
        public static string FormatDate(string dt)
        {
            return dt.Substring(0, 4) + "-" + dt.Substring(4, 2) + "-" + dt.Substring(6, 2) + " " + dt.Substring(8, 2) + ":" + dt.Substring(10, 2);
        }

        /// <summary>Returns SQL Server data type corresponding to internal data type</summary>
        /// <param name="type">Internal data type</param>
        /// <returns></returns>
        public static string ConvertDataTypeToString(DataType type)
        {
            switch (type)
            {
                case DataType.Double:
                    return "float(53)";
                case DataType.Ansi:
                    return "varchar(128)";
                case DataType.Unicode:
                    return "nvarchar(128)";
                case DataType.SmallInt:
                    return "smallint";
                case DataType.Datetime:
                    return "datetime2(0)";
                default:
                    throw new Exception("Unknown data type");
            }
        }

        /// <summary>Converts value of given internal data type to string that can be used in VALUES or WHERE part of SQL query</summary>
        /// <param name="type">Internal data type</param>
        /// <param name="value"></param>
        /// <returns>Data converted to string format and enquoted if needed</returns>
        public static string DataValueToString(DataType type, object value)
        {
            switch (type)
            {
                case DataType.Double:
                    return ((double)value).ToString();
                case DataType.Ansi:
                    return "'" + (string)value + "'";
                case DataType.Unicode:
                    return "'" + (string)value + "'";
                case DataType.Datetime:
                    return "'" + SqlServerProbe.DateTimeToString((DateTime)value) + "'";
                case DataType.SmallInt:
                    return ((short)value).ToString();
                default:
                    throw new Exception("Unknown data type");
            }
        }

        /// <summary>Executes non reader SQL statement. Creates tables on exception</summary>
        public static void ExecuteSql(string sqlStatement, int targetId, MetricGroup metricGroup)
        {
            int tryCount = 0;
            bool canExit = false;
            SqlConnection reposConn = null;
            SqlCommand cmd = null;

            /*
             * If tables don't exist, will try to create them and rerun SQL statements
             */
            while (!canExit && tryCount < 2)
            {
                try
                {
                    reposConn = new SqlConnection(Configuration.GetReposConnectionString("Probe"));

                    cmd = reposConn.CreateCommand();
                    cmd.CommandType = System.Data.CommandType.Text;
                    cmd.CommandText = sqlStatement;

                    reposConn.Open();
                    int rowCount = cmd.ExecuteNonQuery();
                    _logger.Debug("Rows affected: {0}", rowCount);

                    canExit = true;

                    reposConn.Close();
                } // end of try
                catch (SqlException e)
                {
                    if (e.Number == 208) // Invalid object
                    {
                        // Do not create tables if target has been deleted
                        if (!Configuration.targets.ContainsKey(targetId))
                            return;

                        SqlServerProbe.CreateTablesForMetricGroup(targetId, metricGroup);
                    }
                    else
                        _logger.Error("SqlException: " + e.Message + " ErrorCode: " + e.Number.ToString());
                } // end of catch
                finally
                {
                    if (cmd != null)
                        ((IDisposable)cmd).Dispose();

                    if (reposConn != null)
                        ((IDisposable)reposConn).Dispose();
                }

                tryCount++;
            } // end of while
        } // end of ExecuteSql function

        /// <summary>Creates tables in repository</summary>
        public static void CreateTablesForMetricGroup(int targetId, MetricGroup metricGroup)
        {
            bool schemaExists = true, dictTableExist = true, dataTableExists = true;

            string stmt = "";

            try
            {
                using (SqlConnection reposConn = new SqlConnection(Configuration.GetReposConnectionString("Probe")))
                {
                    reposConn.Open();

                    // check if schema exists
                    stmt = "SELECT COUNT(*) [count] FROM sys.schemas WHERE name = '" + SchemaName(targetId) + "'";
                    using (SqlCommand cmd = reposConn.CreateCommand())
                    {
                        cmd.CommandType = System.Data.CommandType.Text;
                        cmd.CommandText = stmt;
                        SqlDataReader dataReader = cmd.ExecuteReader();

                        if (!dataReader.Read())
                            throw new Exception("Something bizzare happened while checking if schema exists");

                        if ((int)dataReader["count"] == 0) // schema doesn't exist. Let's create it
                            schemaExists = false;

                        dataReader.Close();
                        dataReader.Dispose();
                    }

                    // create schema if it doesn't exist
                    if (!schemaExists)
                    {
                        stmt = "CREATE SCHEMA " + SchemaName(targetId) + " AUTHORIZATION dbo";
                        using (SqlCommand cmd = reposConn.CreateCommand())
                        {
                            cmd.CommandType = System.Data.CommandType.Text;
                            cmd.CommandText = stmt;
                            cmd.ExecuteNonQuery();
                        }
                    }

                    if (metricGroup.isMultiRow)
                    {
                        // check if dictionary exists
                        stmt = "SELECT COUNT(*) [count] FROM sys.tables WHERE schema_id = SCHEMA_ID('" + SchemaName(targetId) + "') AND name = '" + metricGroup.dictTableName + "'";
                        using (SqlCommand cmd = reposConn.CreateCommand())
                        {
                            cmd.CommandType = System.Data.CommandType.Text;
                            cmd.CommandText = stmt;

                            using (SqlDataReader dataReader = cmd.ExecuteReader())
                            {
                                if (!dataReader.Read())
                                    throw new Exception("Something bizzare happened while checking if dictionary table exists");

                                if ((int)dataReader["count"] == 0) // schema doesn't exist
                                    dictTableExist = false;

                                dataReader.Close();
                            }
                        }

                        if (!dictTableExist)
                        {
                            // create SEQUENCE for slow changing dictionary
                            if (metricGroup.multiRowKeyAttributesChangeSpeed == ChangeSpeed.Slow)
                            {
                                // create dedicated sequence for dictionary
                                stmt = "CREATE SEQUENCE " + SchemaName(targetId) + ".seq_" + metricGroup.dictTableName;
                                stmt += Environment.NewLine + "START WITH 1 INCREMENT BY 1 NO CYCLE CACHE 20";

                                using (SqlCommand cmd = reposConn.CreateCommand())
                                {
                                    cmd.CommandType = System.Data.CommandType.Text;
                                    cmd.CommandText = stmt;
                                    cmd.ExecuteNonQuery();
                                }
                            }

                            // create dictionary first
                            stmt = "CREATE TABLE " + SchemaName(targetId) + "." + metricGroup.dictTableName + " (" + Environment.NewLine;
                            stmt += "id int NOT NULL";

                            if (metricGroup.multiRowKeyAttributesChangeSpeed == ChangeSpeed.Static)
                                stmt += " IDENTITY(1,1),";
                            else
                                stmt += ",";

                            if (metricGroup.multiRowKeyAttributesChangeSpeed == ChangeSpeed.Slow)
                                stmt +=
                                    Environment.NewLine + "startDate datetime2(0) NOT NULL," +
                                    Environment.NewLine + "endDate datetime2(0) NULL,";

                            for (int i = 0; i < metricGroup.NumberOfMultiRowKeys; i++)
                            {
                                stmt += metricGroup.multiRowKeys[i].name.Replace(' ', '_') + " " + ConvertDataTypeToString(metricGroup.multiRowKeys[i].type) + " NOT NULL," + Environment.NewLine;
                            }

                            for (int i = 0; i < metricGroup.NumberOfMultiRowKeyAttributes; i++)
                            {
                                stmt += metricGroup.multiRowKeyAttributes[i].name.Replace(' ', '_') + " " + ConvertDataTypeToString(metricGroup.multiRowKeyAttributes[i].type) + " NULL," + Environment.NewLine;
                            }

                            stmt += "CONSTRAINT CIDX_" + SchemaName(targetId) + "_" + metricGroup.dictTableName + " PRIMARY KEY CLUSTERED (id";
                            if (metricGroup.multiRowKeyAttributesChangeSpeed == ChangeSpeed.Slow)
                                stmt += ",startDate";

                            stmt += ")," + Environment.NewLine;

                            stmt += "CONSTRAINT UQ_" + SchemaName(targetId) + "_" + metricGroup.dictTableName + " UNIQUE (";
                            
                            if (metricGroup.multiRowKeyAttributesChangeSpeed == ChangeSpeed.Slow)
                            {
                                stmt += Environment.NewLine + "startDate, ";
                            }

                            for (int i = 0; i < metricGroup.NumberOfMultiRowKeys; i++)
                            {
                                stmt += Environment.NewLine + metricGroup.multiRowKeys[i].name.Replace(' ', '_') + ", ";
                            }

                            stmt = Environment.NewLine + stmt.Remove(stmt.Length - 2) + ")" + Environment.NewLine + ")"; // remove last comma

                            using (SqlCommand cmd = reposConn.CreateCommand())
                            {
                                cmd.CommandType = System.Data.CommandType.Text;
                                cmd.CommandText = stmt;
                                cmd.ExecuteNonQuery();
                            }
                        } // end of if (!dictTableExist)
                    } // end of if (metricGroup.isMultiRow)

                    // check if data table exists
                    stmt = "SELECT COUNT(*) [count] FROM sys.tables WHERE schema_id = SCHEMA_ID('" + SchemaName(targetId) + "') AND name = '" + metricGroup.dataTableName + "'";
                    using (SqlCommand cmd = reposConn.CreateCommand())
                    {
                        cmd.CommandType = System.Data.CommandType.Text;
                        cmd.CommandText = stmt;
                        SqlDataReader dataReader = cmd.ExecuteReader();

                        if (!dataReader.Read())
                            throw new Exception("Something bizzare happened while checking if data table exists");

                        if ((int)dataReader["count"] == 0) // table doesn't exist
                            dataTableExists = false;

                        dataReader.Close();
                        dataReader.Dispose();
                    }

                    if (!dataTableExists)
                    {
                        // create data table
                        stmt = "CREATE TABLE " + SchemaName(targetId) + "." + metricGroup.dataTableName + " (" + Environment.NewLine;

                        if (metricGroup.changeSpeed != ChangeSpeed.Slow)
                            stmt += "dt datetime2(0) NOT NULL," + Environment.NewLine;

                        if (metricGroup.isMultiRow)
                            stmt += "dictId int NOT NULL," + Environment.NewLine;

                        for (int i = 0; i < metricGroup.NumberOfMetrics; i++)
                        {
                            stmt += metricGroup.metrics[i].name.Replace(' ', '_') + " " + ConvertDataTypeToString(metricGroup.metrics[i].type) + " NULL,";
                        }

                        if (metricGroup.changeSpeed == ChangeSpeed.Slow)
                            stmt += "startDate datetime2(0) NOT NULL, endDate datetime2(0) NOT NULL,";

                        stmt = stmt.Remove(stmt.Length - 1) + ")" + Environment.NewLine;

                        stmt += "CREATE CLUSTERED INDEX CIDX_" + SchemaName(targetId) + "_" + metricGroup.dataTableName + Environment.NewLine
                            + " ON " + SchemaName(targetId) + "." + metricGroup.dataTableName + " (";

                        if (metricGroup.changeSpeed == ChangeSpeed.Slow)
                            stmt += "startDate,";
                        else
                            stmt += "dt,";

                        if (metricGroup.isMultiRow)
                            stmt += " dictId,";

                        stmt = stmt.Remove(stmt.Length - 1) + ")";

                        _logger.Debug(stmt);

                        using (SqlCommand cmd = reposConn.CreateCommand())
                        {
                            cmd.CommandType = System.Data.CommandType.Text;
                            cmd.CommandText = stmt;
                            cmd.ExecuteNonQuery();
                        }
                    } // end of if (!dataTableExists)

                    reposConn.Close();
                } // end of using SqlConnection
            }
            catch (Exception e)
            {
                _logger.Error(e.Message);
                _logger.Error("SQL Statement: " + stmt);
                _logger.Error(e.StackTrace);
            }
        } // end of CreateTablesForMetricGroup method

        public override void Probe(int timeTableId, Target target, MetricGroup metricGroup)
        {
            bool exceptionCaught = false;
            SqlConnection con = null;
            SqlCommand cmd = null;
            DateTime openStarted = DateTime.Now;

            try
            {
                con = new SqlConnection();
                // connect to the target
                con.ConnectionString = ComposeConnectionString(target.id);

                cmd = con.CreateCommand();
                cmd.CommandType = System.Data.CommandType.Text;
                cmd.CommandText = metricGroup.scriptText;

                con.Open();
                var callback = new AsyncCallback(ProcessResults);
                cmd.BeginExecuteReader(callback, new ProbeResultingCallbackStateObject(timeTableId, target, metricGroup, cmd));
            }
            catch(SqlException e)
            {
                exceptionCaught = true;

                switch (e.Number)
                {
                    case -1:
                    case 53:
                        _logger.Warn("Could not connect to server [{0}], metric group [{1}]", target.name, metricGroup.name);
                        break;
                    default:
                        _logger.Error("Server [{0}], MetricGroup [{1}], Error [{2}]", target.name, metricGroup.name, e.Message);
                        break;
                }
            }
            catch (InvalidOperationException e)
            {
                exceptionCaught = true;
                // Skip real time outs
                DateTime now = DateTime.Now;
                if (now.Subtract(openStarted).Seconds >= con.ConnectionTimeout)
                {
                    _logger.Warn("Timeout expired. Server [{0}], metric group [{1}]", target.name, metricGroup.name);
                }
                else
                {
                    _logger.Error("Metric [" + metricGroup.name + "]: " + e.Message);
                    _logger.Error(e.StackTrace);
                }
            }
            catch (Exception e)
            {
                exceptionCaught = true;
                _logger.Error(e.Message);
                _logger.Error(e.StackTrace);
            }
            finally
            {
                if (exceptionCaught)
                {
                    if (con != null)
                    {
                        con.Close();
                        con.Dispose();
                    }
                }
            }
            // Sql* objects will be disposed in ProcessResults method
        } // end of function

        // this method is called when query execution is finished
        public override void ProcessResults(IAsyncResult result)
        {
            SqlCommand cmd = null;
            SqlConnection con = null;
            SqlDataReader dataReader = null;

            ushort rowsProcessed = 0;
            ushort numOfRows = 1;
            bool columnNamesMatch = true;

            try
            {
                // Retrieve state object
                var stateObj = (ProbeResultingCallbackStateObject)result.AsyncState;

                // Extract data from state object
                cmd = stateObj.SqlCommand;
                int timeTableId = stateObj.TimeTableId;
                Target target = stateObj.Target;
                MetricGroup metricGroup = stateObj.MetricGroup;

                con = cmd.Connection;
                dataReader = cmd.EndExecuteReader(result);

                // prepare ProbeResultingData
                if (metricGroup.isMultiRow)
                    numOfRows = DATA_ROWS_INCREMENT; // set initial size

                var data = new ProbeResultingData(numOfRows, (ushort)(metricGroup.NumberOfMultiRowKeys + metricGroup.NumberOfMultiRowKeyAttributes + metricGroup.NumberOfMetrics));
                data.SetProbeDateTime(DateTime.Now);

                for (int i = 0; i < metricGroup.NumberOfMultiRowKeys; i++)
                {
                    data.AddColumnHeader(metricGroup.multiRowKeys[i].name, metricGroup.multiRowKeys[i].type);
                }

                for (int i = 0; i < metricGroup.NumberOfMultiRowKeyAttributes; i++)
                {
                    data.AddColumnHeader(metricGroup.multiRowKeyAttributes[i].name, metricGroup.multiRowKeyAttributes[i].type);
                }

                for (int i = 0; i < metricGroup.NumberOfMetrics; i++)
                {
                    data.AddColumnHeader(metricGroup.metrics[i].name, metricGroup.metrics[i].type);
                }

                // read results and save in results object
                while (dataReader.Read() && columnNamesMatch)
                {
                    // check that column names match configuration
                    if (rowsProcessed == 0)
                    {
                        for (int i = 0; i < dataReader.FieldCount; i++)
                        {
                            if (i < metricGroup.NumberOfMultiRowKeys)
                            {
                                if (string.Compare(dataReader.GetName(i), metricGroup.multiRowKeys[i].name.Replace(' ', '_'), true) != 0)
                                {
                                    _logger.Error("Actual name of key column # " + i.ToString() + " [" + dataReader.GetName(i) + "] doesn't match to configuration [" + metricGroup.multiRowKeys[i].name.Replace(' ', '_') + "]");
                                    columnNamesMatch = false;
                                    break;
                                }
                            }
                            else if (i < metricGroup.NumberOfMultiRowKeys + metricGroup.NumberOfMultiRowKeyAttributes)
                            {
                                if (string.Compare(dataReader.GetName(i), metricGroup.multiRowKeyAttributes[i - metricGroup.NumberOfMultiRowKeys].name.Replace(' ', '_'), true) != 0)
                                {
                                    _logger.Error("Actual name of key attribute column # " + i.ToString() + " [" + dataReader.GetName(i) + "] doesn't match to configuration [" + metricGroup.multiRowKeyAttributes[i - metricGroup.NumberOfMultiRowKeys].name.Replace(' ', '_') + "]");
                                    columnNamesMatch = false;
                                    break;
                                }
                            }
                            else
                            {
                                if (string.Compare(dataReader.GetName(i), metricGroup.metrics[i - metricGroup.NumberOfMultiRowKeys - metricGroup.NumberOfMultiRowKeyAttributes].name.Replace(' ', '_'), true) != 0)
                                {
                                    _logger.Error("Actual name of column # " + i.ToString() + " [" + dataReader.GetName(i) + "] doesn't match to configuration [" + metricGroup.metrics[i - metricGroup.NumberOfMultiRowKeys - metricGroup.NumberOfMultiRowKeyAttributes].name.Replace(' ', '_') + "]");
                                    columnNamesMatch = false;
                                    break;
                                }
                            }
                        }
                    }

                    if (rowsProcessed == numOfRows)
                    {
                        numOfRows += DATA_ROWS_INCREMENT;
                        data.ChangeNumOfRows(numOfRows);
                    }

                    for (int i = 0; i < dataReader.FieldCount; i++)
                    {
                        // check data type before casting. Data type of a column returned by query may not match the one set up in Configuration
                        switch (data.dataTypes[i])
                        {
                            case DataType.Ansi:
                                if (!DataTypeMappingSqlServer.DoesBelong(dataReader.GetDataTypeName(i), DataType.Ansi))
                                    throw new Exception("Data type of column #" + (i + 1).ToString() + " of '" + metricGroup.name + "' metric (target [" + target.name + "]) does not match any allowed data type for internal data type Ansi");

                                data.values[rowsProcessed, i] = (object)dataReader.GetString(i);

                                break;
                            case DataType.Unicode:
                                if (!DataTypeMappingSqlServer.DoesBelong(dataReader.GetDataTypeName(i), DataType.Unicode))
                                    throw new Exception("Data type of column #" + (i + 1).ToString() + " of '" + metricGroup.name + "' metric (target [" + target.name + "]) does not match any allowed data type for internal data type Unicode");

                                data.values[rowsProcessed, i] = (object)dataReader.GetString(i);

                                break;
                            case DataType.Double:
                                if (!DataTypeMappingSqlServer.DoesBelong(dataReader.GetDataTypeName(i), DataType.Double))
                                    throw new Exception("Data type of column #" + (i + 1).ToString() + " of '" + metricGroup.name + "' metric (target [" + target.name + "]) does not match any allowed data type for internal data type Double");

                                data.values[rowsProcessed, i] = (object)dataReader.GetDouble(i);

                                break;
                            case DataType.SmallInt:
                                if (!DataTypeMappingSqlServer.DoesBelong(dataReader.GetDataTypeName(i), DataType.SmallInt))
                                    throw new Exception("Data type of column #" + (i + 1).ToString() + " of '" + metricGroup.name + "' metric (target [" + target.name + "]) does not match any allowed data type for internal data type Int16");

                                data.values[rowsProcessed, i] = (object)dataReader.GetInt16(i);

                                break;
                            case DataType.Datetime:
                                if (!DataTypeMappingSqlServer.DoesBelong(dataReader.GetDataTypeName(i), DataType.Datetime))
                                    throw new Exception("Data type of column #" + (i + 1).ToString() + " of '" + metricGroup.name + "' metric (target [" + target.name + "]) does not match any allowed data type for internal data type Datetime");

                                data.values[rowsProcessed, i] = (object)dataReader.GetDateTime(i);

                                break;
                            default:
                                throw new Exception("Unknown data type");
                        } // end of switch
                    } // end of for

                    rowsProcessed++;
                }

                // trim extra pre-allocated rows
                if (rowsProcessed != numOfRows)
                    data.ChangeNumOfRows(rowsProcessed);

                Configuration.timeTable.SetLastPoll(timeTableId, data.probeDateTime);

                // pass msg to Analyzer
                Analyzer.Enqueue(target, metricGroup, data);
            } // end of try
            catch (Exception e)
            {
                _logger.Error("SqlServerProbe.ProcessResults: " + e.Message);
                _logger.Error(e.StackTrace);
            }
            finally
            {
                if (dataReader != null)
                {
                    dataReader.Close();
                    dataReader.Dispose();
                }

                if (cmd != null)
                {
                    cmd.Dispose();
                }

                if (con != null)
                {
                    con.Close();
                    con.Dispose();
                }
            }
        } // end of ProcessResults method

        /// <summary>Composes SQL connection string for specified target</summary>
        /// <param name="targetId">Target id</param>
        private string ComposeConnectionString(int targetId)
        {
            var target = Configuration.targets[targetId];

            if (target.isSqlAuthentication)
            {
                return String.Format("Server={0};User Id={1};Password={2};Application Name=SQL Wristband - Probe",
                    target.name,
                    target.username,
                    Configuration.targets.GetPassword(targetId));
            }

            return String.Format("Server={0};Integrated Security=SSPI;Application Name=SQL Wristband - Probe", target.name);
        } // end of ComposeConnectionString function
    } // end of class
} // end of namespace
