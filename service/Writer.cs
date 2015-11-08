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

using SqlWristband.Config;
using SqlWristband.Data;
using SqlWristband.Probes;

namespace SqlWristband
{
    using NLog;
    using System;
    using System.Collections.Concurrent;
    using System.Collections.Generic;
    using System.Data.SqlClient;
    using System.Threading;

    /// <summary>Reads data from queue and writes it into repository</summary>
    public class Writer : IDisposable
    {
        #region variable declarations

        private static bool _shouldStop;
        private static Manager _mgr;
        private static Logger _logger = LogManager.GetLogger("Writer");
        private static Configuration _cfg;

        private static ConcurrentQueue<ProbeResultsDataMessage> _dataQueue;

        private SqlConnection reposConn;

        #endregion variable declarations

        #region public methods declarations

        // constructor
        public Writer(Manager manager, Configuration cfg)
        {
            if (_mgr == null)
                _mgr = manager;

            _cfg = cfg;

            _dataQueue = new ConcurrentQueue<ProbeResultsDataMessage>();
        }

        /// <summary>Enqueues data message. Gets called by Analyzer class</summary>
        /// <param name="timeTableId">Id of schedule in TimeTable</param>
        /// <param name="newData">packed metric data</param>
        public static void Enqueue(Target target, MetricGroup metricGroup, ProbeResultingData newData)
        {
            var msg = new ProbeResultsDataMessage(target, metricGroup, newData);
            _dataQueue.Enqueue(msg);
        }

        public void Dispose()
        {
            if (this.reposConn != null)
                this.reposConn.Dispose();
        }

        /// <summary>Stops execution of ProcessRequests. Async</summary>
        public void RequestStop()
        {
            _logger.Info("Stopping Writer");
            _shouldStop = true;
        }

        /// <summary>Runs continuously and processes messages in the queue</summary>
        public void ProcessQueue()
        {
            ProbeResultsDataMessage msg;

            _logger.Info("Writer started");

            this.reposConn = new SqlConnection(Configuration.GetReposConnectionString("Writer"));

            while (!_shouldStop)
            {
                try
                {
                    // this is to wait until the repository is ready to serve requests
                    while (!Manager.IsRepositoryAccessible)
                    {
                        Thread.Sleep(250);

                        if (_shouldStop)
                            break;
                    }

                    if (_shouldStop)
                        break;

                    if (this.reposConn.State != System.Data.ConnectionState.Open)
                        this.reposConn.Open();

                    while (_dataQueue.TryDequeue(out msg))
                    {
                        var data = msg.Data;
                        var target = msg.Target;
                        var metricGroup = msg.MetricGroup;

                        _logger.Debug("{0} in queue. Date of data being processed: {1}", _dataQueue.Count, data.probeDateTime);

                        if (metricGroup.isMultiRow)
                        {
                            WriteMultipleRowsToRepository(target.id, metricGroup, msg.Data);
                        }
                        else
                        {
                            switch (metricGroup.changeSpeed)
                            {
                                case ChangeSpeed.Slow:
                                    WriteSlowSingleRowToRepository(target.id, metricGroup, msg.Data);
                                    break;
                                case ChangeSpeed.Fast:
                                    WriteFastSingleRowToRepository(target.id, metricGroup, msg.Data);
                                    break;
                                default:
                                    throw new Exception("Unexpected change speed attribute. Metric: " + metricGroup.name + ". Target: " + target.name + ".");
                            }
                        }
                    }

                    Thread.Sleep(15000);
                }
                catch (Exception e)
                {
                    if (this.reposConn != null)
                    {
                        switch (this.reposConn.State)
                        {
                            case System.Data.ConnectionState.Broken:
                            case System.Data.ConnectionState.Closed:
                                Manager.SetRepositoryAccessibility(false);
                                break;
                            default:
                                _logger.Error(e.Message);
                                _logger.Error(e.StackTrace);
                                _mgr.ReportFailure("Writer");
                                return;
                        }
                    }
                    else
                    {
                        _logger.Error(e.Message);
                        _logger.Error(e.StackTrace);
                        _mgr.ReportFailure("Writer");
                        return;
                    }
                } // end of catch
            } // end of while(!_shouldStop)

            if (this.reposConn != null)
                ((IDisposable)this.reposConn).Dispose();

            _logger.Info("Writer stopped");
        } // end of ProcessQueue function

        #endregion public methods declarations

        #region private static methods declarations

        /// <summary>Generates UPDATE or INSERT statement for a single data row - slow changing metric</summary>
        private static string GenerateSqlSingleRowSlow(int targetId, MetricGroup metricGroup, int dataMatches, ProbeResultingData data)
        {
            string dataSqlStmt;

            if (dataMatches == 0) // just update endDate when current data matches one stored in repository
                dataSqlStmt = "UPDATE " + SqlServerProbe.DataTableName(targetId, metricGroup)
                    + " SET endDate = '" + SqlServerProbe.DateTimeToString(data.probeDateTime) + "'"
                    + " WHERE startDate = (SELECT MAX(startDate) FROM " + SqlServerProbe.DataTableName(targetId, metricGroup) + ")";
            else
            {
                if (metricGroup.NumberOfMetrics != data.NumberOfColumns)
                    throw new Exception("Number of metrics doesn't match number of columns in probe results");

                dataSqlStmt = "INSERT INTO " + SqlServerProbe.DataTableName(targetId, metricGroup) + " (";

                for (int i = 0; i < metricGroup.NumberOfMetrics; i++)
                {
                    dataSqlStmt += metricGroup.metrics[i].name.Replace(' ', '_') + ",";
                }

                dataSqlStmt += "startDate,endDate)"
                    + Environment.NewLine + "VALUES (";

                // add metric values
                for (int i = 0; i < metricGroup.NumberOfMetrics; i++)
                {
                    dataSqlStmt += SqlServerProbe.DataValueToString(metricGroup.metrics[i].type, data.values[0, metricGroup.NumberOfMultiRowKeys + metricGroup.NumberOfMultiRowKeyAttributes + i]) + ",";
                }

                // startDate,endDate
                dataSqlStmt += "'" + SqlServerProbe.DateTimeToString(data.probeDateTime) + "','" + SqlServerProbe.DateTimeToString(data.probeDateTime) + "')";
            }

            return dataSqlStmt;
        } // end of GenerateSlowSqlSingleRow function

        // Generates INSERT statement for a single data row - fast changing metric
        private static string GenerateSqlSingleRowFast(int targetId, MetricGroup metricGroup, ProbeResultingData data)
        {
            string dataSqlStmt = "INSERT INTO " + SqlServerProbe.DataTableName(targetId, metricGroup) + " (dt,";

            for (int i = 0; i < metricGroup.NumberOfMetrics; i++)
            {
                dataSqlStmt += metricGroup.metrics[i].name.Replace(' ', '_') + ",";
            }

            dataSqlStmt = dataSqlStmt.Remove(dataSqlStmt.Length - 1); // remove last comma
            dataSqlStmt += ")" + Environment.NewLine + "VALUES ('" + SqlServerProbe.DateTimeToString(data.probeDateTime) + "',";

            if (metricGroup.NumberOfMetrics != data.NumberOfColumns)
                throw new Exception("Number of metrics don't match number of columns in probe results");

            // add metric values
            for (int i = 0; i < metricGroup.NumberOfMetrics; i++)
            {
                dataSqlStmt += SqlServerProbe.DataValueToString(metricGroup.metrics[i].type, data.values[0, metricGroup.NumberOfMultiRowKeys + metricGroup.NumberOfMultiRowKeyAttributes + i]) + ",";
            }

            dataSqlStmt = dataSqlStmt.Remove(dataSqlStmt.Length - 1); // remove last comma
            dataSqlStmt += ")";

            return dataSqlStmt;
        } // end of GenerateFastSqlSingleRow function

        // Generates INSERT statement for a single data row - static dictionary
        private static string GenerateSqlStaticDict(int targetId, MetricGroup metricGroup, ProbeResultingData data, List<int> rowsNotInDict)
        {
            string sqlStmt = "INSERT INTO " + SqlServerProbe.DictTableName(targetId, metricGroup) + " (";
            for (int i = 0; i < metricGroup.NumberOfMultiRowKeys; i++)
                sqlStmt += Environment.NewLine + metricGroup.multiRowKeys[i].name.Replace(' ', '_') + ",";

            sqlStmt = sqlStmt.Remove(sqlStmt.Length - 1); // remove last comma
            sqlStmt += ")" + Environment.NewLine;

            sqlStmt += "VALUES " + Environment.NewLine;

            foreach (int i in rowsNotInDict)
            {
                sqlStmt += "(";
                for (int j = 0; j < metricGroup.NumberOfMultiRowKeys; j++)
                {
                    sqlStmt += SqlServerProbe.DataValueToString(metricGroup.multiRowKeys[j].type, data.values[i, j]) + ",";
                }

                sqlStmt = sqlStmt.Remove(sqlStmt.Length - 1); // remove last comma
                sqlStmt += "),";
            }

            sqlStmt = sqlStmt.Remove(sqlStmt.Length - 1); // remove last comma

            return sqlStmt;
        } // end of GenerateSqlStaticDict method

        // Generates UPDATE statement for closing records and INSERT statement for openning records
        private static string GenerateSqlSlowDict(int targetId, MetricGroup metricGroup, ProbeResultingData data, List<Tuple<int, int>> rowsChanged, List<int> rowsNotInDict)
        {
            CacheTable dict = Configuration.inMemoryCache[InMemoryCache.GetCacheKey(targetId, metricGroup, CacheType.Dictionary)];
            string sqlStmt = string.Empty;

            // old rows where endDate need to be updated
            if (rowsChanged.Count > 0)
            {
                sqlStmt = "UPDATE " + SqlServerProbe.DictTableName(targetId, metricGroup) + Environment.NewLine;
                sqlStmt += "SET endDate = '" + SqlServerProbe.DateTimeToString(data.probeDateTime) + "'" + Environment.NewLine;
                sqlStmt += "WHERE";

                foreach (Tuple<int, int> ids in rowsChanged)
                {
                    sqlStmt += Environment.NewLine + "(id = " + ids.Item2.ToString() + " AND ";
                    sqlStmt += "startDate = '" + SqlServerProbe.DateTimeToString((DateTime)dict[ids.Item2][metricGroup.NumberOfMultiRowKeys + metricGroup.NumberOfMultiRowKeyAttributes]) + "')";
                    sqlStmt += " OR ";
                }

                sqlStmt = sqlStmt.Remove(sqlStmt.Length - 4); // remove last ' OR '
                sqlStmt += ";" + Environment.NewLine;

                // new records for changed rows
                sqlStmt += "INSERT INTO " + SqlServerProbe.DictTableName(targetId, metricGroup) + " (id,";
                for (int i = 0; i < metricGroup.NumberOfMultiRowKeys; i++)
                    sqlStmt += Environment.NewLine + metricGroup.multiRowKeys[i].name.Replace(' ', '_') + ",";

                for (int i = 0; i < metricGroup.NumberOfMultiRowKeyAttributes; i++)
                    sqlStmt += Environment.NewLine + metricGroup.multiRowKeyAttributes[i].name.Replace(' ', '_') + ",";

                sqlStmt += "startDate,endDate)" + Environment.NewLine;
                sqlStmt += "VALUES " + Environment.NewLine;

                foreach (Tuple<int, int> ids in rowsChanged)
                {
                    sqlStmt += "(" + ids.Item2.ToString() + ",";
                    for (int j = 0; j < metricGroup.NumberOfMultiRowKeys; j++)
                    {
                        sqlStmt += SqlServerProbe.DataValueToString(metricGroup.multiRowKeys[j].type, data.values[ids.Item1, j]) + ",";
                    }

                    for (int j = 0; j < metricGroup.NumberOfMultiRowKeyAttributes; j++)
                    {
                        sqlStmt += SqlServerProbe.DataValueToString(metricGroup.multiRowKeyAttributes[j].type, data.values[ids.Item1, metricGroup.NumberOfMultiRowKeys + j]) + ",";
                    }

                    // add startDate and endDate
                    sqlStmt += "'" + SqlServerProbe.DateTimeToString(data.probeDateTime) + "',NULL),";
                }

                sqlStmt = sqlStmt.Remove(sqlStmt.Length - 1); // remove last comma
            }

            // new rows
            if (rowsNotInDict.Count > 0)
            {
                sqlStmt += "INSERT INTO " + SqlServerProbe.DictTableName(targetId, metricGroup) + " (id,";
                for (int i = 0; i < metricGroup.NumberOfMultiRowKeys; i++)
                    sqlStmt += Environment.NewLine + metricGroup.multiRowKeys[i].name.Replace(' ', '_') + ",";

                for (int i = 0; i < metricGroup.NumberOfMultiRowKeyAttributes; i++)
                    sqlStmt += Environment.NewLine + metricGroup.multiRowKeyAttributes[i].name.Replace(' ', '_') + ",";

                sqlStmt += "startDate,endDate)" + Environment.NewLine;
                sqlStmt += "VALUES " + Environment.NewLine;

                foreach (int i in rowsNotInDict)
                {
                    sqlStmt += "(NEXT VALUE FOR " + SqlServerProbe.SchemaName(targetId) + ".seq_" + metricGroup.dictTableName + ",";
                    for (int j = 0; j < metricGroup.NumberOfMultiRowKeys; j++)
                    {
                        sqlStmt += SqlServerProbe.DataValueToString(metricGroup.multiRowKeys[j].type, data.values[i, j]) + ",";
                    }

                    for (int j = 0; j < metricGroup.NumberOfMultiRowKeyAttributes; j++)
                    {
                        sqlStmt += SqlServerProbe.DataValueToString(metricGroup.multiRowKeyAttributes[j].type, data.values[i, metricGroup.NumberOfMultiRowKeys + j]) + ",";
                    }

                    // add startDate and endDate
                    sqlStmt += "'" + SqlServerProbe.DateTimeToString(data.probeDateTime) + "',NULL),";
                }

                sqlStmt = sqlStmt.Remove(sqlStmt.Length - 1); // remove last comma
            }

            return sqlStmt;
        } // end of GenerateSqlSlowDict method

        #endregion private static methods declarations

        #region private methods declarations

        /// <summary>Saves single row & slow changing metric</summary>
        private void WriteSlowSingleRowToRepository(int targetId, MetricGroup metricGroup, ProbeResultingData data)
        {
            int dataMatches;
            string dataSqlStmt;
            object[] newValues;

            // compare with in-memory data
            dataMatches = this.CompareSlowSingleRowWithInMemoryData(targetId, metricGroup, data, this.reposConn);

            // generate SQL statement
            dataSqlStmt = GenerateSqlSingleRowSlow(targetId, metricGroup, dataMatches, data);
            _logger.Trace(dataSqlStmt);

            SqlServerProbe.ExecuteSql(dataSqlStmt, targetId, metricGroup);

            // update in-memory data
            newValues = new object[data.NumberOfColumns];
            for (int i = 0; i < data.NumberOfColumns; i++)
                newValues[i] = data.values[0, i];

            if (dataMatches == -1)
                InMemoryCache.Add(InMemoryCache.GetCacheKey(targetId, metricGroup), -1, new object[] { targetId }, newValues);
            else
                Configuration.inMemoryCache[InMemoryCache.GetCacheKey(targetId, metricGroup)].UpdateRowValues(new object[] { targetId }, newValues);
        } // end of WriteSlowSingleRowToRepository function

        /// <summary>Saves single row & fast changing metric</summary>
        private void WriteFastSingleRowToRepository(int targetId, MetricGroup metricGroup, ProbeResultingData data)
        {
            // generate SQL statement
            string dataSqlStmt = GenerateSqlSingleRowFast(targetId, metricGroup, data);
            _logger.Trace(dataSqlStmt);

            SqlServerProbe.ExecuteSql(dataSqlStmt, targetId, metricGroup);

            // update in-memory data
            object[] newValues = new object[data.NumberOfColumns];
            for (int i = 0; i < data.NumberOfColumns; i++)
                newValues[i] = data.values[0, i];

            // create in-memory cache table if it doesn't exist
            string cacheKey = InMemoryCache.GetCacheKey(-1, metricGroup);
            if (!InMemoryCache.ContainsKey(cacheKey))
            {
                // Do no create new cache table if target has been deleted
                if (!Configuration.targets.ContainsKey(targetId))
                    return;

                InMemoryCache.CreateCacheTableSingleRow(metricGroup, CacheType.Data);
            }

            Configuration.inMemoryCache[cacheKey].AddOrUpdateRowValues(-1, new object[] { targetId }, newValues);
        } // end of WriteFastSingleRowToRepository function

        /// <summary>Saves data into dictionary and data table for multi-value metrics</summary>
        private void WriteMultipleRowsToRepository(int targetId, MetricGroup metricGroup, ProbeResultingData data)
        {
            int id;
            CacheTable dictCache, dataCache;
            List<int> newDictRows;
            List<Tuple<int, int>> oldDictRows;
            object[] key, attributes;
            string dataTableName, dictTableName;

            byte tryCount = 0;
            bool canExit = false;
            SqlTransaction tran = null;
            string dictSqlStmt = string.Empty;
            string dataSqlStmt = string.Empty;

            newDictRows = new List<int>(); // ids of records that should be added to the dictionary (new rows or rows with updated attributes)
            oldDictRows = new List<Tuple<int, int>>(); // ids and dictionary ids of records that changed since last probe and need to be closed

            dataTableName = SqlServerProbe.DataTableName(targetId, metricGroup);
            _logger.Debug("Name of data table: " + dataTableName);

            dictTableName = SqlServerProbe.DictTableName(targetId, metricGroup);
            _logger.Debug("Name of dictionary: " + dictTableName);

            // load the dictionary cache table if it doesn't exist
            if (!InMemoryCache.ContainsKey(dictTableName))
                InMemoryCache.LoadDictionaryIntoCache(targetId, metricGroup, false);

            dictCache = Configuration.inMemoryCache[dictTableName];

            // load the dictionary cache table if it doesn't exist
            if (!InMemoryCache.ContainsKey(dataTableName))
                InMemoryCache.LoadDataIntoCache(targetId, metricGroup, false);

            /*
             * Checks for changed or new records in dictionary and if needed prepares SQL statement to update dictionary table
             */
            switch (metricGroup.multiRowKeyAttributesChangeSpeed)
            {
                case ChangeSpeed.Static:
                    // check whether all records are in the dictionary or some need to be added to it
                    for (int i = 0; i < data.NumberOfRows; i++)
                    {
                        key = new object[metricGroup.NumberOfMultiRowKeys];

                        for (int j = 0; j < metricGroup.NumberOfMultiRowKeys; j++)
                            key[j] = data.values[i, j];

                        if (dictCache.GetIdByKey(key) == -1)
                            newDictRows.Add(i);
                    }

                    // generate SQL statements if there are any new dictionary records
                    if (newDictRows.Count > 0)
                        dictSqlStmt = GenerateSqlStaticDict(targetId, metricGroup, data, newDictRows);

                    break;
                case ChangeSpeed.Slow:
                    // check whether all records are in the dictionary or some need to be added to it
                    for (int i = 0; i < data.NumberOfRows; i++)
                    {
                        key = new object[metricGroup.NumberOfMultiRowKeys];
                        for (int j = 0; j < metricGroup.NumberOfMultiRowKeys; j++)
                            key[j] = data.values[i, j];

                        id = dictCache.GetIdByKey(key);
                        if (id == -1)
                            newDictRows.Add(i);
                        else // check that attributes match
                        {
                            attributes = new object[metricGroup.NumberOfMultiRowKeyAttributes];
                            for (int j = 0; j < metricGroup.NumberOfMultiRowKeyAttributes; j++)
                                attributes[j] = data.values[i, metricGroup.NumberOfMultiRowKeys + j];

                            if (!dictCache.CompareAttributesForKey(id, attributes))
                                oldDictRows.Add(new Tuple<int, int>(i, id)); // this is to close the old record - UPDATE
                        }
                    }

                    // generate SQL statements if there are any changes or new records in dictionary
                    if (oldDictRows.Count > 0 || newDictRows.Count > 0)
                        dictSqlStmt = GenerateSqlSlowDict(targetId, metricGroup, data, oldDictRows, newDictRows);

                    break;
                default:
                    throw new Exception("Unknown dictionary change speed");
            }

            /*
             * Write new data into dictionary but don't close transaction yet
             */
            if (dictSqlStmt.CompareTo(string.Empty) != 0)
            {
                _logger.Trace(dictSqlStmt);

                // If tables don't exist, will try to create them and rerun SQL statements
                while (!canExit && tryCount < 2)
                {
                    try
                    {
                        // we will write to the dictionary first and then to the data table so we need to begin a transaction
                        tran = this.reposConn.BeginTransaction();

                        if (dictSqlStmt.CompareTo(string.Empty) != 0)
                        {
                            // save dictionary changes
                            using (SqlCommand cmd = this.reposConn.CreateCommand())
                            {
                                cmd.Transaction = tran;
                                cmd.CommandType = System.Data.CommandType.Text;
                                cmd.CommandText = dictSqlStmt;
                                int rowCount = cmd.ExecuteNonQuery();
                                _logger.Debug("Rows affected: " + rowCount.ToString());
                            }
                        }

                        InMemoryCache.LoadDictionaryIntoCache(targetId, metricGroup, true, this.reposConn, tran);
                        canExit = true;
                    }
                    catch (SqlException e)
                    {
                        if (tran != null)
                        {
                            tran.Rollback();
                            tran.Dispose();
                            tran = null;
                        }

                        switch (e.Number)
                        {
                            case 208: // Invalid object
                                // Do not create tables if target has been deleted
                                if (!Configuration.targets.ContainsKey(targetId))
                                    return;

                                SqlServerProbe.CreateTablesForMetricGroup(targetId, metricGroup);
                                break;
                            default:
                                _logger.Error("SqlException: " + e.Message + " ErrorCode: " + e.Number.ToString());
                                break;
                        }
                    }

                    tryCount++;
                }
            }

            /*
             * Prepare SQL statement to save data with right references to the dictionary records
             */
            switch (metricGroup.changeSpeed)
            {
                case ChangeSpeed.Fast:
                    dataSqlStmt = "INSERT INTO " + dataTableName + " (dt,dictId,";

                    for (int i = 0; i < metricGroup.NumberOfMetrics; i++)
                    {
                        dataSqlStmt += metricGroup.metrics[i].name.Replace(' ', '_') + ",";
                    }

                    dataSqlStmt = dataSqlStmt.Remove(dataSqlStmt.Length - 1); // remove last comma
                    dataSqlStmt += ")" + Environment.NewLine + "VALUES";

                    for (int i = 0; i < data.NumberOfRows; i++)
                    {
                        dataSqlStmt += Environment.NewLine + "('" + SqlServerProbe.DateTimeToString(data.probeDateTime) + "',";

                        // retrieve corresponding id from dictionary
                        key = new object[metricGroup.NumberOfMultiRowKeys];

                        for (int k = 0; k < metricGroup.NumberOfMultiRowKeys; k++)
                        {
                            key[k] = data.values[i, k];
                        }

                        id = dictCache.GetIdByKey(key);
                        dataSqlStmt += id.ToString() + ",";

                        // add metric values
                        for (int j = 0; j < metricGroup.NumberOfMetrics; j++)
                        {
                            dataSqlStmt += SqlServerProbe.DataValueToString(metricGroup.metrics[j].type, data.values[i, metricGroup.NumberOfMultiRowKeys + metricGroup.NumberOfMultiRowKeyAttributes + j]) + ",";
                        }

                        dataSqlStmt = dataSqlStmt.Remove(dataSqlStmt.Length - 1); // remove last comma
                        dataSqlStmt += "),";
                    }

                    dataSqlStmt = dataSqlStmt.Remove(dataSqlStmt.Length - 1); // remove last comma
                    _logger.Trace(dataSqlStmt);
                    break;
                default:
                    throw new Exception("Unsupported data change speed");
            }

            /*
             * Executes SQL statements
             * If tables don't exist, will try to create them and rerun SQL statements
             */
            try
            {
                // save data
                using (SqlCommand cmd = this.reposConn.CreateCommand())
                {
                    if (tran != null) // use same transaction as for the dictionary
                        cmd.Transaction = tran;

                    cmd.CommandType = System.Data.CommandType.Text;
                    cmd.CommandText = dataSqlStmt;
                    int rowCount = cmd.ExecuteNonQuery();
                    _logger.Debug("Rows affected: " + rowCount.ToString());
                }

                if (tran != null)
                    tran.Commit();

                InMemoryCache.LoadDictionaryIntoCache(targetId, metricGroup, true);
                dictCache = Configuration.inMemoryCache[dictTableName];

                // Update in-memory data cache
                object[] newValues;

                dataCache = Configuration.inMemoryCache[dataTableName];

                for (int i = 0; i < data.NumberOfRows; i++)
                {
                    key = new object[metricGroup.NumberOfMultiRowKeys];

                    for (int j = 0; j < metricGroup.NumberOfMultiRowKeys; j++)
                    {
                        key[j] = data.values[i, j];
                    }

                    id = dictCache.GetIdByKey(key);

                    newValues = new object[metricGroup.NumberOfMetrics];

                    for (int j = 0; j < metricGroup.NumberOfMetrics; j++)
                    {
                        newValues[j] = data.values[i, metricGroup.NumberOfMultiRowKeys + metricGroup.NumberOfMultiRowKeyAttributes + j];
                    }

                    dataCache.AddOrUpdateRowValues(id, new object[0], newValues);
                }

                canExit = true;
            }
            catch (SqlException e)
            {
                _logger.Error("SqlException: " + e.Message + " ErrorCode: " + e.Number.ToString());
                if (tran != null)
                {
                    tran.Rollback();
                    InMemoryCache.LoadDictionaryIntoCache(targetId, metricGroup, true);
                }
            }
        }

        /// <summary>Returns true if new data matches in-memory copy or no history is found - single row - slow changing</summary>
        private int CompareSlowSingleRowWithInMemoryData(int targetId, MetricGroup metricGroup, ProbeResultingData data, SqlConnection connection)
        {
            bool noHistory = false;
            CacheTable dataCache;

            // create in-memory cache table if it doesn't exist
            if (!InMemoryCache.ContainsKey(InMemoryCache.GetCacheKey(-1, metricGroup)))
            {
                // Do not create tables if target has been deleted
                if (!Configuration.targets.ContainsKey(targetId))
                    return -1;

                InMemoryCache.CreateCacheTableSingleRow(metricGroup, CacheType.Data);
            }

            dataCache = Configuration.inMemoryCache[InMemoryCache.GetCacheKey(-1, metricGroup, CacheType.Data)];
            // load latest row from the repository if it is not in-memory yet
            int id = dataCache.GetIdByKey(new object[] { targetId });
            if (id == -1)
            {
                string sqlStmt = "SELECT ";

                for (int i = 0; i < metricGroup.NumberOfMetrics; i++)
                {
                    sqlStmt += metricGroup.metrics[i].name.Replace(' ', '_') + ",";
                }

                sqlStmt = sqlStmt.Remove(sqlStmt.Length - 1); // remove last comma
                sqlStmt += " FROM " + SqlServerProbe.DataTableName(targetId, metricGroup);
                sqlStmt += " WHERE startDate = (SELECT MAX(startDate) FROM " + SqlServerProbe.DataTableName(targetId, metricGroup) + ")";

                using (SqlCommand cmd = connection.CreateCommand())
                {
                    cmd.CommandType = System.Data.CommandType.Text;
                    cmd.CommandText = sqlStmt;

                    try
                    {
                        SqlDataReader dataReader = cmd.ExecuteReader();

                        if (dataReader.Read())
                        {
                            object[] oldValues = new object[metricGroup.NumberOfMetrics];

                            for (int i = 0; i < metricGroup.NumberOfMetrics; i++)
                            {
                                // check data type before casting
                                switch (metricGroup.metrics[i].type)
                                {
                                    case DataType.Ansi:
                                        if (!DataTypeMappingSqlServer.DoesBelong(dataReader.GetDataTypeName(i), DataType.Ansi))
                                            throw new Exception("Data type of column #" + (i + 1).ToString() + " of '" + metricGroup.name + "' metric does not match any allowed data type for internal data type Ansi");

                                        oldValues[i] = (object)dataReader.GetString(i);

                                        break;
                                    case DataType.Unicode:
                                        if (!DataTypeMappingSqlServer.DoesBelong(dataReader.GetDataTypeName(i), DataType.Unicode))
                                            throw new Exception("Data type of column #" + (i + 1).ToString() + " of '" + metricGroup.name + "' metric does not match any allowed data type for internal data type Unicode");

                                        oldValues[i] = (object)dataReader.GetString(i);

                                        break;
                                    case DataType.Double:
                                        if (!DataTypeMappingSqlServer.DoesBelong(dataReader.GetDataTypeName(i), DataType.Double))
                                            throw new Exception("Data type of column #" + (i + 1).ToString() + " of '" + metricGroup.name + "' metric does not match any allowed data type for internal data type Double");

                                        oldValues[i] = (object)dataReader.GetDouble(i);

                                        break;
                                    case DataType.SmallInt:
                                        if (!DataTypeMappingSqlServer.DoesBelong(dataReader.GetDataTypeName(i), DataType.SmallInt))
                                            throw new Exception("Data type of column #" + (i + 1).ToString() + " of '" + metricGroup.name + "' metric does not match any allowed data type for internal data type Int16");

                                        oldValues[i] = (object)dataReader.GetInt16(i);

                                        break;
                                    case DataType.Datetime:
                                        if (!DataTypeMappingSqlServer.DoesBelong(dataReader.GetDataTypeName(i), DataType.Datetime))
                                            throw new Exception("Data type of column #" + (i + 1).ToString() + " of '" + metricGroup.name + "' metric does not match any allowed data type for internal data type Datetime");

                                        oldValues[i] = (object)dataReader.GetDateTime(i);

                                        break;
                                    default:
                                        throw new Exception("Unknown data type");
                                } // end of switch
                                i++;
                            }

                            id = dataCache.Add(-1, new object[] { targetId }, oldValues);
                        }
                        else
                            noHistory = true;

                        dataReader.Close();
                        dataReader.Dispose();
                    }
                    catch (SqlException e)
                    {
                        if (e.Number == 208) // Invalid object
                        {
                            // Do not create tables if target has been deleted
                            if (!Configuration.targets.ContainsKey(targetId))
                                return -1;

                            SqlServerProbe.CreateTablesForMetricGroup(targetId, metricGroup);
                            noHistory = true;
                        }
                        else
                            throw;
                    }
                }

                if (noHistory)
                    return -1;
            }

            // compare old and new values
            object[] newValues = new object[metricGroup.NumberOfMetrics];
            for (int i = 0; i < metricGroup.NumberOfMetrics; i++)
                newValues[i] = data.values[0, i];

            if (dataCache.CompareAttributesForKey(id, newValues))
                return 0;
            else
                return 1;
        }

        #endregion private methods declarations
    } // end of Writer class
}
