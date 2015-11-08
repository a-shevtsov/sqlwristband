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

using SqlWristband.Data;
using SqlWristband.Probes;

namespace SqlWristband.Config
{
    using Microsoft.Win32;
    using NLog;
    using System;
    using System.Collections.Generic;
    using System.Configuration;
    using System.Data;
    using System.Data.SqlClient;

    public enum ChangeSpeed { NotApplicable, Static, Slow, Fast }

    public class Configuration
    {
        #region variable declarations

        private static Manager _mgr;
        private static Logger _logger = LogManager.GetLogger("Configuration");

        // web server (static content) will be listening on wsProtocol://wsHostname:wsPort/
        // web services will be listening on wsProtocol://wsHostname:wsPort/wsPrefix/
        public static string wsProtocol;
        public static string wsHostname;
        public static string wsPort;
        public static string wsPrefix;
        public static string wsRootFolder;

        // Repository related 
        public static string reposInstance;
        public static string reposDatabase;
        public static int reposReconnectRetryInterval;

        // FullSchedule
        public static TimeTable timeTable;

        // Purger
        public static int purgeInterval; // in minutes

        // contains mapping of web service module names to corresponding class names (classes based on BaseRequestProcessor)
        public static Dictionary<string, string> moduleClassMapping;

        public static ProbeCollection probes;

        // contains descriptions of groups of metrics (single and multi probes)
        public static MetricGroupCollection metricGroups;

        public static TargetCollection targets;

        public static ArchiveOffsetCollection archiveOffsets;

        public static ArchiveWatermarkCollection archiveWatermarks;

        public static InMemoryCache inMemoryCache;

        #endregion variable declarations

        #region public method declarations

        public Configuration(Manager manager)
        {
            _mgr = manager;

            inMemoryCache = new InMemoryCache();
            timeTable = new TimeTable();
        }

        public static string GetReposConnectionString(string worker, ushort timeout = 30)
        {
            return String.Format(
                "Server={0};Initial Catalog={1};Integrated Security=SSPI;Application Name=SQL Wristband - {2};Connection Timeout={3};",
                reposInstance, reposDatabase, worker, timeout);
        }

        public void ReadConfigFromFile()
        {
            reposInstance = ConfigurationManager.AppSettings["RepositorySqlInstance"];
            reposDatabase = ConfigurationManager.AppSettings["RepositoryDatabaseName"];
            reposReconnectRetryInterval = Int32.Parse(ConfigurationManager.AppSettings["RepositoryReconnectRetryInterval"]);

            wsProtocol = ConfigurationManager.AppSettings["ListenerProtocol"];
            wsHostname = ConfigurationManager.AppSettings["ListenerHostname"];
            wsPort = ConfigurationManager.AppSettings["ListenerPort"];
            wsPrefix = ConfigurationManager.AppSettings["WebServicePrefix"];

            wsRootFolder = ConfigurationManager.AppSettings["WebServerRootFolder"];

            ReloadModuleClassMapping();
        } // end of ReadConfigFromFile method

        public void ReadConfigFromRepository()
        {
            using (SqlConnection con = new SqlConnection(GetReposConnectionString("Manager", 5)))
            {
                try
                {
                    con.Open();

                    purgeInterval = 360; // 6 hours

                    this.ReloadProbes();
                    this.ReloadMetricGroups();
                    this.ReloadTargets();
                    this.ReloadTimeTable();
                    this.ReloadArchiveOffsets();
                    this.ReloadArchiveWatermarks();

                    con.Close();

                    Manager.SetRepositoryAccessibility(true);
                }
                catch (SqlException e)
                {
                    string msg = "Could not connect to Repository [" + reposInstance + "].[" + reposDatabase + "]"
                        + Environment.NewLine
                        + e.Message;
                    Manager.SetRepositoryAccessibility(false);
                    throw new Exception(msg);
                }
            } // end of using SqlConnection
        }

        public bool AddTarget(string serverName, bool isSqlAuthentication, string sqlUsername, string sqlPassword)
        {
            bool targetAddedSuccessfully = false;
            SqlConnection reposConn = null;
            SqlCommand reposCmd = null;

            try
            {
                reposConn = new SqlConnection(GetReposConnectionString("WebService"));
                reposCmd = reposConn.CreateCommand();

                reposCmd.CommandType = CommandType.Text;
                if (isSqlAuthentication)
                {
                    reposCmd.CommandText = @"
INSERT INTO dbo.Targets (ServerName, IsSqlAuthentication, Username, Password)
VALUES(@serverName, @isSqlAuthentication, @username, @password)";
                }
                else
                {
                    reposCmd.CommandText = @"
INSERT INTO dbo.Targets (ServerName, IsSqlAuthentication)
VALUES(@serverName, @isSqlAuthentication)";
                    
                }
                reposCmd.Parameters.Add("@serverName", SqlDbType.NVarChar, 512);
                reposCmd.Parameters["@serverName"].Value = serverName;

                reposCmd.Parameters.Add("@isSqlAuthentication", SqlDbType.Bit);
                reposCmd.Parameters["@isSqlAuthentication"].Value = isSqlAuthentication;

                if (isSqlAuthentication)
                {
                    reposCmd.Parameters.Add("@username", SqlDbType.NVarChar, 64);
                    reposCmd.Parameters["@username"].Value = sqlUsername;

                    reposCmd.Parameters.Add("@password", SqlDbType.VarChar, 64);
                    reposCmd.Parameters["@password"].Value = sqlPassword;
                }

                reposConn.Open();
                reposCmd.Prepare();

                if (reposCmd.ExecuteNonQuery() == 1)
                    targetAddedSuccessfully = true;

                reposConn.Close();

                this.ReadConfigFromRepository();
            }
            catch (Exception e)
            {
                _logger.Error(e.Message);
            }
            finally
            {
                if (reposCmd != null)
                    ((IDisposable)reposCmd).Dispose();

                if (reposConn != null)
                    ((IDisposable)reposConn).Dispose();
            }

            return targetAddedSuccessfully;
        } // end of AddTarget method

        public bool DeleteTarget(int targetId)
        {
            bool targetDeletedSuccessfully = false;
            SqlConnection reposConn = null;
            SqlCommand reposCmd = null;
            Target tmpTarget;

            // TODO: Remove InMemory dictionaries for this target
            timeTable.RemoveTargetSchedules(targetId);
            targets.TryRemove(targetId, out tmpTarget);

            try
            {
                reposConn = new SqlConnection(GetReposConnectionString("WebService"));
                reposCmd = reposConn.CreateCommand();

                reposCmd.CommandType = CommandType.StoredProcedure;
                reposCmd.CommandText = "dbo.DeleteTarget";
                reposCmd.Parameters.Add("@TargetId", SqlDbType.Int);
                reposCmd.Parameters["@TargetId"].Value = targetId;

                reposConn.Open();
                reposCmd.Prepare();
                reposCmd.ExecuteNonQuery();

                targetDeletedSuccessfully = true;

                reposConn.Close();

                ReadConfigFromRepository();
            }
            catch (Exception e)
            {
                _logger.Error(e.Message);
            }
            finally
            {
                if (reposCmd != null)
                    ((IDisposable)reposCmd).Dispose();

                if (reposConn != null)
                    ((IDisposable)reposConn).Dispose();
            }

            return targetDeletedSuccessfully;
        } // end of DeleteTarget method

        public bool UpdateMetricGroupConfiguration(int metricGroupId, int interval, int retention)
        {
            bool cfgUpdatedSuccessfully = false;
            SqlConnection reposConn = null;
            SqlCommand reposCmd = null;

            _logger.Debug("New settings for target {0}: interval={1}, retention={2}", metricGroupId, interval, retention);

            try
            {
                reposConn = new SqlConnection(GetReposConnectionString("WebService"));
                reposCmd = reposConn.CreateCommand();

                reposCmd.CommandType = CommandType.Text;
                reposCmd.CommandText = "UPDATE dbo.DefaultSchedule SET ";

                if (interval != -1)
                {
                    reposCmd.CommandText += string.Format("IntervalInSeconds = {0}", interval);
                }

                if (retention != -1)
                {
                    if (interval != -1)
                    {
                        reposCmd.CommandText += ", ";
                    }

                    reposCmd.CommandText += string.Format("RetentionPeriodInHours = {0}", retention);
                }

                reposCmd.CommandText += string.Format(" WHERE MetricGroupId = {0}", metricGroupId);
                reposConn.Open();

                if (reposCmd.ExecuteNonQuery() == 1)
                    cfgUpdatedSuccessfully = true;

                reposConn.Close();

                ReloadMetricGroups();
                ReloadTimeTable();
            }
            catch (Exception e)
            {
                _logger.Error(e.Message);
            }
            finally
            {
                if (reposCmd != null)
                    ((IDisposable)reposCmd).Dispose();

                if (reposConn != null)
                    ((IDisposable)reposConn).Dispose();
            }

            return cfgUpdatedSuccessfully;
        }

        public bool UpdateMetricGroupScript(int metricGroupId, string scriptText, out string errorMsg)
        {
            bool cfgUpdatedSuccessfully = false;
            SqlConnection reposConn = null;
            SqlCommand reposCmd = null;

            errorMsg = null;

            _logger.Debug("New script for target {0}: {1}", metricGroupId, scriptText);

            try
            {
                reposConn = new SqlConnection(GetReposConnectionString("WebService"));
                reposCmd = reposConn.CreateCommand();

                reposConn.Open();

                // Check that query can compile
                reposCmd.CommandType = CommandType.Text;
                reposCmd.CommandText = @"SET NOEXEC ON;";
                reposCmd.ExecuteNonQuery();

                reposCmd.CommandText = scriptText;
                reposCmd.ExecuteNonQuery();

                reposCmd.CommandText = @"SET NOEXEC OFF;";
                reposCmd.ExecuteNonQuery();

                reposCmd.CommandText = @"UPDATE dbo.MetricGroups SET Script = @ScriptText WHERE Id = @MetricGroupId";
                reposCmd.Parameters.Add("@ScriptText", SqlDbType.NVarChar, scriptText.Length);
                reposCmd.Parameters["@ScriptText"].Value = scriptText;
                reposCmd.Parameters.Add("@MetricGroupId", SqlDbType.Int);
                reposCmd.Parameters["@MetricGroupId"].Value = metricGroupId;

                reposCmd.Prepare();

                if (reposCmd.ExecuteNonQuery() == 1)
                    cfgUpdatedSuccessfully = true;

                reposConn.Close();

                this.ReloadMetricGroups();
                this.ReloadTimeTable();
            }
            catch (Exception e)
            {
                errorMsg = e.Message;
                _logger.Error(e.Message);
                cfgUpdatedSuccessfully = false;
            }
            finally
            {
                if (reposCmd != null)
                    ((IDisposable)reposCmd).Dispose();

                if (reposConn != null)
                    ((IDisposable)reposConn).Dispose();
            }

            return cfgUpdatedSuccessfully;
        }

        #endregion public method declarations

        #region private method declarations

        private void ReloadProbes()
        {
            object probeClassReference;
            ProbeCollection tmp;

            tmp = new ProbeCollection();

            probeClassReference = Activator.CreateInstance(Type.GetType("SqlWristband.Probes.SqlServerProbe"), new object[] { this });
            tmp.Add("sqls", "SqlServer", probeClassReference);

            probes = tmp;
        }

        private void ReloadMetricGroups()
        {
            MetricGroup metricGroup;
            MetricGroupCollection tmp;

            tmp = new MetricGroupCollection();

            using (SqlConnection reposConn = new SqlConnection(GetReposConnectionString("Manager")))
            {
                reposConn.Open();

                // Read MetricGroups into temporary collection. This is to allow running threads to work with old data.
                using (SqlCommand reposCmd = reposConn.CreateCommand())
                {
                    reposCmd.CommandText = "SELECT Id, Name, ProbeCode, ChangeSpeed, IsMultiRow, IsCumulative, MultiRowKeyAttributesChangeSpeed, Script FROM dbo.MetricGroups ORDER BY Id";
                    reposCmd.CommandType = CommandType.Text;

                    using (SqlDataReader reposReader = reposCmd.ExecuteReader())
                    {
                        while (reposReader.Read())
                        {
                            metricGroup = new MetricGroup(
                                (int)reposReader["Id"],
                                reposReader["Name"].ToString(),
                                probes.GetProbeCodeByName(reposReader["ProbeCode"].ToString()),
                                Configuration.ChangeSpeedFromString(reposReader["ChangeSpeed"].ToString()),
                                (bool)reposReader["IsMultiRow"],
                                (bool)reposReader["IsCumulative"],
                                Configuration.ChangeSpeedFromString(reposReader["MultiRowKeyAttributesChangeSpeed"].ToString()));

                            metricGroup.scriptText = reposReader["Script"].ToString();

                            tmp.Add(metricGroup);
                        }

                        reposReader.Close();
                    }
                }

                foreach (MetricGroup mg in tmp)
                {
                    if (mg.isMultiRow)
                    {
                        // Read MultiRow Keys
                        using (SqlCommand reposCmd = reposConn.CreateCommand())
                        {
                            reposCmd.CommandText = "SELECT Name, DataType from [dbo].[MetricMultiRowKeys] WHERE MetricGroupId = @metricGroupId AND IsKeyAttribute = 'FALSE' ORDER BY Id";
                            reposCmd.CommandType = CommandType.Text;

                            reposCmd.Parameters.Add("@metricGroupId", SqlDbType.Int);
                            reposCmd.Parameters["@metricGroupId"].Value = mg.id;

                            reposCmd.Prepare();

                            using (SqlDataReader reposReader = reposCmd.ExecuteReader())
                            {
                                while (reposReader.Read())
                                {
                                    mg.AddMultiRowKey(
                                        reposReader["Name"].ToString(),
                                        Configuration.DataTypeFromString(reposReader["DataType"].ToString())
                                        );
                                }

                                reposReader.Close();
                            }
                        }

                        // Read MultiRow Key Attributes
                        using (SqlCommand reposCmd = reposConn.CreateCommand())
                        {
                            reposCmd.CommandText = "SELECT Name, DataType from [dbo].[MetricMultiRowKeys] WHERE MetricGroupId = @metricGroupId AND IsKeyAttribute = 'TRUE' ORDER BY Id";
                            reposCmd.CommandType = System.Data.CommandType.Text;

                            reposCmd.Parameters.Add("@metricGroupId", SqlDbType.Int);
                            reposCmd.Parameters["@metricGroupId"].Value = mg.id;

                            reposCmd.Prepare();

                            using (SqlDataReader reposReader = reposCmd.ExecuteReader())
                            {
                                while (reposReader.Read())
                                {
                                    mg.AddMultiRowKeyAttribute(
                                        reposReader["Name"].ToString(),
                                        Configuration.DataTypeFromString(reposReader["DataType"].ToString())
                                        );
                                }

                                reposReader.Close();
                            }
                        }
                    }

                    // Read from Metrics
                    using (SqlCommand reposCmd = reposConn.CreateCommand())
                    {
                        reposCmd.CommandText = "SELECT Name, DataType FROM [dbo].[Metrics] WHERE MetricGroupId = @metricGroupId ORDER BY Id";
                        reposCmd.CommandType = System.Data.CommandType.Text;

                        reposCmd.Parameters.Add("@metricGroupId", SqlDbType.Int);
                        reposCmd.Parameters["@metricGroupId"].Value = mg.id;

                        reposCmd.Prepare();

                        using (SqlDataReader reposReader = reposCmd.ExecuteReader())
                        {
                            while (reposReader.Read())
                            {
                                mg.AddMetric(
                                    reposReader["Name"].ToString(),
                                    Configuration.DataTypeFromString(reposReader["DataType"].ToString())
                                    );
                            }

                            reposReader.Close();
                        }
                    }

                    // Read default schedule
                    using (SqlCommand reposCmd = reposConn.CreateCommand())
                    {
                        reposCmd.CommandText = "SELECT OffsetInSecondsFromMidnight, IntervalInSeconds, RetentionPeriodInHours FROM [dbo].[DefaultSchedule] WHERE MetricGroupId = @metricGroupId";
                        reposCmd.CommandType = System.Data.CommandType.Text;

                        reposCmd.Parameters.Add("@metricGroupId", SqlDbType.Int);
                        reposCmd.Parameters["@metricGroupId"].Value = mg.id;

                        reposCmd.Prepare();

                        using (SqlDataReader reposReader = reposCmd.ExecuteReader())
                        {
                            while (reposReader.Read())
                            {
                                mg.defaultSchedule = new Schedule(
                                    (int)reposReader["OffsetInSecondsFromMidnight"],
                                    (int)reposReader["IntervalInSeconds"],
                                    (int)reposReader["RetentionPeriodInHours"]
                                    );
                            }

                            reposReader.Close();
                        }
                    }
                }

                reposConn.Close();
            }

            metricGroups = tmp;
        }

        private void ReloadTargets()
        {
            TargetCollection tmp = new TargetCollection();

            using (SqlConnection reposConn = new SqlConnection(GetReposConnectionString("Manager")))
            {
                reposConn.Open();

                using (SqlCommand reposCmd = reposConn.CreateCommand())
                {
                    reposCmd.CommandText = "SELECT Id, ServerName, IsSqlAuthentication, Username, Password FROM dbo.Targets ORDER BY Id";
                    reposCmd.CommandType = CommandType.Text;

                    using (SqlDataReader reposReader = reposCmd.ExecuteReader())
                    {
                        while (reposReader.Read())
                        {
                            tmp.Add(
                                (int)reposReader["Id"],
                                new Target(
                                    (int)reposReader["Id"],
                                    reposReader["ServerName"].ToString(),
                                    (bool)reposReader["IsSqlAuthentication"],
                                    reposReader["Username"].ToString(),
                                    reposReader["Password"].ToString()
                                    )
                                );
                        }

                        reposReader.Close();
                    }
                }

                reposConn.Close();
            }

            targets = tmp;
        }

        private void ReloadTimeTable()
        {
            TimeTable tmp = new TimeTable();

            using (SqlConnection reposConn = new SqlConnection(GetReposConnectionString("Manager")))
            {
                reposConn.Open();

                using (SqlCommand reposCmd = reposConn.CreateCommand())
                {
                    reposCmd.CommandText = "SELECT Id, TargetId, MetricGroupId, OffsetInSecondsFromMidnight, IntervalInSeconds, RetentionPeriodInHours FROM dbo.Schedule ORDER BY Id";
                    reposCmd.CommandType = CommandType.Text;

                    using (SqlDataReader reposReader = reposCmd.ExecuteReader())
                    {
                        while (reposReader.Read())
                        {
                            tmp.TryAdd(
                                (int)reposReader["Id"],
                                (int)reposReader["TargetId"],
                                (int)reposReader["MetricGroupId"],
                                new Schedule(
                                    (int)reposReader["OffsetInSecondsFromMidnight"],
                                    (int)reposReader["IntervalInSeconds"],
                                    (int)reposReader["RetentionPeriodInHours"]
                                    )
                                );
                        }

                        reposReader.Close();
                    }
                }

                reposConn.Close();
            }

            timeTable = tmp;
        }

        private void ReloadModuleClassMapping()
        {
            Dictionary<string, string> tmp;

            tmp = new Dictionary<string, string>();

            tmp.Add("fastsingle", "SqlWristband.Web.RequestProcessors.FastSingleMetricRequestProcessor");
            tmp.Add("fastmulti", "SqlWristband.Web.RequestProcessors.FastMultiMetricRequestProcessor");
            tmp.Add("dashboard", "SqlWristband.Web.RequestProcessors.DashboardRequestProcessor");
            tmp.Add("config", "SqlWristband.Web.RequestProcessors.ConfigurationRequestProcessor");

            moduleClassMapping = tmp;
        }

        private void ReloadArchiveOffsets()
        {
            ArchiveOffsetCollection tmp = new ArchiveOffsetCollection();

            using (SqlConnection reposConn = new SqlConnection(GetReposConnectionString("Manager")))
            {
                reposConn.Open();

                using (SqlCommand reposCmd = reposConn.CreateCommand())
                {
                    reposCmd.CommandText = "SELECT Id, ScheduleId, OffsetInMinutes, IntervalInSeconds FROM dbo.ArchiveOffsets ORDER BY Id";
                    reposCmd.CommandType = CommandType.Text;

                    using (SqlDataReader reposReader = reposCmd.ExecuteReader())
                    {
                        while (reposReader.Read())
                        {
                            tmp.TryAdd(
                                (int)reposReader["Id"],
                                new ArchiveOffset(
                                    (int)reposReader["Id"],
                                    (int)reposReader["ScheduleId"],
                                    (int)reposReader["OffsetInMinutes"],
                                    (int)reposReader["IntervalInSeconds"]
                                    )
                                );
                        }

                        reposReader.Close();
                    }
                }

                reposConn.Close();
            }

            archiveOffsets = tmp;
        }

        private void ReloadArchiveWatermarks()
        {
            ArchiveWatermarkCollection tmp = new ArchiveWatermarkCollection();

            using (SqlConnection reposConn = new SqlConnection(GetReposConnectionString("Manager")))
            {
                reposConn.Open();

                SqlCommand reposCmd = null;

                try
                {
                    // Add missing records
                    reposCmd = reposConn.CreateCommand();
                    reposCmd.CommandType = CommandType.Text;
                    reposCmd.CommandText = @"INSERT INTO dbo.ArchiveWatermarks (TargetId, ArchiveOffsetId, ArchivedToDate)
SELECT ao.TargetId, ao.Id, DATETIME2FROMPARTS(2000, 01, 01, 0, 0, 0, 0, 0)
FROM dbo.ArchiveOffsets ao
WHERE NOT EXISTS (SELECT NULL FROM dbo.ArchiveWatermarks aw WHERE aw.TargetId = ao.TargetId AND aw.ArchiveOffsetId = ao.Id)";

                    reposCmd.ExecuteNonQuery();

                    // Read data from the ArchiveWatermarks table
                    reposCmd.CommandText = "SELECT Id, TargetId, ArchiveOffsetId, ArchivedToDate FROM dbo.ArchiveWatermarks ORDER BY Id";

                    using (SqlDataReader reposReader = reposCmd.ExecuteReader())
                    {
                        while (reposReader.Read())
                        {
                            tmp.TryAdd(
                                (int)reposReader["Id"],
                                new ArchiveWatermark(
                                    (int)reposReader["Id"],
                                    (int)reposReader["TargetId"],
                                    (int)reposReader["ArchiveOffsetId"],
                                    (DateTime)reposReader["ArchivedToDate"]
                                    )
                                );
                        }

                        reposReader.Close();
                    }

                    archiveWatermarks = tmp;
                }
                catch (SqlException e)
                {
                    _logger.Error(e.Message);
                    throw;
                }
                finally
                {
                    if (reposCmd != null)
                        ((IDisposable)reposCmd).Dispose();
                }

                reposConn.Close();
            }
        }

        #endregion private method declarations

        #region private static declarations

        private static ChangeSpeed ChangeSpeedFromString(string input)
        {
            if (input == null || input.Equals(String.Empty))
                return ChangeSpeed.NotApplicable;

            if (input.Equals("Fast"))
                return ChangeSpeed.Fast;

            if (input.Equals("Slow"))
                return ChangeSpeed.Slow;

            if (input.Equals("Static"))
                return ChangeSpeed.Static;

            throw new Exception(String.Format("Unknown change speed '{0}'.", input));
        }

        private static DataType DataTypeFromString(string input)
        {
            if (input == null || input.Equals(String.Empty))
                throw new Exception("DataType can not be empty/null.");

            if (input.Equals("Double"))
                return DataType.Double;

            if (input.Equals("Datetime"))
                return DataType.Datetime;

            if (input.Equals("Ansi"))
                return DataType.Ansi;

            if (input.Equals("SmallInt"))
                return DataType.SmallInt;

            if (input.Equals("Unicode"))
                return DataType.Unicode;

            throw new Exception(String.Format("Unknown data type '{0}'.", input));
        }

        #endregion private static declarations
    } // end of Configuration class
}
