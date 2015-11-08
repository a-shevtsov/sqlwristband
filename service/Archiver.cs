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
using SqlWristband.Probes;

namespace SqlWristband
{
    using NLog;
    using System;
    using System.Collections.Generic;
    using System.Data;
    using System.Data.SqlClient;
    using System.Threading;

    /*
     * Archiving is run for each metric/target pair and can have multiple retention period/interval pairs.
     * It archives all metric records in an interval into one averaged record saving space and increasing query speed.
     * Example:
     *  Retention 24 hours, interval 15 minutes (archives data older than 24 hours)
     *  Retention  3 hours, interval  5 minutes (archives data older than 3 hours but not older than 24 hours)
     */
    class Archiver : IDisposable
    {
        private static Manager _mgr;
        private static Configuration _cfg;
        private static Logger _logger = LogManager.GetLogger("Archiver");

        private static bool _shouldStop;

        private SqlConnection _reposConn;

        public Archiver(Manager manager, Configuration cfg)
        {
            if (_mgr == null)
                _mgr = manager;

            _cfg = cfg;

            _shouldStop = false;
        }

        // stops execution of Work method
        public void RequestStop()
        {
            _logger.Info("Stopping Archiver");
            _shouldStop = true;
        }

        public void Dispose()
        {
            if (_reposConn != null)
                _reposConn.Dispose();
        }

        // method that runs continuously and purges data in repository
        public void Work()
        {
            List<int> keys;
            MetricGroup metricGroup;
            InstanceSchedule schedule;
            ArchiveWatermark archiveWatermark, archiveWatermarkNew;
            ArchiveOffset archiveOffset;

            _logger.Info("Archiver started");

            _reposConn = new SqlConnection(Configuration.GetReposConnectionString("Archiver"));

            while (!_shouldStop)
            {
                try
                {
                    // this is to wait until the repository is ready to serve requests
                    if (!Manager.IsRepositoryAccessible)
                    {
                        Thread.Sleep(250);
                        continue;
                    }

                    if (_reposConn.State != ConnectionState.Open)
                        _reposConn.Open();

                    // go over TimeTable to get all target/metricGroup pairs
                    keys = new List<int>(Configuration.archiveWatermarks.Keys);
                    foreach (int key in keys)
                    {
                        if (_shouldStop)
                            break;

                        if (!Configuration.archiveWatermarks.TryGetValue(key, out archiveWatermark))
                            continue;

                        if (
                            !Configuration.archiveOffsets.TryGetValue(archiveWatermark.ArchiveOffsetId,
                                out archiveOffset))
                            continue;

                        if (!Configuration.timeTable.TryGetValue(archiveOffset.ScheduleId, out schedule))
                            continue;

                        metricGroup = Configuration.metricGroups[schedule._metricGroupId];

                        // Do not archive static and slow changing metrics
                        if (metricGroup.changeSpeed != ChangeSpeed.Fast)
                            continue;

                        // Check whether there is enough time to archive data between watermark and offset
                        // Watermark + OffsetInMinutes + IntervalInSeconds < Now
                        if (archiveWatermark.ArchivedToDate
                            .AddMinutes(archiveOffset.OffsetInMinutes)
                            .AddSeconds(archiveOffset.IntervalInSeconds)
                            .CompareTo(DateTime.Now) >= 0)
                            continue;

                        DateTime nextLevelArchiveToDateTime =
                            Configuration.archiveWatermarks.GetNextLevelArchivedToDate(archiveWatermark.Id);
                        DateTime archiveFromDateTime = nextLevelArchiveToDateTime > archiveWatermark.ArchivedToDate
                            ? nextLevelArchiveToDateTime
                            : archiveWatermark.ArchivedToDate;
                        DateTime archiveToDateTime = DateTime.Now.AddMinutes(-1*archiveOffset.OffsetInMinutes);

                        _logger.Debug(
                            "TargetId: [{0}] MetricGroup: [{1}] ArchiveOffset: {2} Interval: {3} From: {4} To: {5}",
                            archiveWatermark.TargetId,
                            metricGroup.name,
                            archiveOffset.OffsetInMinutes,
                            archiveOffset.IntervalInSeconds,
                            archiveFromDateTime,
                            archiveToDateTime
                            );

                        // this is to wait until the repository is ready to serve requests
                        if (!Manager.IsRepositoryAccessible)
                        {
                            Thread.Sleep(250);
                            break;
                        }

                        if (_reposConn.State != ConnectionState.Open)
                            _reposConn.Open();

                        if (!Archive(archiveWatermark.TargetId, archiveOffset, metricGroup, archiveFromDateTime,
                            archiveToDateTime))
                        {
                            _logger.Debug("TargetId: [{0}] MetricGroup: [{1}] Failed to archive data",
                                archiveWatermark.TargetId,
                                metricGroup.name);
                            continue;
                        }

                        _logger.Debug("TargetId: [{0}] MetricGroup: [{1}] Archived successfully",
                            archiveWatermark.TargetId,
                            metricGroup.name);
                        // Update in-memory version of ArchiveWatermark (ArchivedToDate value)
                        archiveWatermarkNew = archiveWatermark;
                        archiveWatermarkNew.ArchivedToDate = archiveToDateTime;
                        Configuration.archiveWatermarks.TryUpdate(key, archiveWatermarkNew, archiveWatermark);
                    } // foreach

                    for (int i = 0; i < 100; i++)
                    {
                        if (_shouldStop)
                            break;
                        Thread.Sleep(250);
                    }
                }
                catch (Exception e)
                {
                    if (_reposConn != null)
                    {
                        switch (_reposConn.State)
                        {
                            case ConnectionState.Broken:
                            case ConnectionState.Closed:
                                Manager.SetRepositoryAccessibility(false);
                                break;
                            default:
                                _logger.Error(e.Message);
                                _logger.Error(e.StackTrace);
                                _mgr.ReportFailure("Archiver");
                                return;
                        }
                    }
                    else
                    {
                        _logger.Error(e.Message);
                        _logger.Error(e.StackTrace);
                        _mgr.ReportFailure("Archiver");
                        return;
                    }
                }
            } // end of while (!_shouldStop)

            _logger.Info("Archiver stopped");
        } // end of Work method
 
        private bool Archive(int targetId, ArchiveOffset archiveOffset, MetricGroup metricGroup,
                            DateTime archiveTo, DateTime archiveFrom)
        {
            // Do not archive static and slow changing metrics
            if (metricGroup.changeSpeed != ChangeSpeed.Fast)
                return false;

            // Compose SQL statement

            // Save aggregated data in a temp table
            string sqlStmt = "SELECT " + RoundDate("dt", archiveOffset.IntervalInSeconds) + " as dt, " + Environment.NewLine;
            // add dictId if the metric group has multiple rows
            if (metricGroup.isMultiRow)
                sqlStmt += "dictId, ";

            // Add AVG(column names)
            foreach (var item in metricGroup.metrics)
                sqlStmt += "AVG(" + item.Value.name.Replace(' ', '_') + ") as " + item.Value.name.Replace(' ', '_') + ", ";

            sqlStmt = sqlStmt.Remove(sqlStmt.Length - 2) + Environment.NewLine; // remove last comma
            sqlStmt += "INTO #AVG_TMP_" + metricGroup.dataTableName + Environment.NewLine;
            sqlStmt += "FROM " + SqlServerProbe.DataTableName(targetId, metricGroup) + Environment.NewLine;
            sqlStmt += "WHERE dt BETWEEN @dateFrom AND @dateTo" + Environment.NewLine;
            sqlStmt += "GROUP BY " + RoundDate("dt", archiveOffset.IntervalInSeconds) + ", ";

            // add dictId if the metric group has multiple rows
            if (metricGroup.isMultiRow)
                sqlStmt += "dictId, ";

            sqlStmt = sqlStmt.Remove(sqlStmt.Length - 2) + ";" + Environment.NewLine + Environment.NewLine; // remove last comma

            // Delete aggregated records
            sqlStmt += "DELETE FROM " + SqlServerProbe.DataTableName(targetId, metricGroup) + " WHERE dt BETWEEN @dateFrom AND @dateTo;" + Environment.NewLine + Environment.NewLine;

            // Copy records from the temp table
            sqlStmt += "INSERT INTO " + SqlServerProbe.DataTableName(targetId, metricGroup) + " (dt, ";

            // add dictId if the metric group has multiple rows
            if (metricGroup.isMultiRow)
                sqlStmt += "dictId, ";

            // Add column names
            foreach (var item in metricGroup.metrics)
                sqlStmt += item.Value.name.Replace(' ', '_') + ", ";

            sqlStmt = sqlStmt.Remove(sqlStmt.Length - 2); // remove last comma

            sqlStmt += ")" + Environment.NewLine;
            sqlStmt += "SELECT dt, ";

            // add dictId if the metric group has multiple rows
            if (metricGroup.isMultiRow)
                sqlStmt += "dictId, ";

            // Add column names
            foreach (var item in metricGroup.metrics)
                sqlStmt += item.Value.name.Replace(' ', '_') + ", ";

            sqlStmt = sqlStmt.Remove(sqlStmt.Length - 2) + Environment.NewLine; // remove last comma
            sqlStmt += "FROM #AVG_TMP_" + metricGroup.dataTableName + Environment.NewLine + Environment.NewLine;
            
            // Update ArchivedToDate value
            sqlStmt += "UPDATE dbo.ArchiveWatermarks SET ArchivedToDate = @dateTo WHERE ArchiveOffsetId = @archiveOffsetId and TargetId = @targetId;";
            
            _logger.Trace(sqlStmt);
            // Execute SQL statement
            SqlTransaction reposTran = null;
            SqlCommand reposCmd = null;

            try
            {
                if (_reposConn.State != ConnectionState.Open)
                    _reposConn.Open();

                reposTran = _reposConn.BeginTransaction();

                reposCmd = _reposConn.CreateCommand();
                reposCmd.Transaction = reposTran;
                reposCmd.CommandType = CommandType.Text;
                reposCmd.CommandText = sqlStmt;
                reposCmd.CommandTimeout = 300;

                reposCmd.Parameters.Add("@targetId", SqlDbType.Int);
                reposCmd.Parameters["@targetId"].Value = targetId;

                reposCmd.Parameters.Add("@archiveOffsetId", SqlDbType.Int);
                reposCmd.Parameters["@archiveOffsetId"].Value = archiveOffset.Id;

                reposCmd.Parameters.Add("@dateFrom", SqlDbType.DateTime2, 6);
                reposCmd.Parameters["@dateFrom"].Value = RoundDate(archiveFrom, archiveOffset.IntervalInSeconds);

                reposCmd.Parameters.Add("@dateTo", SqlDbType.DateTime2, 6);
                reposCmd.Parameters["@dateTo"].Value = archiveTo;

                reposCmd.Prepare();

                reposCmd.ExecuteNonQuery();

                reposTran.Commit();
            }
            catch (SqlException e)
            {
                if (_reposConn.State != ConnectionState.Open)
                {
                    Manager.SetRepositoryAccessibility(false);
                    return false;
                }

                switch (e.Number)
                {
                    case 208: // Ignore missing tables. Target might be recently initialized
                        break;
                    default:
                        _logger.Error("SqlException: {0} ErrorCode: {1}", e.Message, e.Number);
                        break;
                }

                if (reposTran != null)
                {
                    // Transaction might be rolled back if commit fails. In this case second rollback will fail
                    try
                    {
                        reposTran.Rollback();
                    }
                    catch (Exception)
                    {
                        _logger.Debug("Transaction has been rolled back already");
                    }
                }

                return false;
            }
            catch (Exception e)
            {
                if (_reposConn.State == ConnectionState.Open)
                {
                    _logger.Error(e.Message);
                    _logger.Error(e.StackTrace);
                }
                else
                {
                    Manager.SetRepositoryAccessibility(false);
                }

                return false;
            }
            finally
            {
                if (reposCmd != null)
                    ((IDisposable)reposCmd).Dispose();

                if (reposTran != null)
                    ((IDisposable)reposTran).Dispose();
            }

            return true;
        } // end of Archive method

        /// <summary>Rounds up datetime2(9) SQL expression (or column name) to the closest interval</summary>
        /// <param name="expression">SQL expression or column name</param>
        /// <param name="intervalInSeconds">Interval in seconds</param>
        /// DATEADD(second, -1*((DATEPART(hour, dateColumnName)*3600 + DATEPART(minute, dateColumnName)*60 + DATEPART(second, dateColumnName)) % @intervalInSeconds) + @intervalInSeconds/2, dateColumnName)
        private static string RoundDate(string expression, int intervalInSeconds)
        {
            return String.Format(
                "DATEADD(second, -1*((DATEPART(hour, {0})*3600 + DATEPART(minute, {0})*60 + DATEPART(second, {0})) % {1}) + {1}/2, {0})",
                expression,
                intervalInSeconds
                );
        } // end of DateGroupByExpression method

        /// <summary>Rounds up DateTime to the closest interval</summary>
        /// <param name="dt">Date to round up</param>
        /// <param name="intervalInSeconds">Interval in seconds</param>
        private static DateTime RoundDate(DateTime dt, int intervalInSeconds)
        {
            DateTime tmp = dt.AddMilliseconds(-1 * dt.Millisecond);

            return tmp.AddSeconds(-1 * ((tmp.Hour * 3600 + tmp.Minute * 60 + tmp.Second) % intervalInSeconds));
        } // end of DateGroupByExpression method    
    }
}
