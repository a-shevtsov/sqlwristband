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
     * Purges old data for all existing schedules (target-metric pairs)
     * Purging is run for each metric/target pair independently lowering impact of the database and reducing locking times
     */
    public class Purger : IDisposable
    {
        private static Manager _mgr;
        private static Configuration _cfg;
        private static Logger _logger = LogManager.GetLogger("Purger");

        private static bool _shouldStop;

        private SqlConnection _reposConn;

        public Purger(Manager manager, Configuration cfg)
        {
            if (_mgr == null)
                _mgr = manager;

            _cfg = cfg;

            _shouldStop = false;
        }

        // stops execution of Work method
        public void RequestStop()
        {
            _logger.Info("Stopping Purger");
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
            int targetId;
            int timeTableId;
            string sqlStmt;
            MetricGroup metricGroup;
            InstanceSchedule schedule;
            List<Tuple<DateTime, int>> lastPurgeList;

            _logger.Info("Purger started");

            _reposConn = new SqlConnection(Configuration.GetReposConnectionString("Purger"));

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

                    if (this._reposConn.State != ConnectionState.Open)
                        this._reposConn.Open();

                    lastPurgeList = Configuration.timeTable.LastPurge();
                    foreach (Tuple<DateTime, int> lastPurge in lastPurgeList)
                    {
                        timeTableId = lastPurge.Item2;

                        // Skip schedules that have been deleted
                        if (!Configuration.timeTable.TryGetValue(timeTableId, out schedule))
                            continue;

                        // skip schedules that were purged recently
                        if (DateTime.Compare(lastPurge.Item1.AddMinutes(Configuration.purgeInterval), DateTime.Now) > 0)
                            continue;

                        targetId = schedule._targetId;
                        metricGroup = Configuration.metricGroups[schedule._metricGroupId];

                        // prepare SQL statement
                        switch (metricGroup.changeSpeed)
                        {
                            case ChangeSpeed.Slow:
                                sqlStmt = "DELETE FROM " + SqlServerProbe.DataTableName(targetId, metricGroup)
                                    + " WHERE endDate <= '" + SqlServerProbe.DateTimeToString(DateTime.Now.AddHours(-1 * schedule._schedule.retention)) + "'";
                                break;
                            case ChangeSpeed.Fast:
                                sqlStmt = "DELETE FROM " + SqlServerProbe.DataTableName(targetId, metricGroup)
                                    + " WHERE dt <= '" + SqlServerProbe.DateTimeToString(DateTime.Now.AddHours(-1 * schedule._schedule.retention)) + "'";
                                break;
                            default:
                                throw new Exception("Unsupported change speed");
                        }

                        // execute SQL statement
                        try
                        {
                            if (!Manager.IsRepositoryAccessible)
                            {
                                Thread.Sleep(250);
                                break;
                            }

                            if (_reposConn.State != ConnectionState.Open)
                                _reposConn.Open();

                            using (SqlCommand cmd = _reposConn.CreateCommand())
                            {
                                cmd.CommandType = CommandType.Text;
                                cmd.CommandText = sqlStmt;
                                cmd.CommandTimeout = 300;
                                int rowCount = cmd.ExecuteNonQuery();
                                _logger.Debug("Rows deleted from table " + SqlServerProbe.DataTableName(targetId, metricGroup) + ": " + rowCount.ToString());
                            }

                            // update last purge time
                            Configuration.timeTable.SetLastPurge(lastPurge.Item2, DateTime.Now);
                        }
                        catch (SqlException e)
                        {
                            if (e.Number == 208)
                            {
                                _logger.Debug("Table " + SqlServerProbe.DataTableName(targetId, metricGroup) + " does not exist");
                            }
                            else if (this._reposConn.State != ConnectionState.Open)
                            {
                                Manager.SetRepositoryAccessibility(false);
                            }
                            else
                            {
                                _logger.Error("Could not purge " + SqlServerProbe.DataTableName(targetId, metricGroup) + " due to error: " + e.Message);
                            }
                        }
                        catch (Exception e)
                        {
                            if (this._reposConn.State == ConnectionState.Open)
                            {
                                _logger.Error("Could not purge " + SqlServerProbe.DataTableName(targetId, metricGroup) + " due to error: " + e.Message);
                            }
                            else
                                Manager.SetRepositoryAccessibility(false);

                            continue;
                        }

                        Thread.Sleep(250); // we don't want to stress the repository too much
                    } // foreach

                    Thread.Sleep(250);
                }
                catch (Exception e)
                {
                    if (this._reposConn != null)
                    {
                        switch (this._reposConn.State)
                        {
                            case System.Data.ConnectionState.Broken:
                            case System.Data.ConnectionState.Closed:
                                Manager.SetRepositoryAccessibility(false);
                                break;
                            default:
                                _logger.Error(e.Message);
                                _logger.Error(e.StackTrace);
                                _mgr.ReportFailure("Purger");
                                return;
                        }
                    }
                    else
                    {
                        _logger.Error(e.Message);
                        _logger.Error(e.StackTrace);
                        _mgr.ReportFailure("Purger");
                        return;
                    }
                } // end of catch
            } // end of while (!_shouldStop)

            _logger.Info("Purger stopped");
        }
    } // end of Purger class
}
