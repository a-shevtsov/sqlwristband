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

namespace SqlWristband
{
    using NLog;
    using System;
    using System.Collections.Generic;
    using System.Linq;
    using System.Threading;

    public delegate void ProbeDelegate(int timeTableId, Target target, MetricGroup metricGroup);

    public class ActiveDelegate
    {
        public int timeTableId;
        public ProbeDelegate probe;
        public IAsyncResult result;

        public ActiveDelegate(int s, ProbeDelegate p, IAsyncResult r)
        {
            this.timeTableId = s;
            this.probe = p;
            this.result = r;
        }
    }

    /*
     * Spawns new threads that retrieve data from patients and enqueue it into Analyzer's queues
     */
    public class Scheduler
    {
        private const int TimeDelta = 50; // in millisecond

        private static Manager _mgr;
        private static Configuration _cfg;
        private static Logger _logger = LogManager.GetLogger("Scheduler");

        private bool _shouldStop;

        public Scheduler(Manager manager, Configuration cfg)
        {
            if (_mgr == null)
                _mgr = manager;

            _cfg = cfg;
        }

        // stops execution of ProcessRequests
        public void RequestStop()
        {
            _logger.Info("Stopping Scheduler");
            this._shouldStop = true;
        }

        // method that runs continuously and starts tasks
        public void Launcher()
        {
            string probeCode;
            int timeTableId;
            object probeClassReference;
            ProbeDelegate probeDelegate;
            List<ActiveDelegate> activeDelegates;
            List<Tuple<DateTime, int>> nextRuns;

            try
            {
                activeDelegates = new List<ActiveDelegate>();

                _logger.Info("Scheduler started");

                while (!this._shouldStop)
                {
                    // start up Probes
                    nextRuns = Configuration.timeTable.NextRuns;

                    if (nextRuns.Count > 0)
                    {
                        foreach (Tuple<DateTime, int> nextRun in nextRuns.ToList())
                        {
                            // start up a new Probe if there is less than TimeDelta milliseconds to the start or start time has passed already
                            // Use Ticks, otherwise new day values will get you into always false check (comparing late yesterday 23:59:59 with today)
                            if (nextRun.Item1.Ticks < DateTime.Now.Ticks + TimeDelta*10000)
                            {
                                timeTableId = nextRun.Item2;

                                // Check whether schedule is still there
                                if (!Configuration.timeTable.ContainsKey(timeTableId))
                                    continue;

                                var timeTable = Configuration.timeTable[timeTableId];
                                var target = Configuration.targets[timeTable._targetId];
                                var metricGroup = Configuration.metricGroups[timeTable._metricGroupId];
                                probeCode = metricGroup.probeCode;
                                probeClassReference = Configuration.probes.GetClassReferenceByCode(probeCode);
                                _logger.Debug("Delegate for schedule {0} is being started", timeTableId);

                                Delegate d = probeClassReference.GetType().GetMethod("Probe").CreateDelegate(typeof(ProbeDelegate), probeClassReference);
                                probeDelegate = (ProbeDelegate)d;
                                IAsyncResult result = probeDelegate.BeginInvoke(timeTableId, target, metricGroup, null, null);
                                activeDelegates.Add(new ActiveDelegate(timeTableId, probeDelegate, result));

                                Configuration.timeTable.RemoveNextRun(nextRun);

                                _logger.Debug("{0} delegates are running", activeDelegates.Count);
                            }
                        }
                    }

                    // check for ended delegates
                    foreach (ActiveDelegate ad in activeDelegates.ToList())
                    {
                        if (ad.result.IsCompleted)
                        {
                            _logger.Debug("Delegate for schedule " + ad.timeTableId.ToString() + " finished execution");

                            try
                            {
                                ad.probe.EndInvoke(ad.result);
                            }
                            catch (Exception e)
                            {
                                _logger.Error("EndInvoke failed with error: " + e.Message);
                            }

                            activeDelegates.Remove(ad);

                            if (Configuration.timeTable.ContainsKey(ad.timeTableId))
                            {
                                if (Configuration.targets.ContainsKey(Configuration.timeTable[ad.timeTableId]._targetId))
                                {
                                    Configuration.timeTable.SetNextRun(ad.timeTableId);
                                }
                                else
                                {
                                    // Remove all target schedules if target has been deleted
                                    Configuration.timeTable.RemoveTargetSchedules(Configuration.timeTable[ad.timeTableId]._targetId);
                                }
                            }
                            else
                            {
                                // Remove 
                                Configuration.timeTable.Remove(ad.timeTableId);
                            }
                        }
                    }

                    Thread.Sleep(TimeDelta/2);
                }

                _logger.Info("Scheduler stopped");
            }
            catch (Exception e)
            {
                _logger.Error(e.Message);
                _logger.Error(e.StackTrace);
                _mgr.ReportFailure("Scheduler");
            }
        } // end of Launcher method
    } // end of Scheduler class
}
