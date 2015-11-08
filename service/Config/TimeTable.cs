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

namespace SqlWristband.Config
{
    using System;
    using System.Collections.Generic;
    using System.Collections.Concurrent;
    using System.Linq;
    using System.Text;
    using System.Threading.Tasks;

    public struct Schedule
    {
        /// <summary>Offset from Midnight in seconds</summary>
        public int offset;
        /// <summary>Interval between executions in seconds</summary>
        public int interval;
        /// <summary>Retention period in hours</summary>
        public int retention;

        public Schedule(int o, int i, int r)
        {
            this.offset = o;
            this.interval = i;
            this.retention = r;
        }
    }

    public struct InstanceSchedule
    {
        public int _targetId; // Id of the instance
        public int _metricGroupId; // metric group (SQL Wait Stats, SQL File Stats, ...)
        public Schedule _schedule; // schedule

        public DateTime _lastPurged;
        public DateTime _lastPolled;

        public InstanceSchedule(int targetId, int metricGroupId, Schedule schedule)
        {
            this._targetId = targetId;
            this._metricGroupId = metricGroupId;
            this._schedule = schedule;
            this._lastPurged = DateTime.Parse("1980-01-01 00:00:00");
            this._lastPolled = DateTime.Parse("1980-01-01 00:00:00");
        }
    }

    /*
     * Manages schedules for all metric-instance pairs
     */
    public class TimeTable : ConcurrentDictionary<int, InstanceSchedule>
    {
        object _lock = new object();

        List<Tuple<DateTime, int>> _nextRuns;

        public List<Tuple<DateTime, int>> NextRuns
        {
            get
            {
                return this._nextRuns;
            }
        }

        public TimeTable()
        {
            this._nextRuns = new List<Tuple<DateTime, int>>();
        }

        public void TryAdd(int timeTableId, int targetId, int metricGroupId, Schedule schedule)
        {
            lock (_lock)
            {
                this.TryAdd(timeTableId, new InstanceSchedule(targetId, metricGroupId, schedule));
            }

            this.SetNextRun(timeTableId);
        }

        public void Remove(int timeTableId)
        {
            InstanceSchedule tmp;

            lock (_lock)
            {
                // remove corresponding next runs too
                var nextRuns = this._nextRuns.ToList();
                foreach (var nextRun in nextRuns)
                {
                    if (nextRun.Item2 == timeTableId)
                    {
                        this._nextRuns.Remove(nextRun);
                    }
                }

                this.TryRemove(timeTableId, out tmp);
            }
        }

        public void RemoveTargetSchedules(int targetId)
        {
            InstanceSchedule tmp;

            lock (_lock)
            {
                foreach (int timeTableId in this.Keys)
                {
                    if (this[timeTableId]._targetId == targetId)
                    {
                        this.TryRemove(timeTableId, out tmp);

                        // remove corresponding next runs too
                        var nextRuns = this._nextRuns.ToList();
                        foreach (var nextRun in nextRuns)
                        {
                            if (nextRun.Item2 == targetId)
                            {
                                this._nextRuns.Remove(nextRun);
                            }
                        }
                    }
                }
            }
        }

        public void RemoveNextRun(Tuple<DateTime, int> nextRun)
        {
            lock (_lock)
            {
                if (this._nextRuns.Contains(nextRun))
                    this._nextRuns.Remove(nextRun);
            }
        }

        public void SetNextRun(int timeTableId)
        {
            bool scheduleExists = false;

            lock (_lock)
            {
                // check if next run is already set. This can be true if schedule was changed in configuration
                foreach (Tuple<DateTime, int> nextRun in this._nextRuns)
                {
                    if (nextRun.Item2 == timeTableId)
                    {
                        scheduleExists = true;
                        break;
                    }
                }

                if (!scheduleExists)
                {
                    this._nextRuns.Add(new Tuple<DateTime, int>(this.NextRun(timeTableId), timeTableId));
                }
            }
        }

        public DateTime NextRun(int timeTableId)
        {
            int nextRunInSeconds;

            Schedule schedule = this[timeTableId]._schedule;
            DateTime nextRunTime = DateTime.Now;

            int currentTimeInSeconds = (int)Math.Floor(nextRunTime.TimeOfDay.TotalSeconds);
            
            // calculate number of full intervals that have passed since beginning of the day
            nextRunInSeconds = (int)Math.Floor(1.0 * (currentTimeInSeconds - schedule.offset) / schedule.interval);

            // calculate next run time for this day
            nextRunInSeconds = ((nextRunInSeconds + 1) * schedule.interval) + schedule.offset;

            // Set next run time by adding difference between current time and next time
            nextRunTime = nextRunTime.AddSeconds(nextRunInSeconds - currentTimeInSeconds);

            return nextRunTime;
        }

        public void SetLastPurge(int scheduleId, DateTime lastPurged)
        {
            InstanceSchedule scheduleOrig, scheduleNew;
            this.TryGetValue(scheduleId, out scheduleOrig);
            scheduleNew = scheduleOrig;
            scheduleNew._lastPurged = lastPurged;
            this.TryUpdate(scheduleId, scheduleNew, scheduleOrig);
        }

        public void SetLastPoll(int scheduleId, DateTime lastPolled)
        {
            InstanceSchedule scheduleOrig, scheduleNew;
            this.TryGetValue(scheduleId, out scheduleOrig);
            scheduleNew = scheduleOrig;
            scheduleNew._lastPolled = lastPolled;
            this.TryUpdate(scheduleId, scheduleNew, scheduleOrig);
        }

        public List<Tuple<DateTime, int>> LastPurge()
        {
            int scheduleId;
            List<Tuple<DateTime, int>> lastPurgeTimes = new List<Tuple<DateTime, int>>();

            for (int i = 0; i < this.Count; i++)
            {
                scheduleId = this.ElementAt(i).Key;
                lastPurgeTimes.Add(new Tuple<DateTime, int>(this[scheduleId]._lastPurged, scheduleId));
            }

            return lastPurgeTimes;
        }

        public DateTime GetLastPoll(int targetId)
        {
            DateTime maxDt = DateTime.MinValue;

            foreach (int id in this.Keys)
            {
                if (this[id]._targetId == targetId && this[id]._lastPolled > maxDt)
                    maxDt = this[id]._lastPolled;
            }

            return maxDt;
        }

        private void CalculateNextRunForTimeTableId(int timeTableId)
        {
            lock (_lock)
            {
                this._nextRuns.Add(new Tuple<DateTime, int>(this.NextRun(timeTableId), timeTableId));
            }
        }
    } // end of TimeTable class
}
