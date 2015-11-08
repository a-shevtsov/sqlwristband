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
    using System.Collections.Concurrent;
    using System.Threading;

    /*
     * Reads data from queues, analyzes it (threshold violations, baseline calculation, ...) and passes it further to Writer
     */
    public class Analyzer
    {
        private static Manager mgr;
        private static Configuration cfg;
        private static Logger _logger = LogManager.GetLogger("Analyzer");

        private static ConcurrentQueue<ProbeResultsDataMessage> _dataQueue;
        private static bool _shouldStop;

        public Analyzer(Manager manager)
        {
            if (mgr == null)
                mgr = manager;

            cfg = mgr.GetConfiguration();

            _dataQueue = new ConcurrentQueue<ProbeResultsDataMessage>();
        }

        public static void Enqueue(Target target, MetricGroup metricGroup, ProbeResultingData newData)
        {
            var msg = new ProbeResultsDataMessage(target, metricGroup, newData);
            _dataQueue.Enqueue(msg);
        }

        // stops execution of ProcessRequests
        public void RequestStop()
        {
            _logger.Info("Stopping Analyzer");
            _shouldStop = true;
        }

        // method that runs continuously and processes messages in the queue
        public void ProcessQueue()
        {
            int count = 0;
            ProbeResultsDataMessage msg;

            try
            {
                _logger.Info("Analyzer started");

                while (!_shouldStop)
                {
                    if (_dataQueue.TryDequeue(out msg))
                    {
                        // Skip empty results
                        if (msg.Data.NumberOfRows == 0)
                            continue;

                        Writer.Enqueue(msg.Target, msg.MetricGroup, msg.Data);

                        count++;
                        _logger.Debug("{0} messages processed. {1} in queue. Date of data being processed: {2}", count, _dataQueue.Count, msg.Data.probeDateTime);
                    }
                    else
                        Thread.Sleep(50);
                }

                _logger.Info("Analyzer stopped");
            }
            catch (Exception e)
            {
                _logger.Error(e.Message);
                _logger.Error(e.StackTrace);
                mgr.ReportFailure("Analyzer");
            }
        }
    } // end of Analyzer class
}
