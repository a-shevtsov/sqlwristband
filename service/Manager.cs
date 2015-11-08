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
    using System.Collections.Concurrent;
    using System.Threading;

    public class Manager
    {
        private static readonly object _lock = new object();

        private static Logger _logger = LogManager.GetLogger("Manager");

        private volatile static bool _isRepositoryAccessible;
        public static bool IsRepositoryAccessible { get { return _isRepositoryAccessible; } }

        private static DateTime _repositoryLastConnectionAttempt;

        private static bool _shouldStop;

        // Names of threads that had unhandled exceptions
        private static ConcurrentQueue<string> _failedThreads;

        private static Configuration _cfg;

        private static Analyzer _analyzer;
        private static Writer _writer;
        private static WebServer _webServer;
        private static Scheduler _scheduler;
        private static Purger _purger;
        private static Archiver _archiver;

        private static Dictionary<string, Thread> _threads;

        public Manager()
        {
            _shouldStop = false;
            _isRepositoryAccessible = false;
            _repositoryLastConnectionAttempt = DateTime.MinValue;
            _failedThreads = new ConcurrentQueue<string>();

            _threads = new Dictionary<string, Thread>();
        }

        public void Initialize()
        {
            // Read side-by-side configuration file
            _logger.Debug("Reading configuration");
            _cfg = new Configuration(this);
            _cfg.ReadConfigFromFile();

            // Start watchdog if ReadConfig was successfull
            // It will start the rest of the threads
            _logger.Debug("Starting Watchdog thread");
            Thread _watchdogThread = new Thread(this.Watchdog);
            _watchdogThread.Name = "Watchdog";
            _threads.Add(_watchdogThread.Name, _watchdogThread);
            _watchdogThread.Start();
        }

        public static void SetRepositoryAccessibility(bool isAccessible)
        {
            // Skip the rest if status has not changed
            if (_isRepositoryAccessible == isAccessible)
                return;

            lock (_lock)
            {
                _isRepositoryAccessible = isAccessible;
                _repositoryLastConnectionAttempt = DateTime.Now;
            }

            if (isAccessible)
                _logger.Info("Repository is accessible");
            else
                _logger.Info("Repository is not accessible");
        }

        public void Stop()
        {
            _shouldStop = true;

            _logger.Info("Stopping service");

            // Wait for watchdog thread to stop
            if (_threads.ContainsKey("Watchdog"))
                _threads["Watchdog"].Join();

            // Stop web server thread
            _webServer.RequestStop();
            _threads["WebServer"].Join();
        }

        public Configuration GetConfiguration()
        {
            return _cfg;
        }

        public void ReportFailure(string threadName)
        {
            _failedThreads.Enqueue(threadName);
        }

        private bool StartThreads()
        {
            // Start Web server thread
            _logger.Debug("Starting WebServer thread");
            _webServer = new WebServer(this, _cfg);
            Thread _webServerThread = new Thread(_webServer.ProcessRequests);
            _webServerThread.IsBackground = true;
            _webServerThread.Name = "WebServer";
            _threads.Add(_webServerThread.Name, _webServerThread);
            _webServerThread.Start();

            // Start threads in backward order (Analyzer needs to enqueue messages into Writer queue, ...)

            // Start Archiver thread
            _logger.Debug("Starting Archiver thread");
            _archiver = new Archiver(this, _cfg);
            Thread _archiverThread = new Thread(_archiver.Work);
            _archiverThread.IsBackground = true;
            _archiverThread.Name = "Archiver";
            _threads.Add(_archiverThread.Name, _archiverThread);
            _archiverThread.Start();

            // Start Purger thread
            _logger.Debug("Starting Purger thread");
            _purger = new Purger(this, _cfg);
            Thread _purgerThread = new Thread(_purger.Work);
            _purgerThread.IsBackground = true;
            _purgerThread.Name = "Purger";
            _threads.Add(_purgerThread.Name, _purgerThread);
            _purgerThread.Start();

            // Start Writer thread
            _logger.Debug("Starting Writer thread");
            _writer = new Writer(this, _cfg);
            Thread _writerThread = new Thread(_writer.ProcessQueue);
            _writerThread.IsBackground = true;
            _writerThread.Name = "Writer";
            _threads.Add(_writerThread.Name, _writerThread);
            _writerThread.Start();

            // Start Analyzer thread
            _logger.Debug("Starting Analyzer thread");
            _analyzer = new Analyzer(this);
            Thread _analyzerThread = new Thread(_analyzer.ProcessQueue);
            _analyzerThread.IsBackground = true;
            _analyzerThread.Name = "Analyzer";
            _threads.Add(_analyzerThread.Name, _analyzerThread);
            _analyzerThread.Start();

            // Start Scheduler thread
            _logger.Debug("Starting Scheduler thread");
            _scheduler = new Scheduler(this, _cfg);
            Thread _schedulerThread = new Thread(_scheduler.Launcher);
            _schedulerThread.IsBackground = true;
            _schedulerThread.Name = "Scheduler";
            _threads.Add(_schedulerThread.Name, _schedulerThread);
            _schedulerThread.Start();

            return true;
        } // end of StartThreads method

        private void Watchdog()
        {
            string failedThreadName;
            DateTime lastLogRecord = DateTime.MinValue;

            _logger.Info("Watchdog started");

            // Start all other threads
            _logger.Debug("Starting other threads");
            this.StartThreads();

            while (!_shouldStop)
            {
                if (!_isRepositoryAccessible && DateTime.Now.AddSeconds(-1 * Configuration.reposReconnectRetryInterval) > _repositoryLastConnectionAttempt)
                {
                    try
                    {
                        _cfg.ReadConfigFromRepository();
                        // Reset last log record time so if repo goes down shortly again we will log this
                        lastLogRecord = DateTime.MinValue;
                    }
                    catch (Exception e)
                    {
                        SetRepositoryAccessibility(false);

                        // Write repeat errors 10 minutes apart
                        if (lastLogRecord < DateTime.Now.AddSeconds(-600))
                        {
                            _logger.Info("Cannot connect to repository");
                            _logger.Debug(e.Message);
                            lastLogRecord = DateTime.Now;
                        }
                    }
                }

                while (_failedThreads.Count > 0)
                {
                    _failedThreads.TryDequeue(out failedThreadName);
                    _logger.Error(string.Format("Thread {0} failed", failedThreadName));
                }

                Thread.Sleep(100);
            }

            _logger.Info("Stopping threads");

            this.StopThreads();

            _logger.Info("Watchdog stopped");
        }

        private void StopThreads()
        {
            if (_threads.ContainsKey("Archiver"))
                _archiver.RequestStop();

            if (_threads.ContainsKey("Purger"))
                _purger.RequestStop();

            if (_threads.ContainsKey("Scheduler"))
            {
                _scheduler.RequestStop();

                // give some time to process all outstanding messages
                Thread.Sleep(250);
            }

            if (_threads.ContainsKey("Analyzer"))
            {
                _analyzer.RequestStop();

                Thread.Sleep(250);
            }

            if (_threads.ContainsKey("Writer"))
                _writer.RequestStop();

            // Use the Join method to block the current thread  
            // until the object's thread terminates.
            if (_threads.ContainsKey("Archiver"))
                _threads["Archiver"].Join();

            if (_threads.ContainsKey("Purger"))
                _threads["Purger"].Join();

            if (_threads.ContainsKey("Scheduler"))
                _threads["Scheduler"].Join();

            if (_threads.ContainsKey("Analyzer"))
                _threads["Analyzer"].Join();

            if (_threads.ContainsKey("Writer"))
                _threads["Writer"].Join();
        }
    }
}
