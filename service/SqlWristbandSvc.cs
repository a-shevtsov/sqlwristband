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

namespace SqlWristband
{
    using NLog;
    using System;
    using System.Configuration.Install;
    using System.Linq;
    using System.Runtime.InteropServices;
    using System.ServiceProcess;

    public enum ServiceState
    {
        SERVICE_STOPPED = 0x00000001,
        SERVICE_START_PENDING = 0x00000002,
        SERVICE_STOP_PENDING = 0x00000003,
        SERVICE_RUNNING = 0x00000004,
        SERVICE_CONTINUE_PENDING = 0x00000005,
        SERVICE_PAUSE_PENDING = 0x00000006,
        SERVICE_PAUSED = 0x00000007,
    }

    [StructLayout(LayoutKind.Sequential)]
    public struct ServiceStatus
    {
        public long dwServiceType;
        public ServiceState dwCurrentState;
        public long dwControlsAccepted;
        public long dwWin32ExitCode;
        public long dwServiceSpecificExitCode;
        public long dwCheckPoint;
        public long dwWaitHint;
    };

    public partial class SqlWristbandSvc : ServiceBase
    {
        private static Manager mgr;

        public const string SERVICE_NAME = "SqlWristbandSvc";
        public const string SERVICE_NAME_LONG = "SQL Wristband";

        private static Logger _logger = LogManager.GetLogger("SqlWristbandSvc");

        [DllImport("advapi32.dll", SetLastError = true)]
        private static extern bool SetServiceStatus(IntPtr handle, ref ServiceStatus serviceStatus);

        public SqlWristbandSvc()
        {
            InitializeComponent();
        }

        protected override void OnStart(string[] args)
        {
            _logger.Info("Starting service");

            // Update the service state to Start Pending.
            ServiceStatus serviceStatus = new ServiceStatus();
            serviceStatus.dwCurrentState = ServiceState.SERVICE_START_PENDING;
            serviceStatus.dwWaitHint = 100000;
            SetServiceStatus(this.ServiceHandle, ref serviceStatus);

            try
            {
                mgr = new Manager();
                mgr.Initialize();

                // Update the service state to Running.
                serviceStatus.dwCurrentState = ServiceState.SERVICE_RUNNING;
                SetServiceStatus(this.ServiceHandle, ref serviceStatus);
            }
            catch (Exception e)
            {
                _logger.FatalException("Service failed to start", e);

                mgr.Stop();

                // Stop the service
                ServiceController service = new ServiceController(SERVICE_NAME);
                service.Stop();
            }
        }

        protected override void OnStop()
        {
            mgr.Stop();

            // stop the service/program if stop was initiated from within
            if (!Environment.UserInteractive)
            {
                ServiceController service = new ServiceController(SERVICE_NAME);

                if (service.Status != ServiceControllerStatus.StopPending)
                    service.Stop();
            }
        }

        private bool IsServiceInstalled()
        {
            return ServiceController.GetServices().Any(s => s.ServiceName == SERVICE_NAME);
        }

        public void InstallService()
        {
            if (this.IsServiceInstalled())
                this.UninstallService();

            ManagedInstallerClass.InstallHelper(new string[] { System.Reflection.Assembly.GetExecutingAssembly().Location });
        }

        public void UninstallService()
        {
            ManagedInstallerClass.InstallHelper(new string[] { "/u", System.Reflection.Assembly.GetExecutingAssembly().Location });
        }

    } // End of SqlWristbandSvc class
}
