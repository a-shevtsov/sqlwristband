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
    using System;
    using System.ServiceProcess;

    // This is the entry point. Main function is located here and it starts and controls the rest of the program
    public class SqlWristband
    {
        static void Main(string[] args)
        {
            bool consoleMode = false;

            SqlWristbandSvc service;

            // Parse arguments
            if (args.Length > 0)
            {
                for (int i = 0; i < args.Length; i++)
                {
                    switch (args[i].ToLower())
                    {
                        case "/install":
                            service = new SqlWristbandSvc();
                            service.InstallService();
                            service.Dispose();
                            return;
                        case "/uninstall":
                            service = new SqlWristbandSvc();
                            service.UninstallService();
                            service.Dispose();
                            return;
                        case "/console":
                            consoleMode = true;
                            break;
                    }
                }
            }

            // Launch the service in either console or Windows service mode
            if (consoleMode)
            {
                Manager manager = new Manager();

                Console.WriteLine("Starting worker threads...");
                manager.Initialize();
                Console.WriteLine("<Press any key to exit...>");
                Console.Read();

                manager.Stop();
            }
            else
            {
                service = new SqlWristbandSvc();
                var servicesToRun = new ServiceBase[] { service };
                ServiceBase.Run(servicesToRun);
            }
        } // end of Main method
    } // end of SqlWristband class
} // end of namespace
