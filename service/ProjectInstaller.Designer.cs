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
    partial class ProjectInstaller
    {
        /// <summary>
        /// Required designer variable.
        /// </summary>
        private System.ComponentModel.IContainer components = null;

        /// <summary> 
        /// Clean up any resources being used.
        /// </summary>
        /// <param name="disposing">true if managed resources should be disposed; otherwise, false.</param>
        protected override void Dispose(bool disposing)
        {
            if (disposing && (components != null))
            {
                components.Dispose();
            }
            base.Dispose(disposing);
        }

        #region Component Designer generated code

        /// <summary>
        /// Required method for Designer support - do not modify
        /// the contents of this method with the code editor.
        /// </summary>
        private void InitializeComponent()
        {
            this.SqlWristbandSvcProcessInstaller = new System.ServiceProcess.ServiceProcessInstaller();
            this.SqlWristbandSvcInstaller = new System.ServiceProcess.ServiceInstaller();
            // 
            // SqlWristbandSvcProcessInstaller
            // 
            this.SqlWristbandSvcProcessInstaller.Password = null;
            this.SqlWristbandSvcProcessInstaller.Username = null;
            // 
            // SqlWristbandSvcInstaller
            // 
            this.SqlWristbandSvcInstaller.Description = "SQL Server Activity Tracker";
            this.SqlWristbandSvcInstaller.DisplayName = "SQL Wristband";
            this.SqlWristbandSvcInstaller.ServiceName = "SqlWristbandSvc";
            this.SqlWristbandSvcInstaller.AfterInstall += new System.Configuration.Install.InstallEventHandler(this.serviceInstaller1_AfterInstall);
            // 
            // ProjectInstaller
            // 
            this.Installers.AddRange(new System.Configuration.Install.Installer[] {
            this.SqlWristbandSvcProcessInstaller,
            this.SqlWristbandSvcInstaller});

        }

        #endregion

        private System.ServiceProcess.ServiceProcessInstaller SqlWristbandSvcProcessInstaller;
        private System.ServiceProcess.ServiceInstaller SqlWristbandSvcInstaller;
    }
}