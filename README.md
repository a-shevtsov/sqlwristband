# SQL Wristband
Monitoring tool for SQL Server

SQL Wristband collects a few important SQL Server performance metrics.
It was designed to monitor Wait and File statistics but it also tracks disk utilization, CPU and I/O stalls.

The tool is a small Windows service with the following features:
 * Agentless
 * Does not require sysadmin role to collect statistics
 * Low overhead on monitored SQL Server instances
 * Web-based client
 * Space-efficient repository

### Screenshots

#### Wait Stats

![Wait Stats](/screenshots/Wait_Stats.png)

#### File Stats

![File Stats](/screenshots/File_Stats.png)

#### CPU and I/O

![CPU and I/O](/screenshots/CPU_IO.png)

### Installation
SQL Wristband Installation Guide

 * Install [.NET Framework 64-bit 4.5](https://www.microsoft.com/en-us/download/details.aspx?id=30653)
 * Log in with account that is a member of the sysadmin server role on an SQL Server instance where sqlwristband repository is to be created.
 * Launch SqlWristbandSetup.msi and install the tool. sysadmin role can be revoked once installation is complete.
 * Add a rule in Inbound Rules section of Windows Firewall for TCP port 2112.
 * Open http://localhost:2112 or http://servername:2112 URL in your favorite browser.
 * Add SQL Server instances you want to monitor.
