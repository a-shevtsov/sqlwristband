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
using SqlWristband.Data;

namespace SqlWristband.Probes
{
    using NLog;
    using System;
    using System.Data.SqlClient;

    // universal message structure used to pass data over queues. Data will be adjusted by each metric we store just a pointer
    public class ProbeResultsDataMessage
    {
        public Target Target;
        public MetricGroup MetricGroup;
        public ProbeResultingData Data;

        public ProbeResultsDataMessage(Target target, MetricGroup metricGroup, ProbeResultingData data)
        {
            this.Target = target;
            this.MetricGroup = metricGroup;
            this.Data = data;
        }
    }

    public class ProbeResultingCallbackStateObject
    {
        public int TimeTableId;
        public Target Target;
        public MetricGroup MetricGroup;
        public SqlCommand SqlCommand;

        public ProbeResultingCallbackStateObject(int timeTableId, Target target, MetricGroup metricGroup, SqlCommand sqlCommand)
        {
            this.TimeTableId = timeTableId;
            this.Target = target;
            this.MetricGroup = metricGroup;
            this.SqlCommand = sqlCommand;
        }
    }

    public class ProbeResultingData
    {
        private int indexOfLastSetHeader;
        public DateTime probeDateTime; // when data was retrieved from patient
        public ushort NumberOfRows;
        public ushort NumberOfColumns;
        public string[] names; // column names
        public DataType[] dataTypes; // data type of each column
        public object[,] values; // matrix containing values (single row for single-value metrics and multiple rows for multi-value metrics)

        public ProbeResultingData(ushort numOfRows, ushort numOfColumns)
        {
            this.NumberOfRows = numOfRows;
            this.NumberOfColumns = numOfColumns;
            this.indexOfLastSetHeader = -1;
            this.names = new string[numOfColumns];
            this.dataTypes = new DataType[numOfColumns];
            this.values = new object[numOfRows, numOfColumns];
        }

        public void ChangeNumOfRows(ushort numOfRows)
        {
            this.ResizeArray<object>(ref this.values, numOfRows);
            this.NumberOfRows = numOfRows;
        }

        public void SetProbeDateTime(DateTime dt)
        {
            this.probeDateTime = dt;
        }

        public void AddColumnHeader(string name, DataType dataType)
        {
            this.indexOfLastSetHeader++;
            this.names[this.indexOfLastSetHeader] = name;
            this.dataTypes[this.indexOfLastSetHeader] = dataType;
        }

        private void ResizeArray<T>(ref T[,] original, int newNumOfRows)
        {
            T[,] newArray = new T[newNumOfRows, this.NumberOfColumns];

            int rowsToCopy;

            if (this.NumberOfRows >= newNumOfRows)
                rowsToCopy = newNumOfRows;
            else
                rowsToCopy = this.NumberOfRows;

            Array.Copy(original, newArray, rowsToCopy * this.NumberOfColumns);

            original = newArray;
        }
    }

    public abstract class ProbeBase
    {
        protected static Configuration _cfg;
        protected static Logger _logger;

        // this method is called by Probe under Scheduler
        public abstract void Probe(int timeTableId, Target target, MetricGroup metricGroup);

        // this method is called when query execution is finished
        public abstract void ProcessResults(IAsyncResult result);
    }
}
