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

using SqlWristband.Data;

namespace SqlWristband.Config
{
    using System;
    using System.Collections.Generic;

    // description of a metric group. Can containt one or more metrics that can be retrieved by a single probe run
    public class MetricGroup
    {
        public int id;
        public string name; // name of the metric group
        public string probeCode;
        public ChangeSpeed changeSpeed; // Static, Slow or Fast changing value (static/slow/fast)
        public bool isMultiRow;
        public bool isCumulative;
        public string dataTableName;
        public string dictTableName;

        public string scriptText; // text of collection script or query

        public Schedule defaultSchedule;

        // only used if isMultiRow is true
        public Dictionary<int, Column> multiRowKeys;
        public int NumberOfMultiRowKeys;

        public Dictionary<int, Column> multiRowKeyAttributes;
        public int NumberOfMultiRowKeyAttributes;
        public ChangeSpeed multiRowKeyAttributesChangeSpeed; // Static or Slow/Fast changing value (static/slow/fast)

        public Dictionary<int, Column> metrics;
        public int NumberOfMetrics;

        public MetricGroup(int id, string name, string probeCode, ChangeSpeed changeSpeed, bool isMultiRow, bool isCumulative, ChangeSpeed multiRowKeyAttributesChangeSpeed = ChangeSpeed.NotApplicable)
        {
            this.id = id;
            this.name = name;

            this.metrics = new Dictionary<int, Column>();
            this.NumberOfMetrics = 0;
            this.multiRowKeys = new Dictionary<int, Column>();
            this.NumberOfMultiRowKeys = 0;
            this.multiRowKeyAttributes = new Dictionary<int, Column>();
            this.NumberOfMultiRowKeyAttributes = 0;
            this.multiRowKeyAttributesChangeSpeed = multiRowKeyAttributesChangeSpeed;

            this.probeCode = probeCode;
            this.changeSpeed = changeSpeed;
            this.isMultiRow = isMultiRow;
            this.isCumulative = isCumulative;

            // table names
            this.dataTableName = this.probeCode + "_" + this.id.ToString("D9") + "_data_";

            if (this.isMultiRow)
                this.dataTableName += "multi_";
            else
                this.dataTableName += "single_";

            switch (this.changeSpeed)
            {
                case ChangeSpeed.Static: this.dataTableName += "static"; break;
                case ChangeSpeed.Slow: this.dataTableName += "slow"; break;
                case ChangeSpeed.Fast: this.dataTableName += "fast"; break;
            }

            if (this.isMultiRow)
            {
                this.dictTableName = this.probeCode + "_" + this.id.ToString("D9") + "_dict_";
                switch (this.multiRowKeyAttributesChangeSpeed)
                {
                    case ChangeSpeed.Static: this.dictTableName += "static"; break;
                    case ChangeSpeed.Slow: this.dictTableName += "slow"; break;
                    case ChangeSpeed.Fast: this.dictTableName += "fast"; break;
                }
            }
        }

        public void Clear()
        {
            this.multiRowKeys.Clear();
            this.multiRowKeyAttributes.Clear();
            this.metrics.Clear();

            this.NumberOfMetrics = 0;
            this.NumberOfMultiRowKeys = 0;
            this.NumberOfMultiRowKeyAttributes = 0;
        }

        /// <summary>
        /// Returns metric column metadata by name
        /// Searches metrics only
        /// </summary>
        /// <param name="metricName">Name of metric</param>
        public Column this[string metricName]
        {
            get
            {
                foreach (int key in this.metrics.Keys)
                {
                    if (this.metrics[key].name.Equals(metricName))
                        return this.metrics[key];
                }

                throw new Exception(string.Format("Metric '{0}' could not be found in '{1}' metric group.", metricName, this.name));
            }
        }

        public void AddMetric(string name, DataType dataType)
        {
            this.metrics.Add(this.NumberOfMetrics++, new Column(name, dataType));
        }

        public void AddMultiRowKey(string name, DataType dataType)
        {
            this.multiRowKeys.Add(this.NumberOfMultiRowKeys++, new Column(name, dataType));
        }

        public void AddMultiRowKeyAttribute(string name, DataType dataType)
        {
            this.multiRowKeyAttributes.Add(this.NumberOfMultiRowKeyAttributes++, new Column(name, dataType));
        }

        public int GetMetricIdByName(string name)
        {
            for (int i = 0; i < this.NumberOfMetrics; i++)
            {
                if (this.metrics[i].name.CompareTo(name) == 0)
                    return i;
            }

            return -1;
        }

        public int GetKeyIdByName(string name)
        {
            for (int i = 0; i < this.NumberOfMultiRowKeys; i++)
            {
                if (this.multiRowKeys[i].name.CompareTo(name) == 0)
                    return i;
            }

            return -1;
        }

        public int GetKeyAttributeIdByName(string name)
        {
            for (int i = 0; i < this.NumberOfMultiRowKeyAttributes; i++)
            {
                if (this.multiRowKeyAttributes[i].name.CompareTo(name) == 0)
                    return i;
            }

            return -1;
        }
    } // end of MetricGroup class
}
