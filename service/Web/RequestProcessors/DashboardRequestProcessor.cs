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

namespace SqlWristband.Web.RequestProcessors
{
    using NLog;
    using System;

    public class DashboardRequestProcessor : BaseRequestProcessor, IRequestProcessor
    {
        public DashboardRequestProcessor(Configuration cfg) : base(cfg)
        {
            _logger = LogManager.GetLogger("DashboardRequestProcessor");
        }

        public WebServiceResult ProcessRequest(string function, string[] parameters, string postData)
        {
            try
            {
                return StatusTable();
            }
            catch (Exception e)
            {
                _logger.Error(e.Message);
                _logger.Error(e.StackTrace);
                return WebServiceResult.ReturnError(e.Message);
            }
        }

        private WebServiceResult StatusTable()
        {
            DataRow cfgData, cpuIoData;
            TimeSpan sinceLastPoll;
            string strSinceLastPoll;
            string tableContent = "[";

            foreach (int targetId in Configuration.targets.Keys)
            {
                InMemoryCache.GetCurrentValues(
                        targetId,
                        Configuration.metricGroups["SQL Server Configuration"],
                        new string[] { "Version", "Edition" },
                        out cfgData);

                InMemoryCache.GetCurrentValues(
                        targetId,
                        Configuration.metricGroups["SQL Server Activity"],
                        new string[] { "CPU mils", "Physical Reads", "Physical Writes" },
                        out cpuIoData);

                sinceLastPoll = DateTime.Now - Configuration.timeTable.GetLastPoll(targetId);

                if (sinceLastPoll.Days > 0)
                    strSinceLastPoll = String.Format("{0} day(s)", sinceLastPoll.Days);
                else if (sinceLastPoll.Hours > 0)
                    strSinceLastPoll = String.Format("{0} hr(s)", sinceLastPoll.Hours);
                else if (sinceLastPoll.Minutes > 0)
                    strSinceLastPoll = String.Format("{0} min(s)", sinceLastPoll.Minutes);
                else
                    strSinceLastPoll = String.Format("{0} sec(s)", sinceLastPoll.Seconds);

                tableContent +=
@"{" + Environment.NewLine +
@"""id"": """ + targetId + @"""," + Environment.NewLine +
@"""serverName"": """ + Configuration.targets[targetId].name + @"""," + Environment.NewLine +
@"""version"": """ + cfgData["Version"].ToString() + @"""," + Environment.NewLine +
@"""edition"": """ + cfgData["Edition"].ToString() + @"""," + Environment.NewLine +
@"""topWait"": """ + GetMaxValueMultiRowCumulative(targetId, Configuration.metricGroups["SQL Server Wait Stats"], "Wait Time ms", new string[1] { "Wait Type" }) + @"""," + Environment.NewLine +
@"""topFileRead"": """ + GetMaxValueMultiRowCumulative(targetId, Configuration.metricGroups["SQL Server File Stats"], "Number of reads", new string[2] { "Database Name", "Logical File Name" }) + @"""," + Environment.NewLine +
@"""topFileWrite"": """ + GetMaxValueMultiRowCumulative(targetId, Configuration.metricGroups["SQL Server File Stats"], "Number of writes", new string[2] { "Database Name", "Logical File Name" }) + @"""," + Environment.NewLine +
@"""cpu"": """ + cpuIoData["CPU mils"].ToString() + @"""," + Environment.NewLine +
@"""ioReads"": """ + cpuIoData["Physical Reads"].ToString() + @"""," + Environment.NewLine +
@"""ioWrites"": """ + cpuIoData["Physical Writes"].ToString() + @"""," + Environment.NewLine +
@"""lastPoll"": """ + strSinceLastPoll + @"""" + Environment.NewLine +
@"},";
            }

            if (Configuration.targets.Keys.Count > 0)
                tableContent = tableContent.Remove(tableContent.Length - 1); // remove last comma

            tableContent += "]";

            return WebServiceResult.ReturnSuccess(tableContent.Replace(@"\", @"\\"));
        }

        private string GetMaxValueMultiRowCumulative(int targetId, MetricGroup metricGroup, string metric, string[] keysToReturn)
        {
            object[] keys;
            string maxValue = string.Empty;

            if (!InMemoryCache.ContainsKey(InMemoryCache.GetCacheKey(targetId, metricGroup, CacheType.Data)))
                InMemoryCache.LoadDataIntoCache(targetId, metricGroup, false);

            CacheTable dataCache = Configuration.inMemoryCache[InMemoryCache.GetCacheKey(targetId, metricGroup, CacheType.Data)];

            int id = dataCache.GetIdOfMaxValue(metric, metricGroup.metrics[metricGroup.GetMetricIdByName(metric)].type);
            if (id == -1)
                return maxValue;

            if (!InMemoryCache.ContainsKey(InMemoryCache.GetCacheKey(targetId, metricGroup, CacheType.Dictionary)))
                InMemoryCache.LoadDictionaryIntoCache(targetId, metricGroup, false);

            CacheTable dictCache = Configuration.inMemoryCache[InMemoryCache.GetCacheKey(targetId, metricGroup, CacheType.Dictionary)];

            int keyId;

            foreach (string keyName in keysToReturn)
            {
                keyId = metricGroup.GetKeyIdByName(keyName);
                if (keyId != -1)
                {
                    keys = dictCache[id];
                    if (keys == null || keys[keyId] == null)
                        maxValue += " / ";
                    else
                        maxValue += String.Format("{0} / ", keys[keyId]);
                }
                else
                {
                    keyId = metricGroup.GetKeyAttributeIdByName(keyName);
                    keys = dictCache[id];
                    if (keys == null || keys[metricGroup.NumberOfMultiRowKeys + keyId] == null)
                        maxValue += " / ";
                    else
                        maxValue += String.Format("{0} / ", keys[metricGroup.NumberOfMultiRowKeys + keyId]);
                }
            }

            maxValue = maxValue.Remove(maxValue.Length - 3);

            return maxValue;
        }
    }
}
