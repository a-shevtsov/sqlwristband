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

namespace SqlWristband.Web.RequestProcessors
{
    using NLog;
    using System;
    using System.Linq;
    using System.Text.RegularExpressions;

    public class FastMultiMetricRequestProcessor : BaseRequestProcessor, IRequestProcessor
    {
        public FastMultiMetricRequestProcessor(Configuration config) : base(config)
        {
            _logger = LogManager.GetLogger("FastMultiMetricRequestProcessor");
        }

        public WebServiceResult ProcessRequest(string function, string[] parameters, string postData)
        {
            try
            {
                switch (function)
                {
                    case "Range":
                        return Range(parameters);
                    default:
                        return WebServiceResult.ReturnError(GetType().Name + ".ProcessRequest(): function " + function + " could not be found");
                }
            }
            catch (Exception e)
            {
                _logger.Error(e.Message);
                _logger.Error(e.StackTrace);
                return WebServiceResult.ReturnError(e.Message);
            }
        }

        // returns a set of datetime-value tuples in a JSON string
        // param0: MetricGroupName - MetricGroup->Name
        // param1: TargetId - Targets->Id
        // param2: StartDateTime - YYYYMMDDHHMM
        // param3: EndDateTime - YYYYMMDDHHMM
        // param4: Interval - M in minutes
        // param5: MetricName - MetricGroup->metrics->name
        // param6: NumOfRowsToReturn - Number of records to return (TOP X)
        // param7: DictionaryKeyName - MetricGroup->multiRowKeys->name
        // param8: Optional. Dictionary keys to exclude
        // test string ok     http://localhost:3128/ws/fastmulti/Range/SQL%20Server%20Wait%20Stats/0/201305201800/201305201900/5/Wait%20Time%20ms/5/Wait%20Type/
        // test string not ok http://localhost:3128/ws/fastmulti/Range/SQL%20Server%20Wait%20Stats/0/201305201800/201305M900/5/Wait%20Time%20ms/10/Wait%20Type/
        private WebServiceResult Range(string[] parameters)
        {
            if (parameters.Count() < 8)
                return WebServiceResult.ReturnError(GetType().Name + ".Range(): too few parameters. Format is MetricGroupName/TargetId/StartDateTime/EndDateTime/IntervalInMinutes/MetricName/DictionaryKeyName/[DictionaryKeysToExclude]");

            if (parameters.Count() > 9)
                return WebServiceResult.ReturnError(GetType().Name + ".Range(): too many parameters. Format is MetricGroupName/TargetId/StartDateTime/EndDateTime/IntervalInMinutes/MetricName/numOfRowsToReturn/DictionaryKeyName/[DictionaryKeysToExclude]");

            Regex r = new Regex("^[0-9]*$");

            // check that supplied target id is valid
            if (!r.IsMatch(parameters[1]) || !Configuration.targets.ContainsId(Convert.ToInt32(parameters[1])))
                return WebServiceResult.ReturnError(GetType().Name + ".Range(): TargetId is either not numeric or target with specified id doesn't exist");

            if (!r.IsMatch(parameters[2]) || !r.IsMatch(parameters[3]) || !r.IsMatch(parameters[4]))
                return WebServiceResult.ReturnError(GetType().Name + ".Range(): TargetId or StartDateTime or EndDateTime or Interval is not numeric. Format is MetricGroupName/TargetId/StartDateTime/EndDateTime/IntervalInMinutes/MetricName/DictionaryKeyName/[DictionaryKeysToExclude]");

            if (parameters[2].Length != 12 || parameters[3].Length != 12)
                return WebServiceResult.ReturnError(GetType().Name + ".Range(): StartDateTime and EndDateTime must be in YYYYMMDDHHMM format");

            if (!r.IsMatch(parameters[6]))
                return WebServiceResult.ReturnError(GetType().Name + ".Range(): NumOfRowsToReturn is not numeric");

            // look up metric group by name
            var metricGroup = Configuration.metricGroups[parameters[0]];

            string metricColumn = parameters[5].Replace(' ', '_');
            string numOfRowsToReturn = parameters[6];
            string exclusionColumn = parameters[7].Replace(' ', '_');
            string excludedValues = string.Empty;

            if (parameters.Count() == 9)
                excludedValues = parameters[8];

            // prepare parameters
            SqlParameters sqlParameters = new SqlParameters
            {
                { "@dataTable", SqlServerProbe.DataTableName(Convert.ToInt32(parameters[1]), metricGroup) },
                { "@dictionary", SqlServerProbe.DictTableName(Convert.ToInt32(parameters[1]), metricGroup) },
                { "@start_dt", SqlServerProbe.FormatDate(parameters[2]) },
                { "@end_dt", SqlServerProbe.FormatDate(parameters[3]) },
                { "@interval", parameters[4] },
                { "@metricColumn", metricColumn },
                { "@numOfRowsToReturn", numOfRowsToReturn },
                { "@exclusionColumn", exclusionColumn },
                { "@excludedValues", excludedValues }
            };

            // execute procedure and return results
            if (metricGroup.isCumulative)
                return GetData(System.Data.CommandType.StoredProcedure, "dbo.GetRangeMultiRowFastCumulative", sqlParameters);

            return GetData(System.Data.CommandType.StoredProcedure, "dbo.GetRangeMultiRowFastNonCumulative", sqlParameters);
        }
    }
}
