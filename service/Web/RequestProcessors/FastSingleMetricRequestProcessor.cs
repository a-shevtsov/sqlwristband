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

    public class FastSingleMetricRequestProcessor : BaseRequestProcessor, IRequestProcessor
    {
        public FastSingleMetricRequestProcessor(Configuration config) : base(config)
        {
            _logger = LogManager.GetLogger("FastSingleMetricRequestProcessor");
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
                _logger.Debug(e.StackTrace);
                return WebServiceResult.ReturnError(e.Message);
            }
        }

        // returns a set of datetime-value tuples in a JSON string
        // param0: MetricGroupName - MetricGroup->Name
        // param1: MetricName - MetricGroup->Metrics->Name
        // param2: TargetId - Targets->Id
        // param3: StartDateTime - YYYYMMDDHHMM
        // param4: EndDateTime - YYYYMMDDHHMM
        // param5: Interval - M in minutes
        // test string ok http://localhost:3128/ws/fastsingle/Range/SQL%20Server%20Activity/CPU%20mils/0/201305201800/201305201900/5
        // test string not ok http://localhost:3128/ws/fastsingle/Range/SQL%20Server%20Activity/CPU%20mils/0/201305201800/201305M900/5
        private WebServiceResult Range(string[] parameters)
        {
            if (parameters.Count() < 6)
                return WebServiceResult.ReturnError(GetType().Name + ".Range(): not enough parameters. Format is MetricGroupName/MetricName/TargetId/StartDateTime/EndDateTime/IntervalInMinutes");

            Regex r = new Regex("^[0-9]*$");

            // check that supplied target id is valid
            if (!r.IsMatch(parameters[2]) || !Configuration.targets.ContainsId(Convert.ToInt32(parameters[2])))
                return WebServiceResult.ReturnError(GetType().Name + ".Range(): TargetId is either not numeric or doesn't exist");

            if (!r.IsMatch(parameters[3]) || !r.IsMatch(parameters[4]) || !r.IsMatch(parameters[5]))
                return WebServiceResult.ReturnError(GetType().Name + ".Range(): at least one of the parameters is not numeric. Format is TargetId/StartDateTime/EndDateTime/IntervalInMinutes");

            if (parameters[3].Length != 12 || parameters[4].Length != 12)
                return WebServiceResult.ReturnError(GetType().Name + ".Range(): StartDateTime and EndDateTime must be in YYYYMMDDHHMM format");

            // look up metric group by name
            int metricGroupId = Configuration.metricGroups[parameters[0]].id;

            // look up metric by name
            string columnName = parameters[1].Replace(' ', '_');

            // prepare parameters
            SqlParameters sqlParameters = new SqlParameters
            {
                { "@tablename", SqlServerProbe.DataTableName(Convert.ToInt32(parameters[2]), Configuration.metricGroups[metricGroupId]) },
                { "@columnname", columnName },
                { "@start_dt", SqlServerProbe.FormatDate(parameters[3]) },
                { "@end_dt", SqlServerProbe.FormatDate(parameters[4]) },
                { "@interval", parameters[5] }
            };

            // execute procedure and return results
            return GetData(System.Data.CommandType.StoredProcedure, "dbo.GetRangeSingleRowFastCumulative", sqlParameters);
        }
    }
}
