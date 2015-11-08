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

using SqlWristband.Common;
using SqlWristband.Config;

namespace SqlWristband.Web.RequestProcessors
{
    using NLog;
    using System;
    using System.Text.RegularExpressions;

    public class ConfigurationRequestProcessor : BaseRequestProcessor, IRequestProcessor
    {
        public ConfigurationRequestProcessor(Configuration cfg) : base(cfg)
        {
            _logger = LogManager.GetLogger("ConfigurationRequestProcessor");
        }

        public WebServiceResult ProcessRequest(string function, string[] parameters, string postData)
        {
            try
            {
                switch(function)
                {
                    case "addTarget":
                        return AddTarget(postData);
                    case "deleteTarget":
                        return DeleteTarget(parameters);
                    case "getMetricGroupSettings":
                        return MetricGroupTable();
                    case "getServiceAccount":
                        return GetServiceAccount();
                    case "updateMetricGroupSettings":
                       return UpdateMetricGroups(postData);
                    case "updateMetricGroupScript":
                       return UpdateMetricGroupScript(postData);
                    default:
                       return WebServiceResult.ReturnError(String.Format("Unknown method: [{0}].", function));
                }
            }
            catch (Exception e)
            {
                _logger.Error(e.Message);
                _logger.Error(e.StackTrace);
                return WebServiceResult.ReturnError(e.Message);
            }
        }

        /// <summary>Adds new target(s). Checks that server name format is correct FDQN(\InstanceName)</summary>
        /// <param name="rawPostData">Raw HTTP POST data</param>
        /// <returns>Newline-separated list of errors (if any)</returns>
        private WebServiceResult AddTarget(string rawPostData)
        {
            bool isSqlAuthentication = false;

            if (string.IsNullOrEmpty(rawPostData))
                return WebServiceResult.ReturnError("POST data cannot be NULL or empty");

            var postData = ParsePostData(rawPostData);

            if (!postData.ContainsKey("auth"))
                return WebServiceResult.ReturnError("Parameter [auth] must be specified");

            string auth = postData["auth"];

            if (!auth.Equals("sql") && !auth.Equals("windows"))
                return WebServiceResult.ReturnError(String.Format("Unknown authentication type [{0}]", auth));

            if (auth.Equals("sql"))
                isSqlAuthentication = true;

            if (!postData.ContainsKey("serverName"))
                return WebServiceResult.ReturnError("Parameter [serverName] must be specified");

            string serverName = postData["serverName"];
            serverName = Base64.Decode(serverName);

            // Host name: http://en.wikipedia.org/wiki/Hostname#Restrictions_on_valid_host_names
            // Instance name: http://technet.microsoft.com/en-us/library/ms143744(v=sql.90).aspx
            Regex r = new Regex(@"^[a-zA-Z0-9][a-zA-Z0-9\.\-]{0,254}(\\[^\\,:;'&#@]{0,16})?$");

            // check that server name format is correct
            if (!r.IsMatch(serverName))
                return WebServiceResult.ReturnError(String.Format("Supplied server name [{0}] is not in correct format - FDQN[\\InstanceName]",
                        serverName));

            string sqlUsername = null;
            string sqlPassword = null;

            if (isSqlAuthentication)
            {
                if (!postData.ContainsKey("username") || !postData.ContainsKey("password"))
                    return WebServiceResult.ReturnError("Both parameters [username] and [password] must be specified for SQL authentication");

                sqlUsername = postData["username"];
                sqlPassword = postData["password"];

                sqlUsername = Base64.Decode(sqlUsername);
            }

            _cfg.AddTarget(serverName, isSqlAuthentication, sqlUsername, sqlPassword);

            return WebServiceResult.ReturnSuccess();
        } // end of AddTarget method

        /// <summary>Deletes target(s). Checks that passed parameters are numeric</summary>
        /// <param name="parameters">List of target id(s)</param>
        /// <returns>Newline-separated list of errors (if any)</returns>
        private WebServiceResult DeleteTarget(string[] parameters)
        {
            bool isFirstError = true;
            string errors = "[";

            Regex r = new Regex(@"[0-9]*");

            for (int i = 0; i < parameters.Length; i++)
            {
                // check that server name format is correct
                if (!r.IsMatch(parameters[i]))
                {
                    errors += String.Format(
                            @"{0}{{""Target"":""{1}"", ""Error"":""Supplied server name was not in correct format - FDQN(\\InstanceName)""}}",
                            isFirstError ? String.Empty : ",",
                            parameters[i].Replace("\\", "\\\\"));
                }
                else
                {
                    int targetId = Int32.Parse(parameters[i]);

                    if (!_cfg.DeleteTarget(targetId))
                    {
                        errors += String.Format(
                                @"{0}{{""Target"":""{1}"", ""Error"":""Could not delete target""}}",
                                isFirstError ? String.Empty : ",",
                                parameters[i].Replace("\\", "\\\\"));
                    }
                }

                isFirstError = false;
            }

            errors += "]";

            return WebServiceResult.ReturnSuccess(errors);
        } // end of DeleteTarget method

        private WebServiceResult MetricGroupTable()
        {
            string tableContent = "[";

            foreach (MetricGroup mg in Configuration.metricGroups)
            {
                tableContent +=
@"{" + Environment.NewLine +
@"""id"": """ + mg.id.ToString() + @"""," + Environment.NewLine +
@"""name"": """ + mg.name + @"""," + Environment.NewLine +
@"""interval"": """ + mg.defaultSchedule.interval.ToString() + @"""," + Environment.NewLine +
@"""retention"": """ + mg.defaultSchedule.retention.ToString() + @"""," + Environment.NewLine +
@"""script"": """ + Convert.ToBase64String(System.Text.Encoding.UTF8.GetBytes(mg.scriptText)) + @"""" + Environment.NewLine +
@"},";
            }

            tableContent = tableContent.Remove(tableContent.Length - 1); // remove last comma
            tableContent += "]";

            return WebServiceResult.ReturnSuccess(tableContent.Replace(@"\", @"\\"));
        }

        private WebServiceResult UpdateMetricGroups(string postDataRaw)
        {
            int metricGroupId;
            int interval;
            int retention;

            if (string.IsNullOrEmpty(postDataRaw))
                return WebServiceResult.ReturnError("POST data cannot be NULL or empty");

            string postData = DecodeUriCharacters(postDataRaw);

            // Split into separate items one per metric group (metricGroupId[interval]=value&metricGroupId[retention]=value)
            foreach (Match lineMatch in Regex.Matches(postData, @"([0-9]*\[interval*\]=\-?[0-9]*&[0-9]*\[retention\]=\-?[0-9]*)"))
            {
                _logger.Debug(lineMatch.Value);

                // Extract metricGroupId, intervalValue and retentionValue
                foreach (Match itemsMatch in Regex.Matches(lineMatch.Value, @"([0-9]*)\[interval*\]=(\-?[0-9]*)&[0-9]*\[retention\]=(\-?[0-9]*)"))
                {
                    if (itemsMatch.Groups.Count != 4)
                        return WebServiceResult.ReturnError(@"Data is not in ([0-9]*)\[interval*\]=(\-?[0-9]*)&[0-9]*\[retention\]=(\-?[0-9]*) format");

                    metricGroupId = Int32.Parse(itemsMatch.Groups[1].Value);
                    interval = Int32.Parse(itemsMatch.Groups[2].Value);
                    retention = Int32.Parse(itemsMatch.Groups[3].Value);

                    _cfg.UpdateMetricGroupConfiguration(metricGroupId, interval, retention);
                }
            }

            return WebServiceResult.ReturnSuccess();
        }

        private WebServiceResult UpdateMetricGroupScript(string postDataRaw)
        {
            int metricGroupId;
            string scriptBase64;
            string script;
            string errorMsg;

            if (string.IsNullOrEmpty(postDataRaw))
                return WebServiceResult.ReturnError("POST data cannot be NULL or empty");

            string postData = DecodeUriCharacters(postDataRaw);

            _logger.Debug(postData);

            try
            {
                // Extract metricGroupId and script (Base64 encoded)
                foreach (Match itemsMatch in Regex.Matches(postData, @"id=([0-9]*)&script=([0-9a-zA-Z+/=]*)"))
                {
                    if (itemsMatch.Groups.Count != 3)
                        return WebServiceResult.ReturnError(@"Data is not in ([0-9]*)\[interval*\]=(\-?[0-9]*)&[0-9]*\[retention\]=(\-?[0-9]*) format");

                    metricGroupId = Int32.Parse(itemsMatch.Groups[1].Value);
                    scriptBase64 = itemsMatch.Groups[2].Value;
                    _logger.Debug(scriptBase64);

                    script = Base64.Decode(scriptBase64);

                    _logger.Debug(script);

                    if (!_cfg.UpdateMetricGroupScript(metricGroupId, script, out errorMsg))
                        return WebServiceResult.ReturnError(errorMsg);
                }

                return WebServiceResult.ReturnSuccess();
            }
            catch (Exception e)
            {
                _logger.Error(e.Message);
                return WebServiceResult.ReturnError(e.Message);
            }
        }

        private WebServiceResult GetServiceAccount()
        {
            return WebServiceResult.ReturnSuccess(System.Security.Principal.WindowsIdentity.GetCurrent().Name);
        }

    } // end of ConfigurationRequestProcessor class
}
