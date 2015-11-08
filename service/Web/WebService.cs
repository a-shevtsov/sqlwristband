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

namespace SqlWristband
{
    using NLog;
    using System;

    public class ParsedUrl
    {
        public bool IsWebServiceRequest;
        public string module;
        public string function;
        public string[] parameters;
    }

    public class WebServiceResult
    {
        public bool Success { get; set; }
        public string Message { get; set; }

        public static WebServiceResult ReturnSuccess()
        {
            return new WebServiceResult{Success = true, Message = null};
        }

        public static WebServiceResult ReturnSuccess(string message)
        {
            return new WebServiceResult { Success = true, Message = message };
        }

        public static WebServiceResult ReturnError(string message)
        {
            return new WebServiceResult { Success = false, Message = message };
        }
    }
    
    public class WebService
    {
        private static Configuration _cfg;
        private static Logger _logger = LogManager.GetLogger("WebService");

        public WebService(Configuration config)
        {
            _cfg = config;
        }

        public string ProcessDynamic(ParsedUrl parsedUrl, string postData)
        {
            _logger.Debug("Module: " + parsedUrl.module);
            _logger.Debug("Function: " + parsedUrl.function);
            foreach (string parameter in parsedUrl.parameters)
                _logger.Debug("Parameter: " + parameter);

            if (!Manager.IsRepositoryAccessible)
            {
                ReturnError(String.Format("Statistics repository [{0}].[{1}] is not accessible",
                                Configuration.reposInstance,
                                Configuration.reposDatabase
                                )
                            );
            }

            // find class name corresponding to requested module
            if (!Configuration.moduleClassMapping.ContainsKey(parsedUrl.module))
            {
                return ReturnError("Module " + parsedUrl.module + " cannot be found");
            }

            string requestProcessorClassName = Configuration.moduleClassMapping[parsedUrl.module];
            Type requestProcessorType = Type.GetType(requestProcessorClassName);
            if (requestProcessorType == null)
            {
                return ReturnError("Type for class " + requestProcessorClassName + " cannot be found");
            }

            // create an instance of the class that will process the request
            object requestProcessor =
                Activator.CreateInstance(
                    requestProcessorType,
                    new object[] { _cfg });

            // call ProcessRequest method of the class
            WebServiceResult requestProcessorResponse =
                (WebServiceResult)requestProcessorType.GetMethod("ProcessRequest").Invoke(
                    requestProcessor,
                    new object[] { parsedUrl.function, parsedUrl.parameters, postData });

            if (requestProcessorResponse.Success)
                return ReturnSuccess(requestProcessorResponse.Message);

            return ReturnError(requestProcessorResponse.Message);
        } // end of ProcessDynamic method

        public string ReturnError(string message)
        {
            string msg = @"{""Status"":""Error"", ""Message"":""" + Base64.Encode(message) + @"""}";
            _logger.Debug("Error: {0}", message);
            return msg;
        }

        public string ReturnSuccess(string message)
        {
            string msg;

            if (message == null)
                msg = @"{""Status"":""Success""}";
            else
                msg = @"{""Status"":""Success"", ""Message"":""" + Base64.Encode(message) + @"""}";

            _logger.Debug("Success: {0}", message);
            return msg;
        }
    } // end of WebService class
}
