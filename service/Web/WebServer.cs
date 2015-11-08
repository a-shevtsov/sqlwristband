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
using SqlWristband.Web.RequestProcessors;

namespace SqlWristband
{
    using NLog;
    using System;
    using System.IO;
    using System.Collections.Generic;
    using System.Linq;
    using System.Net;
    using System.Reflection;
    using System.Text;
    using System.Threading;
    using System.Threading.Tasks;

    public class WebServer
    {
        private static Manager _mgr;
        private static Configuration _cfg;
        private static Logger _logger = LogManager.GetLogger("WebServer");
        private static string _rootFolder;

        private WebService _ws;
        private HttpListener _listener;
        private bool _shouldStop;
        private List<Task> _processRequestTasks;

        public WebServer(Manager manager, Configuration cfg)
        {
            _mgr = manager;
            _cfg = cfg;
            _rootFolder = Path.GetFullPath(Path.Combine(Path.GetDirectoryName(Assembly.GetExecutingAssembly().Location), Configuration.wsRootFolder));
        }

        // stops execution of ProcessRequests
        public void RequestStop()
        {
            _logger.Info("Stopping WebServer");
            _shouldStop = true;
        }
        
        public void ProcessRequests()
        {
            _logger.Info("Web Server started");

            try
            {
                _processRequestTasks = new List<Task>();
                _ws = new WebService(_cfg);

                _logger.Debug("Starting listener");
                _listener = StartListener();

                while (!_shouldStop)
                {
                    var contextTask = _listener.GetContextAsync();
                    while (!contextTask.IsCompleted)
                    {
                        if (_shouldStop)
                            return;

                        CollectFinishedTasks();
                        contextTask.Wait(50);
                    }

                    // Dispatch new processing task
                    var task = Task.Factory.StartNew(() => ProcessRequest(contextTask.Result));
                    _processRequestTasks.Add(task);

                    CollectFinishedTasks();
                    _logger.Debug("Number of running tasks {0}", _processRequestTasks.Count);
                }

                _listener.Stop();
                _listener.Close();
            }
            catch (Exception e)
            {
                _logger.Error(e.Message);
                _logger.Debug(e.StackTrace);

                if (_listener != null)
                {
                    if (_listener.IsListening)
                        _listener.Stop();

                    _listener.Close();
                }

                _mgr.ReportFailure("WebServer");
            }

            _logger.Info("Web Server stopped");
        }

        private void CollectFinishedTasks()
        {
            int taskId;
            Task[] tasksToWait;

            try
            {
                // Check whether any task finished execution and remove it/them from array
                while (true)
                {
                    // Generate list of tasks to wait
                    tasksToWait = _processRequestTasks.ToArray();
                    taskId = Task.WaitAny(tasksToWait, 0);
                    if (taskId == -1 || taskId == WaitHandle.WaitTimeout)
                        break;
                    _processRequestTasks.Remove(tasksToWait[taskId]);
                    _logger.Debug("Number of running tasks {0}", _processRequestTasks.Count);
                }
            }
            catch (Exception ex)
            {
                _logger.Error(ex.Message);
                _logger.Debug(ex.StackTrace);
            }
        }

        private void ProcessRequest(HttpListenerContext context)
        {
            HttpListenerResponse response;
            ParsedUrl parsedUrl;
            string contentType;
            int bytesRead;
            byte[] responseBuffer, postDataBuffer;

            postDataBuffer = new byte[1024];

            var request = context.Request;
            _logger.Debug("Requested: " + request.RawUrl);
            _logger.Debug("Method: " + request.HttpMethod);

            string postData = null;

            try
            {
                // Retrieve POST message
                if (request.HttpMethod.CompareTo("POST") == 0)
                {
                    postData = "";

                    do
                    {
                        bytesRead = request.InputStream.Read(postDataBuffer, 0, 1024);
                        postData += Encoding.UTF8.GetString(postDataBuffer, 0, bytesRead);
                    } while (bytesRead == 1024);

                    _logger.Debug("Post method data: " + postData);
                }

                // Obtain a response object
                response = context.Response;

                parsedUrl = ParseUrl(request.RawUrl); // get the module name (second parameter in the URL)
                if (parsedUrl == null)
                {
                    response.ContentType = "application/json";
                    // Construct a response
                    responseBuffer = Encoding.UTF8.GetBytes("URL must be in the /" + Configuration.wsPrefix + "/module/function/parameter1/parameter2/... format");
                }
                else if (!parsedUrl.IsWebServiceRequest) // static content was requested
                {
                    if (ProcessStatic(request.RawUrl, out responseBuffer, out contentType))
                    {
                        response.ContentType = contentType;
                    }
                    else
                    {
                        response.StatusCode = 404;
                        response.ContentType = "text/plain";

                        var encoding = new ASCIIEncoding();
                        responseBuffer = encoding.GetBytes("File not found");
                    }
                }
                else // web service was invoked
                {
                    response.ContentType = "application/json";
                    responseBuffer = Encoding.UTF8.GetBytes(_ws.ProcessDynamic(parsedUrl, postData));
                }

                // Get a response stream and write the response to it
                response.ContentLength64 = responseBuffer.Length;
                Stream output = response.OutputStream;
                output.Write(responseBuffer, 0, responseBuffer.Length);

                // Close the output stream.
                output.Close();
            }
            catch (Exception ex)
            {
                _logger.Error(ex.Message);
                _logger.Debug(ex.StackTrace);
            }
        }

        private HttpListener StartListener()
        {
            if (HttpListener.IsSupported)
                _logger.Debug("HttpListener is supported!");
            else
                _logger.Debug("HttpListener is NOT supported!");

            HttpListener listener = new HttpListener();

            listener.Start();
            _logger.Debug("HttpListener was started!");

            if (listener.IsListening)
                _logger.Debug("HttpListener is listening!");
            else
                _logger.Debug("HttpListener is NOT listening!");

            listener.Prefixes.Add(Configuration.wsProtocol + "://" + Configuration.wsHostname + ":" + Configuration.wsPort + "/"); // static web server

            return listener;
        }

        // parses URL string assuming the format /<wsPrefix>/<module>/<function>/<parameter1>/<parameter2>/...
        private ParsedUrl ParseUrl(string rawUrl)
        {
            ParsedUrl parsedUrl = new ParsedUrl();

            // check that the string does indeed start with wsPrefix
            if (!rawUrl.StartsWith("/" + Configuration.wsPrefix + "/"))
                parsedUrl.IsWebServiceRequest = false;
            else
            {
                parsedUrl.IsWebServiceRequest = true;

                string[] parsedUrlTemp = rawUrl.Split('/');

                if (parsedUrlTemp.Count() < 4)
                    return null;

                parsedUrl.module = parsedUrlTemp[2];
                parsedUrl.function = parsedUrlTemp[3];
                parsedUrl.parameters = parsedUrlTemp.Where((val, idx) => idx > 3).ToArray();

                // restore spaces as they should be
                for (int i = 0; i < parsedUrl.parameters.Count(); i++)
                {
                    parsedUrl.parameters[i] = parsedUrl.parameters[i].Replace("%20", " ");
                    parsedUrl.parameters[i] = parsedUrl.parameters[i].Replace("%5C", "\\");
                }
            }

            return parsedUrl;
        }

        /// <summary>Reads static file from disk</summary>
        /// <param name="rawUrl">URL without domain name</param>
        /// <param name="content">Contents of file</param>
        /// <param name="contentType">Content type of file</param>
        /// <returns>True if file was read successfully</returns>
        private bool ProcessStatic(string rawUrl, out byte[] content, out string contentType)
        {
            int extensionOffset, parametersOffset;
            string path, extension;

            parametersOffset = rawUrl.IndexOf('?');

            if (parametersOffset == -1)
                path = rawUrl.TrimStart('/');
            else
                path = rawUrl.Substring(0, parametersOffset).TrimStart('/');

            if (path.Equals(""))
                path = "Index.html";

            extensionOffset = path.LastIndexOf('.');

            extension = extensionOffset >= 0 ? path.Substring(extensionOffset + 1, path.Length - extensionOffset - 1) : "html";

            switch (extension) // TODO: replace with a dictionary
            {
                case "js": contentType = "application/json"; break;
                case "html": contentType = "text/html"; break;
                case "ico": contentType = "image/ico"; break;
                case "png": contentType = "image/png"; break;
                case "jpg": contentType = "image/jpeg"; break;
                case "jpeg": contentType = "image/jpeg"; break;
                case "gif": contentType = "image/gif"; break;
                case "css": contentType = "text/css"; break;
                case "woff": contentType = "application/font-woff"; break;
                case "ttf": contentType = "application/x-font-ttf"; break;
                default: contentType = "text/html"; break;
            }

            try
            {
                string filename = Path.Combine(_rootFolder, path.Replace('/', '\\'));

                // return false if file doesn't exist
                if (!File.Exists(filename))
                {
                    _logger.Error("File [" + filename + "] was not found");
                    content = null;
                    return false;
                }
                
                content = File.ReadAllBytes(filename);
            }
            catch (Exception e)
            {
                _logger.Error(e.Message);
                _logger.Debug(e.StackTrace);
                content = null;
                return false;
            }

            return true;
        }
    } // end of WebServer class
}
