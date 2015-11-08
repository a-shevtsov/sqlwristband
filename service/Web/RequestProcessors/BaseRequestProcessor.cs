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

namespace SqlWristband.Web.RequestProcessors
{
    using NLog;
    using System;
    using System.Collections.Generic;
    using System.Data;
    using System.Data.SqlClient;
    // SqlWristband
    using Config;

    public interface IRequestProcessor
    {
        WebServiceResult ProcessRequest(string function, string[] parameters, string postData);
    }

    public class SqlParameters : List<Tuple<string, string>>
    {
        public void Add(string name, string value)
        {
            Add(new Tuple<string, string>(name, value));
        }
    }

    // skeleton of the base class that handles requests to the web service
    public class BaseRequestProcessor
    {
        protected static Configuration _cfg;
        protected static Logger _logger;

        public BaseRequestProcessor() { }

        public BaseRequestProcessor(Configuration config)
        {
            _cfg = config;
        }

        protected virtual WebServiceResult GetData(CommandType cmdType, string sqlQuery, SqlParameters sqlParameters)
        {
            string data;

            try
            {
                using (SqlConnection reposConnection = new SqlConnection(Configuration.GetReposConnectionString("WebService")))
                {
                    reposConnection.Open();

                    using (SqlCommand reposCommand = reposConnection.CreateCommand())
                    {
                        reposCommand.CommandText = sqlQuery;
                        reposCommand.CommandType = cmdType;

                        for (int i = 0; i < sqlParameters.Count; i++)
                        {
                            reposCommand.Parameters.Add(sqlParameters[i].Item1, SqlDbType.VarChar, sqlParameters[i].Item2.Length);
                            reposCommand.Parameters[sqlParameters[i].Item1].Value = sqlParameters[i].Item2;
                        }

                        // prepare the statement if there is at least one parameter
                        if (sqlParameters.Count > 0)
                            reposCommand.Prepare();

                        SqlDataReader dataReader = reposCommand.ExecuteReader();
                        data = "[";
                        while (dataReader.Read())
                        {
                            if (data.Length > 1)
                                data += ",";

                            if (!DBNull.Value.Equals(dataReader[0]))
                                data += (string)dataReader[0];
                        }

                        data += "]";
                    }
                }
            }
            catch (Exception e)
            {
                _logger.Error("GetData: " + e.Message);
                _logger.Debug(e.StackTrace);
                return WebServiceResult.ReturnError(e.Message);
            }

            return WebServiceResult.ReturnSuccess(data);
        }

        protected string DecodeUriCharacters(string rawUriString)
        {
            return rawUriString.Replace("%5B", "[").Replace("%5D", "]").Replace("%3D", "=").Replace("%2B", "+");
        }

        protected Dictionary<string, string> ParsePostData(string rawPostData)
        {
            var parsedData = new Dictionary<string, string>();

            foreach (var item in rawPostData.Split(new[] { '&' }, StringSplitOptions.RemoveEmptyEntries))
            {
                var tokens = item.Split(new[] { '=' }, StringSplitOptions.RemoveEmptyEntries);
                if (tokens.Length < 2)
                    continue;

                parsedData.Add(DecodeUriCharacters(tokens[0]), DecodeUriCharacters(tokens[1]));
                _logger.Debug(
                    "POST parameter [{0}] = [{1}]",
                    DecodeUriCharacters(tokens[0]),
                    DecodeUriCharacters(tokens[1])
                    );
            }

            return parsedData;
        }
    }
}
