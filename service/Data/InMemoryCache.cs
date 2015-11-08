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

namespace SqlWristband.Data
{
    using NLog;
    using System;
    using System.Collections.Concurrent;
    using System.Collections.Generic;
    using System.Data.SqlClient;

    public enum CacheType { Dictionary, Data }

    public class InMemoryCache
    {
        private static Logger _logger = LogManager.GetLogger("InMemoryCache");
        // <name of the table in the repository, in-memory copy of active records of the dictionary>
        private static ConcurrentDictionary<string, CacheTable> _cache = new ConcurrentDictionary<string, CacheTable>();

        #region public method declarations

        public CacheTable this[string cacheKey]
        {
            get { return _cache[cacheKey]; }
        }

        public static void Add(string tableName, int id, object[] keys, object[] values)
        {
            _cache[tableName].Add(id, keys, values);
        }

        public static bool TryAdd(string tableName, CacheTable dict)
        {
            return _cache.TryAdd(tableName, dict);
        }

        public static bool ContainsKey(string tableName)
        {
            return _cache.ContainsKey(tableName);
        }

        /// <summary> Creates in-memory cache table for storing latest data. Single row metrics only.
        /// Such tables will be used in reports/monitors representing current activity across all targets. </summary>
        /// <param name="metricGroup">metric group</param>
        /// <param name="CacheType">data/dictionary</param>
        public static void CreateCacheTableSingleRow(MetricGroup metricGroup, CacheType cacheType)
        {
            if (metricGroup.isMultiRow)
                throw new Exception("Only single-row metrics are supported");

            Dictionary<int, Column> keyColumns = new Dictionary<int, Column>();
            Dictionary<int, Column> valueColumns = new Dictionary<int, Column>();

            // targetId is the key because we have only one current records per target
            keyColumns.Add(0, new Column("targetId", DataType.SmallInt));

            // value columns
            for (int i = 0; i < metricGroup.NumberOfMetrics; i++)
            {
                valueColumns.Add(i, metricGroup.metrics[i]);
            }

            // create new dictionary in dictionaryCache
            if (!ContainsKey(GetCacheKey(-1, metricGroup, cacheType)))
                TryAdd(GetCacheKey(-1, metricGroup, cacheType), new CacheTable(keyColumns, valueColumns, metricGroup.isCumulative));
        } // end of CreateCacheTableSingleRowRealtime

        /// <summary> Returns cache key name
        /// For single-row metric cache is common for all targets (table name only, no schema)
        /// Multi-row metrics each have its own cache </summary>
        /// <param name="targetId">target id or -1 for single row metrics</param>
        /// <param name="metricGroup">metric group</param>
        /// <param name="CacheType">data/dictionary</param>
        public static string GetCacheKey(int targetId, MetricGroup metricGroup, CacheType cacheType = CacheType.Data)
        {
            switch (cacheType)
            {
                case CacheType.Data:
                    if (metricGroup.isMultiRow)
                        return SqlServerProbe.DataTableName(targetId, metricGroup);
                    else
                        return metricGroup.dataTableName;
                case CacheType.Dictionary:
                    return SqlServerProbe.DictTableName(targetId, metricGroup);
                default:
                    throw new Exception("Unsupported cache type");
            }
        } // end of GetCacheKey method
        
        public static bool GetCurrentValues(int targetId, MetricGroup metricGroup, string[] metrics, out DataRow data)
        {
            if (metricGroup.isMultiRow)
                throw new Exception("Only single-row metric group has been implemented so far");

            return GetCurrentValuesSingleRow(targetId, metricGroup, metrics, out data);            
        }

        // Loads dictionary from repository into in-memory cache. Creates a new record in dictionaryCache
        public static void LoadDictionaryIntoCache(int targetId, MetricGroup metricGroup, bool allowReload, SqlConnection connection = null, SqlTransaction transaction = null)
        {
            string cacheKey;
            SqlConnection conn;

            int tryCount = 0;
            bool canExit = false;

            // create new in-memory cache for dictionary
            if (!ContainsKey(GetCacheKey(targetId, metricGroup, CacheType.Dictionary)))
            {
                Dictionary<int, Column> keyColumns = new Dictionary<int, Column>();
                Dictionary<int, Column> attrColumns = new Dictionary<int, Column>();

                // key columns
                for (int i = 0; i < metricGroup.NumberOfMultiRowKeys; i++)
                    keyColumns.Add(i, metricGroup.multiRowKeys[i]);

                // attribute columns
                for (int i = 0; i < metricGroup.NumberOfMultiRowKeyAttributes; i++)
                    attrColumns.Add(i, metricGroup.multiRowKeyAttributes[i]);

                TryAdd(GetCacheKey(targetId, metricGroup, CacheType.Dictionary), new CacheTable(keyColumns, attrColumns, false));
            }

            cacheKey = GetCacheKey(targetId, metricGroup, CacheType.Dictionary);
            CacheTable tmpCache = _cache[cacheKey].CloneAndClear();

            // don't reload cache unless allowReload is specified
            if (allowReload == false && tmpCache.loadedFromDatabase)
                return;

            string sqlStmt = "SELECT id, ";

            for (int i = 0; i < metricGroup.NumberOfMultiRowKeys; i++)
                sqlStmt += metricGroup.multiRowKeys[i].name.Replace(' ', '_') + ", ";

            for (int i = 0; i < metricGroup.NumberOfMultiRowKeyAttributes; i++)
                sqlStmt += metricGroup.multiRowKeyAttributes[i].name.Replace(' ', '_') + ", ";

            if (metricGroup.multiRowKeyAttributesChangeSpeed == ChangeSpeed.Static)
                sqlStmt = sqlStmt.Remove(sqlStmt.Length - 2); // remove last comma
            else
                sqlStmt += "startDate ";

            sqlStmt += Environment.NewLine + "FROM " + SqlServerProbe.DictTableName(targetId, metricGroup);

            if (metricGroup.multiRowKeyAttributesChangeSpeed == ChangeSpeed.Slow)
                sqlStmt += Environment.NewLine + "WHERE endDate IS NULL";

            _logger.Trace(sqlStmt);

            while (!canExit && tryCount < 2)
            {
                try
                {
                    if (connection == null)
                    {
                        conn = new SqlConnection(Configuration.GetReposConnectionString("Cache"));
                        conn.Open();
                    }
                    else
                        conn = connection;

                    using (SqlCommand cmd = conn.CreateCommand())
                    {
                        cmd.CommandText = sqlStmt;
                        cmd.CommandType = System.Data.CommandType.Text;

                        if (transaction != null)
                            cmd.Transaction = transaction;

                        using (SqlDataReader dataReader = cmd.ExecuteReader())
                        {
                            int id;
                            object[] keys = new object[metricGroup.NumberOfMultiRowKeys];
                            object[] values = new object[metricGroup.NumberOfMultiRowKeyAttributes];

                            while (dataReader.Read())
                            {
                                id = (int)dataReader["id"];

                                for (int i = 0; i < metricGroup.NumberOfMultiRowKeys; i++)
                                    keys[i] = dataReader[1 + i];

                                for (int i = 0; i < metricGroup.NumberOfMultiRowKeyAttributes; i++)
                                    values[i] = dataReader[1 + metricGroup.NumberOfMultiRowKeys + i];

                                // add record to dictionary
                                switch (metricGroup.multiRowKeyAttributesChangeSpeed)
                                {
                                    case ChangeSpeed.Static:
                                        tmpCache.Add(id, keys, values);
                                        break;
                                    case ChangeSpeed.Slow:
                                        tmpCache.Add(id, keys, values, (DateTime)dataReader[1 + metricGroup.NumberOfMultiRowKeys + metricGroup.NumberOfMultiRowKeyAttributes]);
                                        break;
                                    default:
                                        throw new Exception("Only Static and Slow changing dictionaries are supported");
                                }
                            }

                            tmpCache.loadedFromDatabase = true;
                            Replace(cacheKey, tmpCache);

                            dataReader.Close();
                        }
                    }

                    if (connection == null)
                        conn.Close();
                }
                catch (SqlException e)
                {
                    if (e.Number == 208) // Invalid object
                    {
                        // Do not create tables if target has been deleted
                        if (!Configuration.targets.ContainsKey(targetId))
                            return;

                        SqlServerProbe.CreateTablesForMetricGroup(targetId, metricGroup);
                    }
                    else
                        _logger.Error("SqlException: " + e.Message + " ErrorCode: " + e.Number.ToString());
                } // end of catch

                tryCount++;
            } // end of while
        } // end of LoadDictionaryIntoCache function

        // Loads data from repository into in-memory cache. Creates a new record in dictionaryCache
        public static void LoadDataIntoCache(int targetId, MetricGroup metricGroup, bool allowReload, SqlConnection connection = null, SqlTransaction transaction = null)
        {
            string cacheKey;
            SqlConnection conn;

            if (metricGroup.changeSpeed != ChangeSpeed.Fast)
                throw new Exception("Only fast changing metric is allowed");

            SqlCommand cmd = null;
            SqlDataReader dataReader = null;

            // create new in-memory cache for dictionary
            if (!ContainsKey(GetCacheKey(targetId, metricGroup, CacheType.Data)))
            {
                Dictionary<int, Column> valueColumns = new Dictionary<int, Column>();

                // value/metric columns
                for (int i = 0; i < metricGroup.NumberOfMetrics; i++)
                {
                    valueColumns.Add(i, metricGroup.metrics[i]);
                }

                TryAdd(GetCacheKey(targetId, metricGroup, CacheType.Data), new CacheTable(new Dictionary<int, Column>(), valueColumns, metricGroup.isCumulative));
            }

            cacheKey = GetCacheKey(targetId, metricGroup, CacheType.Data);
            CacheTable tmpCache = _cache[cacheKey].CloneAndClear();

            // don't reload cache unless allowReload is specified
            if (allowReload == false && tmpCache.loadedFromDatabase)
                return;

            try
            {
                string sqlStmt = "SELECT dictId, ";

                for (int i = 0; i < metricGroup.NumberOfMetrics; i++)
                {
                    sqlStmt += metricGroup.metrics[i].name.Replace(' ', '_') + ", ";
                }

                sqlStmt = sqlStmt.Remove(sqlStmt.Length - 2); // remove last comma

                sqlStmt += Environment.NewLine + "FROM " + SqlServerProbe.DataTableName(targetId, metricGroup) +
                    Environment.NewLine + "WHERE dt = (SELECT MAX(dt) FROM " + SqlServerProbe.DataTableName(targetId, metricGroup) + ")";

                _logger.Trace(sqlStmt);

                if (connection == null)
                {
                    conn = new SqlConnection(Configuration.GetReposConnectionString("Cache"));
                    conn.Open();
                }
                else
                    conn = connection;

                int attempt = 1;
                bool canTry = true;

                while (attempt < 3 && canTry)
                {
                    cmd = conn.CreateCommand();
                    cmd.CommandText = sqlStmt;
                    cmd.CommandType = System.Data.CommandType.Text;

                    if (transaction != null)
                        cmd.Transaction = transaction;

                    try
                    {
                        dataReader = cmd.ExecuteReader();

                        int id;
                        object[] values = new object[metricGroup.NumberOfMetrics];

                        while (dataReader.Read())
                        {
                            id = (int)dataReader["dictId"];

                            for (int i = 0; i < metricGroup.NumberOfMetrics; i++)
                            {
                                values[i] = dataReader[metricGroup.metrics[i].name.Replace(' ', '_')];
                            }

                            tmpCache.Add(id, new object[0], values);
                        }

                        dataReader.Close();

                        tmpCache.loadedFromDatabase = true;

                        Replace(cacheKey, tmpCache);
                    }
                    catch (SqlException e)
                    {
                        if (transaction != null)
                        {
                            transaction.Rollback();
                        }

                        switch (e.Number)
                        {
                            case 208: // Invalid object
                                // Do not create tables if target has been deleted
                                if (!Configuration.targets.ContainsKey(targetId))
                                    return;

                                SqlServerProbe.CreateTablesForMetricGroup(targetId, metricGroup);
                                break;
                            default:
                                _logger.Error("SqlException: " + e.Message + " ErrorCode: " + e.Number.ToString());
                                canTry = false;
                                break;
                        }
                    }
                    finally
                    {
                        if (dataReader != null)
                            ((IDisposable)dataReader).Dispose();

                        if (cmd != null)
                            ((IDisposable)cmd).Dispose();
                    }

                    attempt++;
                }

                if (connection == null)
                    conn.Close();
            }
            catch (Exception e)
            {
                _logger.Error("SqlException: " + e.Message);
            }
        } // end of LoadDataIntoCache function

        #endregion public method declarations

        #region private method declarations

        private static bool GetCurrentValuesSingleRow(int targetId, MetricGroup metricGroup, string[] metrics, out DataRow data)
        {
            data = null;
            object[] values = null;
            CacheTable cache = null;

            // Do not load data from repository if it has not been done yet. This means no current stats are available yet.
            if (metricGroup.isCumulative)
            {
                if (ContainsKey(GetCacheKey(-1, metricGroup)))
                    cache = _cache[GetCacheKey(-1, metricGroup)];
            }
            else
            {
                if (ContainsKey(GetCacheKey(targetId, metricGroup)))
                    cache = _cache[GetCacheKey(targetId, metricGroup)];
            }

            data = new DataRow();

            // get values by key (targetId)
            if (cache != null)
            {
                int id = cache.GetIdByKey(new object[] { (object)targetId });
                if (id != -1)
                    values = cache[id];
            }

            if (values == null)
            {
                foreach (string metricName in metrics)
                    data.Add(metricName, new DataValue(metricGroup[metricName].type, null));

                return false;
            }

            foreach (string metricName in metrics)
            {
                data.Add(metricName,
                    new DataValue(cache.GetColumnMetadataByName(metricName).type,
                                values[cache.GetValueColumnIdByName(metricName) + 1]
                          )
                    );
            }

            return true;
        }

        private static void LoadCacheTableFromDatabaseSingleRowRealtime(MetricGroup metricGroup)
        {
            SqlConnection conn = null;
            SqlCommand cmd = null;
            SqlDataReader dataReader = null;

            // check whether dictionary is loaded
            if (!ContainsKey(GetCacheKey(-1, metricGroup)))
                CreateCacheTableSingleRow(metricGroup, CacheType.Data);

            CacheTable cache = _cache[GetCacheKey(-1, metricGroup)];
            
            if (cache.loadedFromDatabase)
                return;

            try
            {
                conn = new SqlConnection(Configuration.GetReposConnectionString("Cache"));
                conn.Open();

                int targetId;
                string query;

                object[] values = new object[metricGroup.NumberOfMetrics];

                // load data for each target with the specified metric group active
                foreach (InstanceSchedule schedule in Configuration.timeTable.Values)
                {
                    targetId = schedule._targetId;

                    if (schedule._metricGroupId == metricGroup.id)
                    {
                        query = QueryToLoadSingleRowRealtime(targetId, metricGroup);

                        cmd = null;
                        dataReader = null;

                        int attempt = 1;
                        bool canTry = true;

                        while (attempt < 3 && canTry)
                        {
                            try
                            {
                                cmd = new SqlCommand(query);
                                cmd.Connection = conn;

                                dataReader = cmd.ExecuteReader();

                                // should return only one row
                                if (dataReader.Read())
                                {
                                    for (int i = 0; i < metricGroup.NumberOfMetrics; i++)
                                    {
                                        values[i] = dataReader[metricGroup.metrics[i].name.Replace(' ', '_')];
                                    }

                                    cache.Add(-1, new object[] { targetId }, values);
                                }

                                dataReader.Close();
                            }
                            catch (SqlException e)
                            {
                                switch (e.Number)
                                {
                                    case 208: // Invalid object
                                        // Do not create tables if target has been deleted
                                        if (!Configuration.targets.ContainsKey(targetId))
                                            return;

                                        SqlServerProbe.CreateTablesForMetricGroup(targetId, metricGroup);
                                        break;
                                    default:
                                        _logger.Error("SqlException: " + e.Message + " ErrorCode: " + e.Number.ToString());
                                        _logger.Error(e.StackTrace);
                                        canTry = false;
                                        break;
                                }
                            }
                            finally
                            {
                                if (dataReader != null)
                                    ((IDisposable)dataReader).Dispose();

                                if (cmd != null)
                                    ((IDisposable)cmd).Dispose();
                            }

                            attempt++;
                        }
                    }
                }

                conn.Close();
            }
            catch (SqlException e)
            {
                _logger.Error(e.Message);
                _logger.Error(e.StackTrace);
            }
            finally
            {
                if (conn != null)
                    ((IDisposable)conn).Dispose();
            }

            cache.loadedFromDatabase = true;
        }

        // Returns query that select the latest data record for single-row realtime metric
        private static string QueryToLoadSingleRowRealtime(int targetId, MetricGroup metricGroup)
        {
            string query = "SELECT TOP 1 ";

            for (int i = 0; i < metricGroup.NumberOfMetrics; i++)
            {
                query += metricGroup.metrics[i].name.Replace(' ', '_') + ", ";
            }

            query = query.Remove(query.Length - 2); // remove last comma
            query += " FROM " + SqlServerProbe.DataTableName(targetId, metricGroup) + " ORDER BY ";

            switch (metricGroup.changeSpeed)
            {
                case ChangeSpeed.Slow:
                    query += "startDate";
                    break;
                case ChangeSpeed.Fast:
                    query += "dt";
                    break;
                default:
                    throw new Exception("Only Slow and Fast change speeds have been implemented so far");
            }

            query += " DESC";

            return query;
        }

        /// <summary>
        /// Finds cache table by specified key and replaces with new instance
        /// </summary>
        /// <param name="cacheKey"></param>
        /// <param name="newInstance"></param>
        private static void Replace(string cacheKey, CacheTable newInstance)
        {
            _cache[cacheKey] = newInstance;
        }

        #endregion private method declarations
    } // end of InMemoryCache class
}
