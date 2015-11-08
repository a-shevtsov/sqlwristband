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

namespace SqlWristband.Data
{
    using System;
    using System.Collections.Generic;
    using System.Linq;
    using System.Text;
    using System.Threading.Tasks;

    public class CacheTable : MultiKeyDictionary
    {
        public bool loadedFromDatabase;
        public bool isCumulative;

        private readonly object _syncLocker = new object(); // synchronization locker for Cumulative tables

        private MultiKeyDictionary _oldCache;

        public CacheTable(Dictionary<int, Column> keyColumns, Dictionary<int, Column> valueColumns, bool isCumulative)
            : base(keyColumns, valueColumns)
        {
            this.loadedFromDatabase = false;
            this.isCumulative = isCumulative;

            if (isCumulative)
                this._oldCache = new MultiKeyDictionary(keyColumns, valueColumns);
        }

        public override void Clear()
        {
            lock (_syncLocker)
            {
                base.Clear();

                if (this.isCumulative)
                    this._oldCache.Clear();
            }
        }

        public override object[] this[int id]
        {
            get
            {
                if (this.isCumulative)
                {
                    if (!this._oldCache.ContainsId(id))
                        return null;

                    object[] oldValues = this._oldCache[id];

                    object[] diff = new object[this.NumberOfKeys + this.NumberOfValues];
                    for (int i = 0; i < this.NumberOfKeys; i++)
                    {
                        diff[i] = this._rows[id][i];
                    }

                    int col;
                    for (int i = 0; i < this.NumberOfValues; i++)
                    {
                        col = this.NumberOfKeys + i;

                        switch (this._valueColumns[i].type)
                        {
                            case DataType.SmallInt:
                                diff[col] = (object)((short)this._rows[id][col] - (short)oldValues[col]);
                                break;
                            case DataType.Double:
                                diff[col] = (object)((double)this._rows[id][col] - (double)oldValues[col]);
                                break;
                            default:
                                throw new Exception("SmallInt and Double data types are the only supported cumulative metrics");
                        }
                    }

                    return diff;
                }
                else
                    return base._rows[id];
            }
        }

        public override object[] this[object[] keys]
        {
            get
            {
                int id = this.GetIdByKey(keys);

                if (id == -1)
                    return null;

                if (this.isCumulative)
                {
                    object[] oldValues = this._oldCache[id];

                    if (oldValues == null)
                        return null;

                    return this[id];
                }
                else
                    return base._rows[id];
            }
        }

        public override void UpdateRowValues(object[] keys, object[] values)
        {
            if (this.isCumulative)
            {
                lock (this._syncLocker)
                {
                    int id = this.GetIdByKey(keys);

                    if (id != -1)
                    {
                        object[] oldValues = new object[this.NumberOfValues];

                        for (int i = 0; i < this.NumberOfValues; i++)
                            oldValues[i] = this._rows[id][this.NumberOfKeys + i];

                        this._oldCache.AddOrUpdateRowValues(-1, keys, oldValues);
                    }

                    base.UpdateRowValues(keys, values);
                }
            }
            else
            {
                base.UpdateRowValues(keys, values);
            }
        }

        public override void UpdateRowValues(int id, object[] values)
        {
            lock (this._syncLocker)
            {
                if (this.isCumulative)
                {
                    object[] oldValues = new object[this.NumberOfValues];

                    for (int i = 0; i < this.NumberOfValues; i++)
                    {
                        oldValues[i] = this._rows[id][this.NumberOfKeys + i];
                    }

                    object[] keys = new object[this.NumberOfKeys];

                    for (int i = 0; i < this.NumberOfKeys; i++)
                    {
                        keys[i] = this._rows[id][i];
                    }

                    this._oldCache.AddOrUpdateRowValues(id, keys, oldValues);
                }

                base.UpdateRowValues(id, values);
            }
        }

        public override int AddOrUpdateRowValues(int id, object[] keys, object[] values, DateTime? startDate = null)
        {
            if (keys.Length != this.NumberOfKeys)
                throw new Exception("Number of keys passed (" + keys.Length.ToString() + ") doesn't match number of key columns in the dictionary (" + this.NumberOfKeys.ToString() + ")");

            if (values.Length != this.NumberOfValues)
                throw new Exception("Number of values passed (" + values.Length.ToString() + ") doesn't match number of value columns in the dictionary (" + this.NumberOfValues.ToString() + ")");

            int id2;

            if (keys.Length == 0)
            {
                id2 = id;
                if (this._rows.ContainsKey(id))
                {
                    this.UpdateRowValues(id, values);
                }
                else
                {
                    this.Add(id, keys, values, startDate);
                }
            }
            else
            {
                id2 = this.GetIdByKey(keys);

                if (id2 == -1)
                {
                    id2 = this.Add(id, keys, values, startDate);
                }
                else
                {
                    this.UpdateRowValues(id2, values);
                }
            }

            return id2;
        }

        /// <summary>
        /// Creates a copy of this instance but with no data
        /// </summary>
        public CacheTable CloneAndClear()
        {
            CacheTable clone = new CacheTable(this._keyColumns, this._valueColumns, this.isCumulative);

            clone.loadedFromDatabase = this.loadedFromDatabase;

            return clone;
        }

        public int GetIdOfMaxValue(string columnName, DataType valueDataType)
        {
            int id = -1;
            short maxShortValue = short.MinValue;
            double maxDoubleValue = double.MinValue;

            if (valueDataType != DataType.SmallInt && valueDataType != DataType.Double)
                throw new Exception("Only SmallInt and Read data types are handled at the moment");

            int col = this.GetValueColumnIdByName(columnName);
            if (col == -1)
                return -1;

            col += this.NumberOfKeys;

            lock (this._syncLocker)
            {
                foreach (int row in this._rows.Keys)
                {
                    if (this[row] == null)
                        continue;

                    if (valueDataType == DataType.Double)
                    {
                        if ((double)this[row][col] > maxDoubleValue)
                        {
                            maxDoubleValue = (double)this[row][col];
                            id = row;
                        }
                    }
                    else
                    {
                        if ((short)this[row][col] > maxShortValue)
                        {
                            maxShortValue = (short)this[row][col];
                            id = row;
                        }
                    }
                }
            }

            return id;
        }
    }
}
