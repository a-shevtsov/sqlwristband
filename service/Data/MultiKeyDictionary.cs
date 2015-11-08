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
    using System.Collections.Concurrent;
    using System.Collections.Generic;

    // TODO: consider replacing with KD-Tree http://home.wlu.edu/~levys/software/kd/
    public class MultiKeyDictionary
    {
        #region variable declarations

        public readonly int NumberOfKeys;
        public readonly int NumberOfValues;

        private readonly object _locker = new object();
        protected int _maxId;
        protected Dictionary<int, Column> _keyColumns;
        protected Dictionary<int, Column> _valueColumns;
        protected ConcurrentDictionary<int, object[]> _rows; // each member will be an array (simply a set of values of unknown type)

        #endregion variable declarations

        #region public method declarations

        public MultiKeyDictionary(Dictionary<int, Column> keyColumns, Dictionary<int, Column> valueColumns)
        {
            this.NumberOfKeys = keyColumns.Count;
            this.NumberOfValues = valueColumns.Count;

            this._maxId = 1;
            this._rows = new ConcurrentDictionary<int, object[]>();
            this._keyColumns = new Dictionary<int, Column>();
            this._valueColumns = new Dictionary<int, Column>();

            for (int i = 0; i < this.NumberOfKeys; i++)
                this._keyColumns[i] = keyColumns[i];

            for (int i = 0; i < this.NumberOfValues; i++)
                this._valueColumns[i] = valueColumns[i];
        }

        public virtual void Clear()
        {
            lock (this._locker)
            {
                this._rows.Clear();
                this._maxId = 1;
            }
        }

        public ICollection<int> Ids()
        {
            return this._rows.Keys;
        }

        public virtual object[] this[int id]
        {
            get
            {
/*
                if (!this._rows.ContainsKey(id) || this.NumberOfValues == 0)
                    return null;

                object[] values = new object[this.NumberOfValues];
                for (int i = 0; i < this.NumberOfValues; i++)
                    values[i] = this._rows[id][this.NumberOfKeys + i];

                return values;
 */
                return this._rows[id];
            }
        }

        public virtual object[] this[object[] keys]
        {
            get
            {
                int id = this.GetIdByKey(keys);
                if (id == -1)
                    return null;

                return this[id];
            }
        }

        public virtual bool ContainsId(int id)
        {
            return this._rows.ContainsKey(id);
        }

        public int Add(int id, object[] keys, object[] values, DateTime? startDate = null)
        {
            if (keys.Length != this.NumberOfKeys)
                throw new Exception("Number of keys passed (" + keys.Length.ToString() + ") doesn't match number of key columns in the dictionary (" + this.NumberOfKeys.ToString() + ")");

            if (values.Length != this.NumberOfValues)
                throw new Exception("Number of values passed (" + values.Length.ToString() + ") doesn't match number of value columns in the dictionary (" + this.NumberOfValues.ToString() + ")");

            int id2;

            object[] row;

            if (startDate == null)
                row = new object[keys.Length + values.Length];
            else
                row = new object[keys.Length + values.Length + 1];

            for (int i = 0; i < keys.Length; i++)
                row[i] = keys[i];

            for (int i = 0; i < values.Length; i++)
                row[keys.Length + i] = values[i];

            if (startDate != null)
                row[keys.Length + values.Length] = startDate;

            lock (this._locker)
            {
                if (id == -1)
                {
                    id2 = this._maxId++;
                }
                else
                {
                    id2 = id;

                    if (id2 >= this._maxId)
                        this._maxId = id2 + 1;
                }

                this._rows.AddOrUpdate(id2, row, (k, v) => row);
            }

            return id2;
        }

        public virtual void UpdateRowValues(object[] keys, object[] values)
        {
            if (keys.Length != this.NumberOfKeys)
                throw new Exception("Number of keys passed (" + keys.Length.ToString() + ") doesn't match number of key columns in the dictionary (" + this.NumberOfKeys.ToString() + ")");

            if (values.Length != this.NumberOfValues)
                throw new Exception("Number of values passed (" + values.Length.ToString() + ") doesn't match number of value columns in the dictionary (" + this.NumberOfValues.ToString() + ")");

            lock (this._locker)
            {
                // get row number
                int id = this.GetIdByKey(keys);

                if (id < 0)
                    throw new Exception("Cannot find row to update");

                // update values
                for (int i = 0; i < this.NumberOfValues; i++)
                    this._rows[id][this.NumberOfKeys + i] = values[i];
            }
        }

        public virtual void UpdateRowValues(int id, object[] values)
        {
            if (values.Length != this.NumberOfValues)
                throw new Exception("Number of values passed (" + values.Length.ToString() + ") doesn't match number of value columns in the dictionary (" + this.NumberOfValues.ToString() + ")");

            lock (this._locker)
            {
                // update values
                for (int i = 0; i < this.NumberOfValues; i++)
                    this._rows[id][this.NumberOfKeys + i] = values[i];
            }
        }

        /// <summary>Adds a new row or updates values in existing one (matches by keys array)</summary>
        /// <param name="id">Id [for new/of existing] record. Pass '-1' to autogenerate id</param>
        /// <param name="keys">Array of keys</param>
        /// <param name="values">Array of values</param>
        /// <param name="startDate">Optional. Skipped if null. Used by slow changing dimentions</param>
        /// <returns>Id of the added or updated record</returns>
        public virtual int AddOrUpdateRowValues(int id, object[] keys, object[] values, DateTime? startDate = null)
        {
            if (keys.Length != this.NumberOfKeys)
                throw new Exception("Number of keys passed (" + keys.Length.ToString() + ") doesn't match number of key columns in the dictionary (" + this.NumberOfKeys.ToString() + ")");

            if (values.Length != this.NumberOfValues)
                throw new Exception("Number of values passed (" + values.Length.ToString() + ") doesn't match number of value columns in the dictionary (" + this.NumberOfValues.ToString() + ")");

            int id2;

            if (keys.Length == 0)
            {
                if (this._rows.ContainsKey(id))
                {
                    id2 = id;
                    this.UpdateRowValues(id, values);
                }
                else
                {
                    id2 = this.Add(id, keys, values, startDate);
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
                    this.UpdateRowValues(keys, values);
                }
            }

            return id2;
        }

        public int GetIdByKey(object[] keys)
        {
            bool isOk;

            if (keys.Length != this.NumberOfKeys)
                throw new Exception("Number of keys passed (" + keys.Length.ToString() + ") doesn't match number of key columns in the dictionary (" + this.NumberOfKeys.ToString() + ")");

            lock (this._locker)
            {
                foreach (int id in this._rows.Keys)
                {
                    isOk = true;
                    for (int c = 0; c < this.NumberOfKeys; c++)
                    {
                        if (!this.Compare(this._rows[id][c], keys[c]))
                        { // break if even one key doesn't match
                            isOk = false;
                            break;
                        }
                    }

                    if (isOk)
                        return id;
                }
            }

            return -1; // row was not found
        }

        public bool CompareAttributesForKey(int id, object[] attributes)
        {
            bool isOk;

            lock (this._locker)
            {
                isOk = true;
                for (int c = 0; c < attributes.Length; c++)
                {
                    if (!this.Compare(this._rows[id][this.NumberOfKeys + c], attributes[c]))
                    { // break if even one key doesn't match
                        isOk = false;
                        break;
                    }
                }

                if (isOk)
                    return true;
            }

            return false; // row was not found
        }

        public int GetValueColumnIdByName(string columnName)
        {
            for (int i = 0; i < this._valueColumns.Count; i++)
            {
                if (this._valueColumns[i].name.CompareTo(columnName) == 0)
                    return i;
            }

            return -1;
        }

        public Column GetColumnMetadataByName(string columnName)
        {
            int id = this.GetValueColumnIdByName(columnName);
            if (id == -1)
                throw new Exception("Could not find column [" + columnName + "]");

            return this._valueColumns[id];
        }

        #endregion public method declarations

        #region private method declarations

        private bool Compare<T>(T left, T right)
        {
            EqualityComparer<T> comparer = EqualityComparer<T>.Default;

            if (comparer.Equals(left, right))
                return true;

            return false;
        }

        #endregion private method declarations
    } // end of MultiKeyDictionary class
}
