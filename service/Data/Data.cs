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

    public enum DataType { Double, Ansi, Unicode, Datetime, SmallInt }

    public struct Column
    {
        public string name;
        public DataType type;

        public Column(string name, DataType type)
        {
            this.name = name;
            this.type = type;
        }
    }

    public struct DataValue
    {
        public DataType type;
        public object value;

        public DataValue(DataType type, object value)
        {
            this.type = type;
            this.value = value;
        }

        public override string ToString()
        {
            if (this.value == null)
                return string.Empty;

            switch (this.type)
            {
                case DataType.Double:
                    return ((double)this.value).ToString("n2").Replace(".00", "");
                default:
                    return this.value.ToString();
            }
        }
    }

    public class DataRow : Dictionary<string, DataValue> { } // <column name, value>
}
