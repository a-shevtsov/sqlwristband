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

namespace SqlWristband.Config
{
    using System;
    using System.Collections.Concurrent;

    public struct Target
    {
        public int id;
        public string name;
        public bool isSqlAuthentication;
        public string username;
        public string password;

        public Target(int id, string name, bool isSqlAuthentication = false, string username = null, string password = null)
        {
            this.id = id;
            this.name = name;
            this.isSqlAuthentication = isSqlAuthentication;
            this.username = username;
            this.password = password;
        }
    }

    public class TargetCollection : ConcurrentDictionary<int, Target>
    {
        object _lock = new object();

        /// <summary>Adds new target</summary>
        /// <param name="id">Id of the target. If -1 then Id will be generated</param>
        /// <param name="target">Target object</param>
        /// <returns>Id of added record</returns>
        public int Add(int id, Target target)
        {
            int id2 = id;

            lock (this._lock)
            {
                if (id == -1)
                    id2 = this.Count;

                this.TryAdd(id2, target);
            }

            return id2;
        }

        public int GetIdByName(string name)
        {
            lock (this._lock)
            {
                foreach (int targetId in this.Keys)
                {
                    if (string.Equals(this[targetId].name, name, StringComparison.OrdinalIgnoreCase))
                        return targetId;
                }
            }

            throw new Exception("Target [" + name + "] was not found");
        }

        public bool ContainsId(int id)
        {
            return this.ContainsKey(id);
        }

        public string GetPassword(int targetId)
        {
            return Base64.Decode(this[targetId].password);
        }
    }
}
