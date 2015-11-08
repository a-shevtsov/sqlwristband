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

namespace SqlWristband.Config
{
    using System;
    using System.Collections.Generic;

    public class MetricGroupCollection : List<MetricGroup>
    {
        public new MetricGroup this[int id]
        {
            get
            {
                foreach (MetricGroup mgd in this)
                {
                    if (mgd.id == id)
                        return mgd;
                }

                throw new System.Exception("Cannot find requested metric group by id: " + id.ToString());
            }
        }

        public MetricGroup this[string name]
        {
            get
            {
                foreach (MetricGroup mgd in this)
                {
                    if (mgd.name.Equals(name))
                        return mgd;
                }

                throw new System.Exception("Cannot find requested metric group by name: " + name);
            }
        }
    } // end of MetricGroupCollection class
}
