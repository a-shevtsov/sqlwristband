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

namespace SqlWristband.Probes
{
    using System;
    using System.Collections.Generic;
    using System.Linq;

    public class ProbeCollection
    {
        private struct Probe
        {
            public string code;
            public string name;
            public object classReference;

            public Probe(string code, string name, object classReference)
            {
                if (code.Length != 4)
                    throw new Exception("Probe code must be 4 characters long. '" + code.ToString() + "'");

                this.code = code;
                this.name = name;
                this.classReference = classReference;
            }
        }

        private List<Probe> probes;

        public ProbeCollection()
        {
            this.probes = new List<Probe>();
        }

        public void Clear()
        {
            this.probes.Clear();
        }

        public void Add(string code, string name, object classReference)
        {
            this.probes.Add(new Probe(code, name, classReference));
        }

        public string GetProbeCodeByName(string name)
        {
            foreach (Probe pd in this.probes)
            {
                if (pd.name.Equals(name))
                    return pd.code;
            }

            throw new System.Exception("Cannot find requested probe by name: " + name);
        }

        public object GetClassReferenceByCode(string code)
        {
            foreach (Probe pd in this.probes)
            {
                if (pd.code.SequenceEqual(code))
                    return pd.classReference;
            }

            throw new System.Exception("Cannot find requested probe by code: " + code.ToString());
        }
    } // end of Probes class
}
