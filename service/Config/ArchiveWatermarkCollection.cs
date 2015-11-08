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
    using System.Collections.Concurrent;
    using System.Linq;

    public struct ArchiveWatermark
    {
        public int Id;
        public int TargetId;
        public int ArchiveOffsetId;
        public DateTime ArchivedToDate;

        public ArchiveWatermark(int id, int targetId, int archiveOffsetId, DateTime archivedToDate)
        {
            Id = id;
            TargetId = targetId;
            ArchiveOffsetId = archiveOffsetId;
            ArchivedToDate = archivedToDate;
        }
    }

    public class ArchiveWatermarkCollection : ConcurrentDictionary<int, ArchiveWatermark>
    {
        /// <summary>Returns watermark for next level archive offset or retention period for last archive offset level</summary>
        /// <param name="archiveWatermarkId">Id of archive watermark</param>
        public DateTime GetNextLevelArchivedToDate(int archiveWatermarkId)
        {
            int scheduleId = Configuration.archiveOffsets[this[archiveWatermarkId].ArchiveOffsetId].ScheduleId;
            int targetId = this[archiveWatermarkId].TargetId;
            int offset = Configuration.archiveOffsets[this[archiveWatermarkId].ArchiveOffsetId].OffsetInMinutes;

            var nextLevelWatermarks =
                from aw in this
                where aw.Value.TargetId == targetId
                    && Configuration.archiveOffsets[aw.Value.ArchiveOffsetId].ScheduleId == scheduleId
                    && Configuration.archiveOffsets[aw.Value.ArchiveOffsetId].OffsetInMinutes > offset
                orderby Configuration.archiveOffsets[aw.Value.ArchiveOffsetId].OffsetInMinutes
                select aw.Value;

            if (nextLevelWatermarks.Count() > 0)
            {
                return nextLevelWatermarks.First().ArchivedToDate;
            }

            return DateTime.Now.AddHours(-1 * Configuration.timeTable[scheduleId]._schedule.retention);
        } // end of GetNextLevelArchivedToDate
    }
}
