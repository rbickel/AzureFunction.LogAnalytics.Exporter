
using System;
using Microsoft.Azure.Cosmos.Table;

namespace Rbkl.io
{

    public class Summary : TableEntity
    {
        public Summary() { }
        public Summary(string last, string next, int count, string status, long duration, DateTime run)
        {
            PartitionKey = string.Format("{0:D19}", DateTime.MaxValue.Ticks - DateTime.Parse(last).Ticks);
            RowKey = string.Format("{0:D19}", DateTime.MaxValue.Ticks - DateTime.Parse(next).Ticks);
            LastCursor = last;
            NextCursor = next;
            EventsCount = count;
            Status = status;
            Duration = duration;
            Run = run;
        }
        public string LastCursor { get; set; }
        public string NextCursor { get; set; }
        public int EventsCount { get; set; }
        public string Status { get; set; }
        public DateTime Run { get; set; }
        public long Duration { get; set; }
    }
}