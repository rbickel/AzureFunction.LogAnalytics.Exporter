using System;
using Microsoft.Azure.WebJobs;
using Microsoft.Extensions.Logging;
using Newtonsoft.Json;
using System.Threading.Tasks;
using System.Collections.Generic;
using Newtonsoft.Json.Linq;
using System.Linq;
using System.Diagnostics;
using Microsoft.Azure.Cosmos.Table;

namespace Rbkl.io
{
    public static class LogAnalyticsExporter
    {
        public static string workspaceId = Environment.GetEnvironmentVariable("workspaceId");
        public static string tenantId = Environment.GetEnvironmentVariable("tenantId");
        public static bool _local = string.Equals(Environment.GetEnvironmentVariable("isLocal"), "true");

        private static string _clientId = Environment.GetEnvironmentVariable("clientId");
        private static string _clientSecret = Environment.GetEnvironmentVariable("clientSecret");
        private const string _QUEUENAME = "batchcursors-queue";
        private const string _INDEXERTABLENAME = "BatchIndexTable";
        private const string _EVENTHUBEPATH = "allevents";
        private static string _CURSORFORMAT = "yyyy-MM-dd HH:mm:ss.fffffff";
        private const string _CURSORCOLUMNNAME = "cursor";
        private const int _TAKE = 1000;
        private const string _SAFETYLAG = "ago(5m)";

        [FunctionName(nameof(DatabaseCursorSlicer))]
        public static async Task DatabaseCursorSlicer(
            [TimerTrigger("0 * * * * *")] TimerInfo myTimer,
            [Queue(_QUEUENAME)] IAsyncCollector<Summary> queuesCollector,
            [Table(_INDEXERTABLENAME)] IAsyncCollector<Summary> summaryCollector,
            [Table(_INDEXERTABLENAME)] CloudTable summaryTable,
            ILogger logger
        )
        {
            var run = DateTime.Now;
            logger.LogInformation($"C# Timer trigger function executed at: {run}");

            //Need to retrieve the latest cursor value
            var initialCursor = DateTime.Now.AddMinutes(-31).ToUniversalTime().ToString(_CURSORFORMAT);

            Summary summary = null;
            var lastSummaryQuery = new TableQuery<Summary>().Take(1);
            try
            {
                summary = (await summaryTable.ExecuteQuerySegmentedAsync(lastSummaryQuery, null)).FirstOrDefault();
            }
            catch (Exception e)
            {
                logger.LogError(e, "Impossible to find the latest cursor from the Table");
            }

            var previousCursor = summary?.NextCursor ?? initialCursor;

            var analytics = LogAnalyticQuery.GetInstance(logger);
            await analytics.Authenticate(tenantId, _local, _clientId, _clientSecret);

            bool exit = true;
            do
            {
                exit = true;
                var timer = Stopwatch.StartNew();
                
                //get next cursor ranges within the safety boundaries
                var ranges = analytics.GetCursorRanges($"{previousCursor}", _SAFETYLAG, _TAKE);
                
                await foreach(var r in ranges)
                {
                    logger.LogDebug($"Summary: {JsonConvert.SerializeObject(r)}");
                    await queuesCollector.AddAsync(r);
                    await summaryCollector.AddAsync(r);
                    //continue the loop if some items were found
                    exit = false;
                    previousCursor = r.LastCursor;
                }
                await queuesCollector.FlushAsync();
                await summaryCollector.FlushAsync();

            } while (exit);

            logger.LogInformation("Complete");
        }

        [FunctionName(nameof(BatchProcessor))]
        public static async Task BatchProcessor(
            [QueueTrigger(_QUEUENAME)] string message,
            [Table(_INDEXERTABLENAME)] CloudTable summaryTable,
            [EventHub(_EVENTHUBEPATH, Connection = "EventHubConnection")] IAsyncCollector<string> eventHub,
            ILogger logger)
        {
            var run = DateTime.Now;
            logger.LogInformation($"Processing batch {run}");
            var analytics = LogAnalyticQuery.GetInstance(logger);
            await analytics.Authenticate(tenantId, _local, _clientId, _clientSecret);

            var timer = Stopwatch.StartNew();
            var summary = JsonConvert.DeserializeObject<Summary>(message);

            var events = await analytics.FetchEvents($"datetime({summary.LastCursor})", $"datetime({summary.NextCursor})");
            await eventHub.SendEvents(events, logger);

            summary = new Summary(summary.LastCursor, summary.NextCursor, events.Count, "OK", timer.ElapsedMilliseconds, run);
            summary.ETag = "*";  //Etag required for replace
            await summaryTable.ExecuteAsync(TableOperation.Replace(summary));
        }

        private static async Task SendEvents(this IAsyncCollector<string> outputEvents, IEnumerable<object> events, ILogger logger)
        {
            foreach (var e in events)
            {
                await outputEvents.AddAsync(JsonConvert.SerializeObject(e));
            }

            await outputEvents.FlushAsync();
            logger.LogInformation($"{events.Count()} Events sent successfully");
        }

        public static async Task<IList<Dictionary<string, object>>> FetchEvents(this LogAnalyticQuery query, string after, string to)
        {
            var q = $@"union withsource=SourceTable * 
                    | extend ItemId = _ItemId
                    | extend _ingestionTime = ingestion_time() 
                    | extend cursor = format_datetime(_ingestionTime,'yyyy-MM-dd HH:mm:ss.fffffff') 
                    | where _ingestionTime > {after}
                    | where _ingestionTime <= {to}
                    | order by _ingestionTime asc";
            return await query.ExecuteQuery(workspaceId, q);
        }

        public static async IAsyncEnumerable<Summary> GetCursorRanges(this LogAnalyticQuery query, string lastCursor, string safetyLag, int limit)
        {
            //Making 1 second grouped ranges for better performances
            var q = $@"union withsource=SourceTable * 
                    | extend ItemId = _ItemId
                    | extend _ingestionTime = ingestion_time() 
                    | extend cursor = format_datetime(_ingestionTime,'yyyy-MM-dd HH:mm:ss.fffffff') 
                    | where _ingestionTime > datetime({lastCursor})
                    | where _ingestionTime <= {safetyLag}
                    | order by _ingestionTime asc
                    | limit 10000
                    | summarize max(cursor), count() by bin(_ingestionTime, 1s)";

            var cursors = await query.GetNextCursors(workspaceId, q, limit);

            foreach (var c in cursors)
            {
                yield return new Summary(lastCursor, c.Item1){ EventsCount = c.Item2 };
                lastCursor = c.Item1;
            }
        }
    }
}
