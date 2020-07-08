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
        private const string _EVENTHUBENAME = "allevents"; 
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

            var lastCursor = summary?.NextCursor ?? initialCursor;
            var nextCursor = "";

            var analytics = LogAnalyticQuery.GetInstance(logger);
            await analytics.Authenticate(tenantId, _local, _clientId, _clientSecret);

            do
            {
                var timer = Stopwatch.StartNew();
                //get next cursors within the safety boundaries
                nextCursor = await analytics.GetNextCursor($"datetime({lastCursor})", _SAFETYLAG, _TAKE);
                logger.LogInformation($"next cursor: {nextCursor}");
                if (string.IsNullOrEmpty(nextCursor))
                {
                    logger.LogInformation("All logs added to the queue");
                    break;
                }

                //log to CloudTable
                var message = new Summary(lastCursor, nextCursor, -1, "", timer.ElapsedMilliseconds, run);
                await queuesCollector.AddAsync(message);
                await summaryCollector.AddAsync(message);
                await queuesCollector.FlushAsync();
                await summaryCollector.FlushAsync();

                lastCursor = nextCursor;

            } while (true);

            logger.LogInformation("Complete");
        }

        [FunctionName(nameof(BatchProcessor))]
        public static async Task BatchProcessor(
            [QueueTrigger(_QUEUENAME)] string message,
            [Table(_INDEXERTABLENAME)] CloudTable summaryTable,
            [EventHub("allevents", Connection = "EventHubConnection")] IAsyncCollector<string> eventHub,
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


        //[FunctionName("PullLogsFromLogAnalytics")]
        private static async Task PullLogsFromLogAnalytics(
            [TimerTrigger("*/5 * * * * *")] TimerInfo myTimer,
            [EventHub("allevents")] IAsyncCollector<string> outputEvents,
            [Table("LogStreamingIndex")] IAsyncCollector<Summary> summaryCollector,
            [Table("LogStreamingIndex")] CloudTable summaryTable,
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

            var lastCursor = summary?.NextCursor ?? initialCursor;
            var nextCursor = "";

            var analytics = LogAnalyticQuery.GetInstance(logger);
            await analytics.Authenticate(tenantId, _local, _clientId, _clientSecret);

            do
            {
                var timer = Stopwatch.StartNew();
                //get next cursors within the safety boundaries
                nextCursor = await analytics.GetNextCursor($"datetime({lastCursor})", _SAFETYLAG, _TAKE);
                logger.LogInformation($"next cursor: {nextCursor}");
                if (string.IsNullOrEmpty(nextCursor))
                {
                    logger.LogInformation("All logs processed");
                    break;
                }

                var events = await analytics.FetchEvents($"datetime({lastCursor})", $"datetime({nextCursor})");

                await outputEvents.SendEvents(events, logger);

                //log to CloudTable
                await summaryCollector.AddAsync(new Summary(lastCursor, nextCursor, events.Count, "OK", timer.ElapsedMilliseconds, run));
                await summaryCollector.FlushAsync();

                lastCursor = ((JValue)events.LastOrDefault()[_CURSORCOLUMNNAME])?.Value<string>() ?? null;

            } while (true);

            logger.LogInformation("Complete");
        }

        private static async Task SendEvents(this IAsyncCollector<string> outputEvents, IEnumerable<object> events, ILogger logger)
        {
            foreach(var e in events)
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

        public static async Task<string> GetNextCursor(this LogAnalyticQuery query, string after, string safetyLag, int limit)
        {
            var q = $@"union withsource=SourceTable * 
                    | extend ItemId = _ItemId
                    | extend _ingestionTime = ingestion_time() 
                    | extend cursor = format_datetime(_ingestionTime,'yyyy-MM-dd HH:mm:ss.fffffff') 
                    | where _ingestionTime > {after}
                    | where _ingestionTime <= {safetyLag}
                    | order by _ingestionTime asc
                    | take {limit}
                    | summarize count() by cursor";
            return await query.FindNextCursor(workspaceId, q, limit);
        }
    }


}
