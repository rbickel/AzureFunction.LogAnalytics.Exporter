using System;
using Microsoft.Azure.WebJobs;
using Microsoft.Extensions.Logging;
using Newtonsoft.Json;
using System.Threading.Tasks;
using System.Collections.Generic;
using Newtonsoft.Json.Linq;
using System.Linq;
using Microsoft.WindowsAzure.Storage.Table;
using System.Diagnostics;

namespace Company.Function
{
    public static class PullLogsFromLogAnalytics
    {
        public static string workspaceId = Environment.GetEnvironmentVariable("workspaceId");
        public static string tenantId = Environment.GetEnvironmentVariable("tenantId");
        public static bool _local = string.Equals(Environment.GetEnvironmentVariable("isLocal"), "true");
        private static string cursorFormat = "yyyy-MM-dd HH:mm:ss.fffffff";
        private const string CursorColumn = "cursor";

        private const int _take = 1000;
        private const string _safetyLag = "ago(30m)";

        [FunctionName("PullLogsFromLogAnalytics")]
        public static async Task RunAsync(
            [TimerTrigger("*/5 * * * * *")] TimerInfo myTimer,
            [EventHub("allevents", Connection = "EventHubConnection")] IAsyncCollector<string> outputEvents,
            [Table("LogStreamingIndex", Connection = "SummaryTableConnection")] IAsyncCollector<Summary> summaryCollector,
            [Table("LogStreamingIndex", Connection = "SummaryTableConnection")] CloudTable summaryTable,
            ILogger logger
        )
        {
            var run = DateTime.Now;
            logger.LogInformation($"C# Timer trigger function executed at: {run}");

            //Need to retrieve the latest cursor value
            var initialCursor = DateTime.Now.AddMinutes(-31).ToUniversalTime().ToString(cursorFormat);

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
            await analytics.Authenticate(tenantId, _local);

            do
            {
                var timer = Stopwatch.StartNew();
                //get next cursors within the safety boundaries
                nextCursor = await analytics.GetNextCursor($"datetime({lastCursor})", _safetyLag, _take);
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

                lastCursor = ((JValue)events.LastOrDefault()[CursorColumn])?.Value<string>() ?? null;

            } while (true);

            logger.LogInformation("Complete");
        }

        private static async Task SendEvents(this IAsyncCollector<string> outputEvents, IEnumerable<object> events, ILogger logger)
        {
            var settings = new JsonSerializerSettings();
            settings.NullValueHandling = NullValueHandling.Ignore;
            settings.DefaultValueHandling = DefaultValueHandling.Ignore;

            int i = 0;
            foreach (var e in events)
            {
                i++;
                var serialized = JsonConvert.SerializeObject(e, settings);
                await outputEvents.AddAsync(serialized);
                if (i % 10 == 0)
                {
                    logger.LogDebug($"{i} Events sent");
                }
            }

            await outputEvents.FlushAsync();
            logger.LogInformation($"{i} Events sent successfully");
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
