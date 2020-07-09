using Microsoft.Extensions.Logging;
using Newtonsoft.Json;
using System.Threading.Tasks;
using System.Net.Http;
using System.Text;
using System.Collections.Generic;
using Newtonsoft.Json.Linq;
using Microsoft.Azure.Services.AppAuthentication;
using System;

namespace Rbkl.io
{
    public class LogAnalyticQuery
    {
        private static LogAnalyticQuery _instance;
        private static object _lock = new object();
        private static HttpClient _http = new HttpClient();
        private ILogger _log;
        private string _bearer;

        private LogAnalyticQuery(ILogger log) { _log = log; }
        public static LogAnalyticQuery GetInstance(ILogger log)
        {
            if (_instance == null)
            {
                lock (_lock)
                {
                    if (_instance == null)
                    {
                        _instance = new LogAnalyticQuery(log);
                    }
                }
            }
            return _instance;
        }

        ///Authenticate using Serivce Principal
        ///https://dev.loganalytics.io/documentation/Authorization/AAD-Setup
        public async Task Authenticate(string directoryId, bool useMSI, string clientId, string clientSecret)
        {
            //Authenticate with MSI
            if (!useMSI)
            {
                var tokenProvider = new AzureServiceTokenProvider();
                _bearer = await tokenProvider.GetAccessTokenAsync("https://api.loganalytics.io");
                return;
            }

            //Use to authenticate without MSI
            var parameters = new Dictionary<string, string>{
                {"grant_type","client_credentials"},
                {"client_id", clientId},
                {"client_secret",clientSecret},
                {"resource", "https://api.loganalytics.io"}
            };

            var body = new FormUrlEncodedContent(parameters);
            var response = await _http.PostAsync($"https://login.microsoftonline.com/{directoryId}/oauth2/token", body);
            _log.LogInformation($"Authentication: {response.StatusCode.ToString()}");
            var content = await response.Content.ReadAsStringAsync();
            _log.LogDebug(content);
            _log.LogDebug(JsonConvert.SerializeObject(response.Headers));
            dynamic token = JsonConvert.DeserializeObject(content);
            _bearer = (string)token.access_token;
            _log.LogDebug(_bearer);
        }

        public async Task<IList<Dictionary<string, object>>> ExecuteQuery(string workspaceId, string kusto)
        {
            var settings = new JsonSerializerSettings();
            settings.NullValueHandling = NullValueHandling.Ignore;
            settings.DefaultValueHandling = DefaultValueHandling.Ignore;

            var content = await QueryApi(workspaceId, kusto);

            try
            {
                _log.LogInformation($"Parsing results...");
                dynamic tables = JsonConvert.DeserializeObject(content, settings);
                var columns = (JArray)tables.tables[0].columns;
                _log.LogInformation($"Retrieved {columns.Count} columns");
                var rows = (JArray)tables.tables[0].rows;
                _log.LogInformation($"Retrieved {rows.Count} rows");

                return MergeResults(columns, rows);
            }
            catch
            {
                _log.LogError(content);
                throw;
            }
        }

        public async Task<IEnumerable<Tuple<string, long>>> GetNextCursors(string workspaceId, string kusto, int max)
        {
            var content = await QueryApi(workspaceId, kusto);

            try
            {
                dynamic tables = JsonConvert.DeserializeObject(content);
                var rows = (JArray)tables.tables[0].rows;
                _log.LogInformation($"Retrieved {rows.Count} rows");
                return GroupCursors(rows, max);
            }
            catch
            {
                _log.LogError(content);
                throw;
            }
        }

        public async Task<string> QueryApi(string workspaceId, string kusto)
        {
            string APIURL = $"https://api.loganalytics.io/v1/workspaces/{workspaceId}/query";

            var stringPayload = JsonConvert.SerializeObject(new { query = kusto });
            _log.LogInformation(stringPayload);

            var httpRequest = new HttpRequestMessage(HttpMethod.Post, APIURL);
            httpRequest.Content = new StringContent(stringPayload, Encoding.UTF8, "application/json");
            httpRequest.Headers.Add("Authorization", "Bearer " + _bearer);

            var response = await _http.SendAsync(httpRequest);
            _log.LogInformation($"Query result: {response.StatusCode.ToString()}");
            var content = await response.Content.ReadAsStringAsync();
            _log.LogDebug(JsonConvert.SerializeObject(response.Headers));
            
            if (!response.IsSuccessStatusCode)
            {
                _log.LogError(content);
                throw new HttpRequestException(content);
            }
            return content;
        }

        ///Parse the result to an array of JSON objects, instead of mutlipel arrays,
        ///https://dev.loganalytics.io/documentation/Using-the-API/ResponseFormat
        private IList<Dictionary<string, object>> MergeResults(JArray columns, JArray rows)
        {
            string columnMetaName = "name";
            _log.LogDebug("merge table results");
            var result = new List<Dictionary<string, object>>();

            foreach (var r in rows.Values<JArray>())
            {
                //_log.LogInformation($"r = {r.ToString()}");
                var item = new Dictionary<string, object>();
                for (int i = 0; i < columns.Count; i++)
                {
                    //Add only the field if there is a value
                    if (!string.IsNullOrEmpty(r[i].ToString()))
                    {
                        var columnName = columns[i].SelectToken(columnMetaName).ToString();
                        item.Add(columnName, r[i]);
                    }
                }
                result.Add(item);
            }
            return result;
        }
        
        ///Input rows contains 3 columsn
        /// 0: Bin grouping
        /// 1: cursor
        /// 2: count
        private IEnumerable<Tuple<string, long>> GroupCursors(JArray rows, int max)
        {
            long sum = 0;
            for (int i = 0; i < rows.Count; i++)
            {
                //_log.LogDebug($"r = {rows[i]}");
                string lastCursor = rows[i].Value<JArray>()[1].Value<string>();
                sum += rows[i].Value<JArray>()[2].Value<long>();
                if (sum >= max || rows.Count-1 == i)
                {
                    yield return new Tuple<string, long>(lastCursor, sum);
                    sum = 0;
                }
            }
        }
    }

    ///CloudTable Log Tail Pattern implementation
    ///https://docs.microsoft.com/en-us/azure/storage/tables/table-storage-design-patterns#log-tail-pattern

}