using Dynamicweb.Core;
using Dynamicweb.DataIntegration.EndpointManagement;
using Dynamicweb.DataIntegration.Integration;
using Dynamicweb.DataIntegration.Integration.Interfaces;
using Dynamicweb.DataIntegration.Providers.ODataProvider.Interfaces;
using Dynamicweb.DataIntegration.Providers.ODataProvider.Model;
using Dynamicweb.Logging;
using Newtonsoft.Json;
using System;
using System.Collections.Generic;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Net;
using System.Threading;
using System.Threading.Tasks;

namespace Dynamicweb.DataIntegration.Providers.ODataProvider
{
    internal class EndpointSourceReader : ISourceReader
    {
        private readonly IHttpRestClient _httpRestClient;
        private readonly EndpointAuthentication _endpointAuthentication;
        private readonly ILogger _logger;
        private readonly Mapping _mapping;
        private readonly Endpoint _endpoint;
        private readonly string _mode;
        private readonly int _maximumPageSize;
        private readonly string _nextPaginationUrlName;
        private Dictionary<string, object> _nextItem;
        private IEnumerable<Dictionary<string, object>> _responseResult;
        private List<Dictionary<string, object>> _totalResponseResult;
        private IEnumerator<Dictionary<string, object>> _responseEnumerator;
        private string _paginationUrl;
        private readonly string _highWaterMarkMapPath = SystemInformation.MapPath("/Files/Integration/HighWaterMark/");
        private readonly string _requestResponseMapPath = SystemInformation.MapPath("/Files/Integration/Responses/");
        private bool? _hasFormattedValues;
        private readonly string _odataFormattedValue = "OData.Community.Display.V1.FormattedValue";
        private readonly int _requestIntervals;
        private int _requestCounter = 1;
        private readonly bool _doNotStoreLastResponseInLogFile;
        private bool _requestTimedOutFromGlobalSettings;

        internal void SaveRequestResponseFile()
        {
            if (!_doNotStoreLastResponseInLogFile)
            {
                string mapPath = _requestResponseMapPath;
                if (!Directory.Exists(mapPath))
                {
                    Directory.CreateDirectory(mapPath);
                }
                string logFileName = Scheduling.Task.MakeSafeFileName(_mapping.Job.Name) + $"_{_mapping.SourceTable.Name}.log";
                using (TextWriter writer = File.CreateText(mapPath.CombinePaths(logFileName)))
                {
                    var serializer = new JsonSerializer();
                    serializer.Serialize(writer, _totalResponseResult);
                }
            }
        }

        internal void SaveHighWaterMarkFile(List<Dictionary<string, object>> sourceRow)
        {
            string mapPath = _highWaterMarkMapPath;
            if (!Directory.Exists(mapPath))
            {
                Directory.CreateDirectory(mapPath);
            }
            string logFileName = Scheduling.Task.MakeSafeFileName(_mapping.Job.Name) + $"_{_mapping.SourceTable.Name}.log";
            if (File.Exists(mapPath.CombinePaths(logFileName)))
            {
                File.Delete(mapPath.CombinePaths(logFileName));
            }
            File.WriteAllText(mapPath.CombinePaths(logFileName), JsonConvert.SerializeObject(sourceRow));
        }

        private void DeleteHighWaterMarkFile()
        {
            string mapPath = _highWaterMarkMapPath;
            string logFileName = Scheduling.Task.MakeSafeFileName(_mapping.Job.Name) + $"_{_mapping.SourceTable.Name}.log";
            if (File.Exists(mapPath.CombinePaths(logFileName)))
            {
                File.Delete(mapPath.CombinePaths(logFileName));
            }
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="EndpointSourceReader"/> class.
        /// </summary>
        /// <param name="httpRestClient">The HTTP rest client.</param>
        /// <param name="logger">The logger.</param>
        /// <param name="mapping">The mapping.</param>
        /// <param name="endpoint">The endpoint.</param>
        /// <param name="nextPaginationUrlName">Name of the next pagination URL. "odata.nextLink" (case insensitive) is supposed to be a standard.</param>
        internal EndpointSourceReader(IHttpRestClient httpRestClient, ILogger logger, Mapping mapping, Endpoint endpoint, string mode, int maximumPageSize, EndpointAuthenticationService endpointAuthenticationService, bool readFromLastRequestResponse, int requestIntervals, bool doNotStoreLastResponseInLogFile, string nextPaginationUrlName = "odata.nextLink")
        {
            _totalResponseResult = new List<Dictionary<string, object>>();
            _httpRestClient = httpRestClient;
            _logger = logger;
            _mapping = mapping;
            _endpoint = endpoint;
            _mode = mode;
            _maximumPageSize = maximumPageSize;
            _nextPaginationUrlName = nextPaginationUrlName;
            _requestIntervals = requestIntervals;
            _doNotStoreLastResponseInLogFile = doNotStoreLastResponseInLogFile;
            string logFileName = Scheduling.Task.MakeSafeFileName(mapping.Job.Name) + $"_{_mapping.SourceTable.Name}.log";
            _endpointAuthentication = endpointAuthenticationService.GetEndpointAuthenticationById(_endpoint.AuthenticationId);
            if (File.Exists(_highWaterMarkMapPath.CombinePaths(logFileName)))
            {
                _responseResult = JsonConvert.DeserializeObject<IEnumerable<Dictionary<string, object>>>(File.ReadAllText(_highWaterMarkMapPath.CombinePaths(logFileName)));
                if (_responseResult.First().TryGetValue("@odata.nextLink", out var paginationUrl))
                {
                    HandleRequest(paginationUrl.ToString(), $"Starting reading data from endpoint: '{_endpoint.Name}', using URL: '{paginationUrl}'");
                }
                _responseEnumerator = _responseResult.GetEnumerator();
            }
            else if (readFromLastRequestResponse && File.Exists(_requestResponseMapPath.CombinePaths(logFileName)))
            {
                _responseResult = _totalResponseResult = JsonConvert.DeserializeObject<List<Dictionary<string, object>>>(File.ReadAllText(_requestResponseMapPath.CombinePaths(logFileName)));
                _responseEnumerator = _responseResult.GetEnumerator();
            }
            else
            {
                if (readFromLastRequestResponse)
                {
                    _logger?.Info("Request file does not exists, now fetching from endpoint.");
                }

                _endpoint.Parameters = AddFilterAndSelectValuesToEndpointParameters(_endpoint.Parameters);
                AddConfigurableAddInsSelectionsToEndpoint();
                if (BaseEndpointProvider.EndpointIsLoadAllEntities(_endpoint.Url))
                {
                    _endpoint.Url = new Uri(new Uri(_endpoint.Url), mapping.SourceTable.Name).AbsoluteUri;
                }
                string url = _endpoint.FullUrl;
                HandleRequest(url, $"Starting reading data from endpoint: '{_endpoint.Name}', using URL: '{url}'");
            }
        }

        internal void AddConfigurableAddInsSelectionsToEndpoint()
        {
            if (_maximumPageSize > 0)
            {
                _endpoint.Headers = AddOrUpdateParameter(_endpoint.Headers, "prefer", "maxpagesize=" + _maximumPageSize);
            }
            if (_mode == "First page")
            {
                _endpoint.Parameters = AddOrUpdateParameter(_endpoint.Parameters, "$top", _maximumPageSize.ToString());
            }
            if (_mode == "Delta Replication")
            {
                DateTime? lastRunDateTime = _mapping.Job.LastSuccessfulRun;
                if (lastRunDateTime != null)
                {
                    DateTime dateTimeInUtc = TimeZoneInfo.ConvertTimeToUtc(lastRunDateTime.Value);
                    string dateTimeFilterName = "";
                    bool isEdmDate = false;

                    foreach (var column in _mapping.SourceTable.Columns)
                    {
                        switch (column.Name)
                        {
                            case "Last_Date_Modified":
                                dateTimeFilterName = "Last_Date_Modified";
                                isEdmDate = true;
                                break;
                            case "Order_Date":
                                dateTimeFilterName = "Order_Date";
                                isEdmDate = true;
                                break;
                            case "LastDateTimeModified":
                                dateTimeFilterName = "LastDateTimeModified";
                                break;
                            case "lastModifiedDateTime":
                                dateTimeFilterName = "lastModifiedDateTime";
                                break;
                            case "modifiedon":
                                dateTimeFilterName = "modifiedon";
                                break;
                        }
                    }
                    if (!string.IsNullOrWhiteSpace(dateTimeFilterName))
                    {
                        if (isEdmDate)
                        {
                            dateTimeFilterName += " ge " + dateTimeInUtc.ToString("yyyy-MM-dd", CultureInfo.InvariantCulture) + "z";
                        }
                        else
                        {
                            dateTimeFilterName += " gt " + dateTimeInUtc.ToString("yyyy-MM-ddTHH:mm:ss.fff", CultureInfo.InvariantCulture) + "z";
                        }
                        _endpoint.Parameters = AddOrUpdateParameter(_endpoint.Parameters, "$filter", dateTimeFilterName);
                    }
                }
            }
        }

        public static IDictionary<string, string> AddOrUpdateParameter(IDictionary<string, string> parameters, string parameterName, string parameterValue)
        {
            IDictionary<string, string> result = parameters;
            if (result.ContainsKey(parameterName))
            {
                result[parameterName] = parameterValue;
            }
            else
            {
                result.Add(parameterName, parameterValue);
            }
            return result;
        }

        public static IDictionary<string, string> RemoveParameter(IDictionary<string, string> parameters, string parameterName, string parameterValue)
        {
            IDictionary<string, string> result = parameters;
            if (result.ContainsKey(parameterName))
            {
                if (result[parameterName] == parameterValue)
                {
                    result.Remove(parameterName);
                }
                else
                {
                    result[parameterName] = result[parameterName].Replace(parameterValue, "");
                }
            }
            return result;
        }

        private IDictionary<string, string> AddFilterAndSelectValuesToEndpointParameters(IDictionary<string, string> parameters)
        {
            IDictionary<string, string> result = parameters;
            var activeColumnMappings = _mapping.GetColumnMappings().Where(obj => obj.Active).ToList();
            if (activeColumnMappings.Any())
            {
                var selectColumnNames = activeColumnMappings.Where(obj => obj.SourceColumn != null)?.Select(obj => obj.SourceColumn.Name).ToList();
                if (!parameters.TryGetValue("$select", out _))
                {
                    result.Add("$select", string.Join(",", selectColumnNames));
                }
                else
                {
                    result["$select"] += "," + string.Join(",", selectColumnNames);
                }
            }

            var mappingConditionals = _mapping.Conditionals.ToList();
            if (mappingConditionals.Any())
            {
                List<string> filterValues = new List<string>();
                foreach (var item in mappingConditionals)
                {
                    string condition = item.Condition;
                    string operatorInOData = "";
                    switch (item.ConditionalOperator)
                    {
                        case ConditionalOperator.EqualTo:
                            operatorInOData = "eq";
                            break;
                        case ConditionalOperator.LessThan:
                            operatorInOData = "lt";
                            break;
                        case ConditionalOperator.GreaterThan:
                            operatorInOData = "gt";
                            break;
                        case ConditionalOperator.DifferentFrom:
                            operatorInOData = "ne";
                            break;
                        case ConditionalOperator.Contains:
                            filterValues.Add($"contains({item.SourceColumn.Name},'{item.Condition}')");
                            continue;
                        case ConditionalOperator.In:
                            operatorInOData = "eq";
                            List<string> conditions = item.Condition.Split(',').ToList();
                            if (item.SourceColumn.Type == typeof(string))
                            {
                                condition = $"{string.Join($"' or {item.SourceColumn.Name} eq '", conditions)}";
                            }
                            else
                            {
                                condition = $"{string.Join($" or {item.SourceColumn.Name} eq ", conditions)}";
                            }
                            break;

                    }
                    if (item.SourceColumn.Type == typeof(string))
                    {
                        filterValues.Add($"({item.SourceColumn.Name} {operatorInOData} '{condition}')");
                    }
                    else
                    {
                        filterValues.Add($"({item.SourceColumn.Name} {operatorInOData} {condition})");
                    }
                }
                if (filterValues.Any())
                {
                    if (!parameters.TryGetValue("$filter", out _))
                    {
                        result.Add("$filter", string.Join(" and ", filterValues));
                    }
                    else
                    {
                        result["$filter"] += " and " + string.Join(" and ", filterValues);
                    }
                }
            }
            return result;
        }

        /// <summary>
        /// Handles a specified response stream, by creating an IEnumerable with yield return, so we can later enumerate the result one object at a time without loading them all into memory
        /// </summary>
        /// <param name="responseStream">The response stream.</param>
        /// <param name="responseStatusCode">The response status code.</param>
        /// <param name="responseHeaders">The response headers.</param>
        private void HandleStream(Stream responseStream, HttpStatusCode responseStatusCode, Dictionary<string, string> responseHeaders)
        {
            _responseResult = ExtractStream(responseStream);
            _responseEnumerator = _responseResult.GetEnumerator();
        }

        /// <summary>
        /// Extracts and parses an OData stream into an IEnumerable of objects using yield return.
        /// </summary>
        /// <param name="responseStream">The response stream.</param>
        /// <returns></returns>
        private IEnumerable<Dictionary<string, object>> ExtractStream(Stream responseStream)
        {
            var serializer = new JsonSerializer();

            using (var streamReader = new StreamReader(responseStream))
            {
                using (var jsonTextReader = new JsonTextReader(streamReader))
                {
                    // Iterate through the Stream
                    while (jsonTextReader.Read())
                    {
                        // If this is a dataobject yield return it
                        if (jsonTextReader.TokenType == JsonToken.StartObject && jsonTextReader.Path.Contains("value["))
                        {
                            Dictionary<string, object> deserializedJson = serializer.Deserialize<Dictionary<string, object>>(jsonTextReader);
                            if (!_hasFormattedValues.HasValue)
                            {
                                _hasFormattedValues = deserializedJson.Any(kvp => kvp.Key.Contains($"@{_odataFormattedValue}", StringComparison.OrdinalIgnoreCase));
                            }
                            if (_hasFormattedValues.Value)
                            {
                                var columnMappings = _mapping.SourceTable.Columns;
                                var listOfBooleans = columnMappings.Where(obj => obj.Type == typeof(bool));
                                var keyValuePairsWithFormattedValues = deserializedJson.Where(obj => obj.Key.Contains($"@{_odataFormattedValue}", StringComparison.OrdinalIgnoreCase)).ToList();
                                foreach (var item in keyValuePairsWithFormattedValues)
                                {
                                    string key = item.Key.Replace($"@{_odataFormattedValue}", "");
                                    if (!listOfBooleans.Any(obj => obj.Name == key))
                                    {
                                        deserializedJson[key] = item.Value;
                                    }
                                }
                            }
                            if (!_doNotStoreLastResponseInLogFile)
                            {
                                _totalResponseResult.Add(deserializedJson);
                            }
                            yield return deserializedJson;
                        }

                        // If this is a pagination link store it for later pagination
                        if (jsonTextReader.TokenType == JsonToken.PropertyName && jsonTextReader.Path.Contains(_nextPaginationUrlName)) //Todo: Make case insensitive once new DW.Core package is out with Contains extension
                        {
                            _paginationUrl = jsonTextReader.ReadAsString();
                        }
                    }

                    // If we reach the end of the Stream yield return null.
                    yield return null;
                }
            }
        }

        private bool HandleRequest(string url, string loggerInfo)
        {
            if (CheckIfEndpointIsReadyForUse(url))
            {
                _logger?.Info(loggerInfo);
                Task task;
                int timeoutInMilliseconds = 20 * 60 * 1000; //20 minutes
                string globalSettingTimeout = Configuration.SystemConfiguration.Instance.GetValue("/Globalsettings/Modules/DataIntegration/Job/TimeoutInMilliseconds");
                if (!string.IsNullOrEmpty(globalSettingTimeout))
                {
                    int globalSettingTimeoutAsInt = Converter.ToInt32(globalSettingTimeout);
                    if (globalSettingTimeoutAsInt > 0)
                    {
                        timeoutInMilliseconds = globalSettingTimeoutAsInt;
                        _requestTimedOutFromGlobalSettings = true;
                    }
                }
                if (_endpointAuthentication.IsTokenBased())
                {
                    string token = OAuthHelper.GetToken(_endpoint, _endpointAuthentication);
                    task = RetryHelper.RetryOnExceptionAsync<Exception>(10, async () => { _httpRestClient.GetAsync(url, HandleStream, token, (Dictionary<string, string>)_endpoint.Headers).Wait(new CancellationTokenSource(timeoutInMilliseconds).Token); }, _logger);
                }
                else
                {
                    task = RetryHelper.RetryOnExceptionAsync<Exception>(10, async () => { _httpRestClient.GetAsync(url, HandleStream, _endpointAuthentication, (Dictionary<string, string>)_endpoint.Headers).Wait(new CancellationTokenSource(timeoutInMilliseconds).Token); }, _logger);
                }
                if (task.IsCanceled)
                {
                    string aditionalErrorMSG = _requestTimedOutFromGlobalSettings ? "(To change go to global settings and look for TimeoutInMilliseconds)" : "";
                    throw new TimeoutException($"Request has timed out with a wait of {timeoutInMilliseconds} in milliseconds {aditionalErrorMSG}");
                }
                task.Wait();
                _logger?.Info("Data received, now processing data.");
                return true;
            }
            else
            {
                _logger?.Info($"Endpoint: '{_endpoint.Name}' is not ready for use on URL: '{url}'");
                return false;
            }
        }

        /// <inheritdoc />
        public bool IsDone()
        {
            var moveNext = _responseEnumerator.MoveNext();
            if (_requestIntervals == 0 || _requestIntervals > _requestCounter || (moveNext && string.IsNullOrWhiteSpace(_paginationUrl)))
            {
                if (!moveNext)
                {
                    FinishJob();
                    return true;
                }
                _nextItem = _responseEnumerator.Current;
                if (_nextItem != null)
                {
                    return false;
                }
                if (string.IsNullOrWhiteSpace(_paginationUrl))
                {
                    FinishJob();
                    return true;
                }
                if (HandleRequest(_paginationUrl, $"Paginating request to endpoint: '{_endpoint.Name}', using URL: '{_paginationUrl}'"))
                {
                    _requestCounter++;
                }
                else
                {
                    return true;
                }
                _paginationUrl = null;
                moveNext = _responseEnumerator.MoveNext();
                if (!moveNext)
                {
                    FinishJob();
                    return true;
                }
                _nextItem = _responseEnumerator.Current;
                if (_nextItem is null)
                {
                    FinishJob();
                    return true;
                }
                return false;
            }
            else
            {
                FinishJob();
                var nextPaginationUrl = new List<Dictionary<string, object>>
                {
                    new Dictionary<string, object>
                    {
                        { "@odata.nextLink", _paginationUrl }
                    }
                };
                SaveHighWaterMarkFile(nextPaginationUrl);
                return true;
            }
        }

        private bool CheckIfEndpointIsReadyForUse(string url)
        {
            string checkUrl = url;
            if (url.Contains("?"))
            {
                checkUrl = checkUrl.Split('?')[0];
            }
            checkUrl += "?$top=1";
            bool result = false;
            Task task;
            if (_endpointAuthentication.IsTokenBased())
            {
                string token = OAuthHelper.GetToken(_endpoint, _endpointAuthentication);
                task = _httpRestClient.GetAsync(checkUrl, HandleResponse, token);
            }
            else
            {
                task = _httpRestClient.GetAsync(checkUrl, HandleResponse, _endpointAuthentication);
            }
            task.Wait();
            void HandleResponse(Stream responseStream, HttpStatusCode responseStatusCode, Dictionary<string, string> responseHeaders)
            {
                if (responseStatusCode == HttpStatusCode.OK)
                {
                    result = true;
                }
            }
            return result;
        }

        private void FinishJob()
        {
            DeleteHighWaterMarkFile();
            SaveRequestResponseFile();
        }

        /// <inheritdoc />
        public Dictionary<string, object> GetNext()
        {
            return _nextItem;
        }

        /// <inheritdoc />
        public void Dispose()
        {
            _responseEnumerator?.Dispose();
        }
    }
}
