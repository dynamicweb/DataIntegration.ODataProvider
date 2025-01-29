using Dynamicweb.Core;
using Dynamicweb.DataIntegration.EndpointManagement;
using Dynamicweb.DataIntegration.Integration;
using Dynamicweb.DataIntegration.Integration.Interfaces;
using Dynamicweb.DataIntegration.Providers.ODataProvider.Interfaces;
using Dynamicweb.DataIntegration.Providers.ODataProvider.Model;
using Dynamicweb.Logging;
using System;
using System.Collections.Generic;
using System.Data;
using System.IO;
using System.Linq;
using System.Net;
using System.Text;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;

namespace Dynamicweb.DataIntegration.Providers.ODataProvider;

internal class ODataSourceReader : ISourceReader
{
    private readonly IHttpRestClient _httpRestClient;
    private readonly ILogger _logger;
    private readonly Mapping _mapping;
    private readonly Endpoint _endpoint;
    private readonly string _mode;
    private readonly string _deltaModifier;
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
    private readonly int _maximumCharacterLengthOfAutoAddedSelectStatement = 1250;
    private readonly int _timeoutInMilliseconds;
    private readonly bool _failJobOnEndpointIsBusy;

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
                writer.Write(JsonSerializer.Serialize(_totalResponseResult));
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
        File.WriteAllText(mapPath.CombinePaths(logFileName), JsonSerializer.Serialize(sourceRow));
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
    /// Initializes a new instance of the <see cref="ODataSourceReader"/> class.
    /// </summary>
    /// <param name="httpRestClient">The HTTP rest client.</param>
    /// <param name="logger">The logger.</param>
    /// <param name="mapping">The mapping.</param>
    /// <param name="endpoint">The endpoint.</param>
    /// <param name="nextPaginationUrlName">Name of the next pagination URL. "odata.nextLink" (case insensitive) is supposed to be a standard.</param>
    internal ODataSourceReader(IHttpRestClient httpRestClient, ILogger logger, Mapping mapping, Endpoint endpoint, string mode, string deltaModifier, int maximumPageSize, bool readFromLastRequestResponse, int requestIntervals, bool doNotStoreLastResponseInLogFile, bool failJobOnEndpointIsBusy, string nextPaginationUrlName = "odata.nextLink")
    {
        _totalResponseResult = new List<Dictionary<string, object>>();
        _httpRestClient = httpRestClient;
        _logger = logger;
        _mapping = mapping;
        _endpoint = endpoint;
        _mode = mode;
        _deltaModifier = deltaModifier;
        _maximumPageSize = maximumPageSize;
        _nextPaginationUrlName = nextPaginationUrlName;
        _requestIntervals = requestIntervals;
        _doNotStoreLastResponseInLogFile = doNotStoreLastResponseInLogFile;
        _timeoutInMilliseconds = GetTimeOutInMilliseconds();
        _failJobOnEndpointIsBusy = failJobOnEndpointIsBusy;
        string logFileName = Scheduling.Task.MakeSafeFileName(mapping.Job.Name) + $"_{_mapping.SourceTable.Name}.log";

        IDictionary<string, string> headers = GetAllHeaders();

        if (File.Exists(_highWaterMarkMapPath.CombinePaths(logFileName)))
        {
            _responseResult = JsonSerializer.Deserialize<IEnumerable<Dictionary<string, object>>>(File.ReadAllText(_highWaterMarkMapPath.CombinePaths(logFileName)));
            if (_responseResult.First().TryGetValue("@odata.nextLink", out var paginationUrl))
            {
                HandleRequest(paginationUrl.ToString(), $"Starting reading data from endpoint: '{_endpoint.Name}', using URL: '{paginationUrl}'", headers);
            }
            _responseEnumerator = _responseResult.GetEnumerator();
        }
        else if (readFromLastRequestResponse && File.Exists(_requestResponseMapPath.CombinePaths(logFileName)))
        {
            var previusMapping = _mapping.Job.Mappings.Where(mapping => (mapping.SourceTable?.Name ?? "").Equals(_mapping.SourceTable?.Name ?? "", StringComparison.OrdinalIgnoreCase) && mapping.GetId() < _mapping.GetId())?.OrderByDescending(obj => obj.GetId())?.FirstOrDefault();
            if (previusMapping != null)
            {
                var previusMappingFilters = GetFilterAsParameters(previusMapping);
                var currentMappingFilters = GetFilterAsParameters(_mapping);
                if (previusMappingFilters.Except(currentMappingFilters, StringComparer.OrdinalIgnoreCase).Concat(currentMappingFilters.Except(previusMappingFilters, StringComparer.OrdinalIgnoreCase)).Any())
                {
                    CallEndpoing(headers, readFromLastRequestResponse);
                    return;
                }
            }

            List<Dictionary<string, JsonElement>> deserializedJsons = JsonSerializer.Deserialize<List<Dictionary<string, JsonElement>>>(File.ReadAllText(_requestResponseMapPath.CombinePaths(logFileName)));

            _totalResponseResult = deserializedJsons.Select(dict => dict.ToDictionary(obj => obj.Key, obj => obj.Value.ValueKind == JsonValueKind.String ? (object)obj.Value.GetString() : (object)obj.Value.GetRawText())).ToList();
            _responseResult = _totalResponseResult;
            _responseEnumerator = _responseResult.GetEnumerator();
        }
        else
        {
            if (readFromLastRequestResponse)
            {
                _logger?.Info("Last response file does not exists, now fetching data from the endpoint.");
            }

            CallEndpoing(headers, readFromLastRequestResponse);
        }
    }

    internal void CallEndpoing(IDictionary<string, string> headers, bool readFromLastRequestResponse)
    {
        IDictionary<string, string> parameters = new Dictionary<string, string>();

        if (_mode != null && _mode.Equals("First page", StringComparison.OrdinalIgnoreCase) && _maximumPageSize > 0)
        {
            parameters.Add("$top", _maximumPageSize.ToString());
        }

        var selectAsParameters = GetSelectAsParameters(readFromLastRequestResponse);
        var modeAsParemters = GetModeAsParameters();
        var filterAsParameters = GetFilterAsParameters(_mapping);
        var expandAsParameters = GetExpandAsParameters(_mapping);

        if (!string.IsNullOrEmpty(modeAsParemters))
        {
            filterAsParameters.Add(modeAsParemters);
        }

        if (selectAsParameters.Count != 0)
        {
            parameters.Add("$select", string.Join(",", selectAsParameters));
        }

        if (filterAsParameters.Count != 0)
        {
            parameters.Add("$filter", string.Join(" and ", filterAsParameters));
        }

        if (expandAsParameters.Count != 0)
        {
            //It is possible to use $select for the middle table by just separating select and expand with semicolon:
            //Customers?$select=CustomerID&$expand=Orders($select=OrderID;$expand=Order_Details($select=UnitPrice))
            parameters.Add("$expand", string.Join(",", expandAsParameters));
        }

        if (_endpoint.Parameters != null)
        {
            foreach (var parameter in _endpoint.Parameters)
            {
                if (!parameters.ContainsKey(parameter.Key))
                {
                    parameters.Add(parameter.Key, parameter.Value);
                }
                else if (parameter.Key.Equals("$filter", StringComparison.OrdinalIgnoreCase) && !ODataProvider.EndpointIsLoadAllEntities(_endpoint.Url))
                {
                    parameters[parameter.Key] = $"{parameters[parameter.Key]} and {parameter.Value}";
                }
            }
        }

        string url = GetEndpointURL(_endpoint.Url, _mapping.SourceTable.Name, "", parameters);
        HandleRequest(url, $"Starting reading data from endpoint: '{_endpoint.Name}', using URL: '{url}'", headers);
    }

    internal IDictionary<string, string> GetAllHeaders()
    {
        IDictionary<string, string> result = new Dictionary<string, string>();
        if (_maximumPageSize > 0)
        {
            result.Add("prefer", "odata.maxpagesize=" + _maximumPageSize);
        }
        if (_endpoint.Headers != null)
        {
            foreach (var header in _endpoint.Headers)
            {
                if (!result.ContainsKey(header.Key))
                {
                    result.Add(header.Key, header.Value);
                }
            }
        }
        return result;
    }

    public static string GetEndpointURL(string baseURL, string tableName, string patchURL, IDictionary<string, string> parameters = null)
    {
        string result = baseURL;
        if (ODataProvider.EndpointIsLoadAllEntities(baseURL))
        {
            if (!baseURL.EndsWith("/") && !baseURL.EndsWith("metadata", StringComparison.OrdinalIgnoreCase))
            {
                baseURL += "/";
            }
            result = new Uri(new Uri(baseURL), tableName).AbsoluteUri;
        }
        if (!string.IsNullOrEmpty(patchURL))
        {
            result += patchURL;
        }

        if (parameters != null && parameters.Count > 0)
        {
            StringBuilder stringBuilder = new StringBuilder(string.Empty);
            foreach (KeyValuePair<string, string> parameter in parameters)
            {
                stringBuilder.Append("&" + parameter.Key + "=" + WebUtility.UrlEncode(parameter.Value));
            }

            if (stringBuilder.Length > 0)
            {
                if (!result.Contains("?"))
                {
                    return $"{result}?{stringBuilder.Remove(0, 1)}";
                }

                if (!result.EndsWith("?"))
                {
                    return $"{result}{stringBuilder}";
                }

                return $"{result}{stringBuilder.Remove(0, 1)}";
            }
        }

        return result;
    }

    private string GetModeAsParameters()
    {
        string result = "";
        if (_mode != null && _mode.Equals("Delta Replication", StringComparison.OrdinalIgnoreCase))
        {
            DateTime? lastRunDateTime = _mapping.Job.LastSuccessfulRun;
            if (lastRunDateTime != null)
            {
                string dateTimeFilterName = "";
                bool isEdmDate = false;

                if (!string.IsNullOrEmpty(_deltaModifier))
                {
                    List<string> deltaModifiers = _deltaModifier.Split(',').Select(val => val.Trim()).ToList();
                    foreach (var delta in deltaModifiers)
                    {
                        if (_mapping.SourceTable.Columns.Any(obj => obj.Name.Equals(delta, StringComparison.OrdinalIgnoreCase)))
                        {
                            var dateTimeFilterColumn = _mapping.SourceTable.Columns.Where(obj => obj.Name.Equals(delta, StringComparison.OrdinalIgnoreCase)).First();
                            isEdmDate = dateTimeFilterColumn.Type == typeof(DateOnly);
                            dateTimeFilterName = dateTimeFilterColumn.Name;
                            break;
                        }
                    }
                }

                if (string.IsNullOrWhiteSpace(dateTimeFilterName))
                {
                    foreach (var column in _mapping.SourceTable.Columns)
                    {
                        switch (column.Name)
                        {
                            case "Last_Date_Modified":
                                dateTimeFilterName = "Last_Date_Modified";
                                break;
                            case "Order_Date":
                                dateTimeFilterName = "Order_Date";
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
                        isEdmDate = column.Type == typeof(DateOnly);
                    }
                }

                if (!string.IsNullOrWhiteSpace(dateTimeFilterName))
                {
                    var theDateTime = ODataWriter.GetTheDateTimeInZeroTimeZone(lastRunDateTime.Value, isEdmDate);
                    if (isEdmDate)
                    {
                        dateTimeFilterName += " ge " + theDateTime;
                    }
                    else
                    {
                        dateTimeFilterName += " gt " + theDateTime;
                    }
                    result = dateTimeFilterName;
                }
            }
        }
        return result;
    }

    private List<string> GetSelectAsParameters(bool readFromLastRequestResponse)
    {
        List<string> result = new();
        var activeColumnMappings = _mapping.GetColumnMappings().Where(obj => obj.Active && obj.SourceColumn != null).ToList();
        if (activeColumnMappings.Any())
        {
            var selectColumnNames = activeColumnMappings.Where(obj => string.IsNullOrEmpty(obj.SourceColumn.Group))?.Select(obj => obj.SourceColumn.Name).ToList();

            if (readFromLastRequestResponse)
            {
                var theJobsTableMappingsWithSameSourceTable = _mapping.Job.Mappings.Where(obj => obj.SourceTable.Name.Equals(_mapping.SourceTable.Name, StringComparison.OrdinalIgnoreCase)).ToList();
                if (theJobsTableMappingsWithSameSourceTable.Any())
                {
                    foreach (var tableMapping in theJobsTableMappingsWithSameSourceTable)
                    {
                        selectColumnNames.AddRange(tableMapping.GetColumnMappings().Where(obj => obj.Active && obj.SourceColumn != null && !selectColumnNames.Contains(obj.SourceColumn.Name))?.Select(obj => obj.SourceColumn.Name).ToList());
                    }
                }
            }

            selectColumnNames = selectColumnNames.Distinct(StringComparer.OrdinalIgnoreCase).ToList();

            //max limit for url-length is roughly 2048 characters, so we skip adding if there is more than 1250 in the parameters.
            var selectColumnNamesJoined = string.Join(",", selectColumnNames);
            int length = selectColumnNamesJoined.Length;
            if (length <= _maximumCharacterLengthOfAutoAddedSelectStatement)
            {
                result = selectColumnNames;
            }
            else
            {
                _logger?.Info("Detected many active column mappings, so will not auto add $select with all active column mappings and by that limit the data received from Endpoint.");
            }
        }
        return result;
    }

    private List<string> GetFilterAsParameters(Mapping mapping)
    {
        List<string> result = new();
        var mappingConditionals = mapping.Conditionals.ToList();
        if (mappingConditionals.Any())
        {
            foreach (var item in mappingConditionals)
            {
                if (item.Condition != null)
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
                            if (item.SourceColumn.Type == typeof(string))
                            {
                                result.Add($"contains({item.SourceColumn.Name},'{item.Condition}')");
                            }
                            else
                            {
                                LogWarningForConditional(item);
                            }
                            continue;
                        case ConditionalOperator.NotContains:
                            if (item.SourceColumn.Type == typeof(string))
                            {
                                result.Add($"contains({item.SourceColumn.Name},'{item.Condition}') ne true");
                            }
                            else
                            {
                                LogWarningForConditional(item);
                            }
                            continue;
                        case ConditionalOperator.In:
                            operatorInOData = "eq";
                            List<string> equalConditions = item.Condition.Split(',').Select(val => val.Trim()).ToList();
                            if (item.SourceColumn.Type == typeof(string))
                            {
                                condition = $"{string.Join($"' or {item.SourceColumn.Name} eq '", equalConditions)}";
                            }
                            else
                            {
                                condition = $"{string.Join($" or {item.SourceColumn.Name} eq ", equalConditions)}";
                            }
                            break;
                        case ConditionalOperator.NotIn:
                            operatorInOData = "ne";
                            List<string> notEqualConditions = item.Condition.Split(',').Select(val => val.Trim()).ToList();
                            if (item.SourceColumn.Type == typeof(string))
                            {
                                condition = $"{string.Join($"' and {item.SourceColumn.Name} ne '", notEqualConditions)}";
                            }
                            else
                            {
                                condition = $"{string.Join($" and {item.SourceColumn.Name} ne ", notEqualConditions)}";
                            }
                            break;
                        case ConditionalOperator.StartsWith:
                            if (item.SourceColumn.Type == typeof(string))
                            {
                                result.Add($"startswith({item.SourceColumn.Name},'{item.Condition}')");
                            }
                            else
                            {
                                LogWarningForConditional(item);
                            }
                            continue;
                        case ConditionalOperator.NotStartsWith:
                            if (item.SourceColumn.Type == typeof(string))
                            {
                                result.Add($"startswith({item.SourceColumn.Name},'{item.Condition}') ne true");
                            }
                            else
                            {
                                LogWarningForConditional(item);
                            }
                            continue;
                        case ConditionalOperator.EndsWith:
                            if (item.SourceColumn.Type == typeof(string))
                            {
                                result.Add($"endswith({item.SourceColumn.Name},'{item.Condition}')");
                            }
                            else
                            {
                                LogWarningForConditional(item);
                            }
                            continue;
                        case ConditionalOperator.NotEndsWith:
                            if (item.SourceColumn.Type == typeof(string))
                            {
                                result.Add($"endswith({item.SourceColumn.Name},'{item.Condition}') ne true");
                            }
                            else
                            {
                                LogWarningForConditional(item);
                            }
                            continue;

                    }
                    var conditionToAdd = $"{condition}";
                    if (item.SourceColumn.Type == typeof(string))
                    {
                        conditionToAdd = $"'{condition}'";
                    }
                    else if (item.SourceColumn.Type == typeof(bool))
                    {
                        conditionToAdd = $"{condition.ToLower()}";
                    }
                    result.Add($"({item.SourceColumn.Name} {operatorInOData} {conditionToAdd})");
                }
                else
                {
                    _logger?.Warn($"The condition for the table mapping {_mapping.SourceTable.Name} to {_mapping.DestinationTable.Name} on source column {item.SourceColumn.Name} is null, so this have been removed from the $filter.");
                }
            }
        }
        return result;
    }

    private void LogWarningForConditional(MappingConditional item)
    {
        _logger?.Warn($"Can only add {item.ConditionalOperator} on Edm.String and the {item.SourceColumn.Name} is a type of {item.SourceColumn.Type.Name} for the table mapping {_mapping.SourceTable.Name} to {_mapping.DestinationTable.Name}, so this have been removed from the $filter.");
    }

    private List<string> GetExpandAsParameters(Mapping mapping)
    {
        List<string> result = new();
        var sourceColumnsWithGroups = mapping.SourceTable.Columns.Where(obj => !string.IsNullOrEmpty(obj.Group));
        if (sourceColumnsWithGroups.Any())
        {
            result.AddRange(sourceColumnsWithGroups.DistinctBy(obj => obj.Group).Select(obj => obj.Name));
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
        var something = JsonDocument.Parse(responseStream);
        foreach (var item in something.RootElement.EnumerateObject())
        {
            if (item.Name.StartsWith("value", StringComparison.OrdinalIgnoreCase))
            {
                List<Dictionary<string, JsonElement>> deserializedJsons = JsonSerializer.Deserialize<List<Dictionary<string, JsonElement>>>(item.Value.GetRawText());
                foreach (var deserializedJson in deserializedJsons)
                {
                    var deserializedDictionary = deserializedJson.ToDictionary(obj => obj.Key, obj => obj.Value.ValueKind == JsonValueKind.String ? (object)obj.Value.GetString() : (object)obj.Value.GetRawText());
                    if (!_hasFormattedValues.HasValue)
                    {
                        _hasFormattedValues = deserializedDictionary.Any(kvp => kvp.Key.Contains($"@{_odataFormattedValue}", StringComparison.OrdinalIgnoreCase));
                    }
                    if (_hasFormattedValues.Value)
                    {
                        var columnMappings = _mapping.SourceTable.Columns;
                        var listOfBooleans = columnMappings.Where(obj => obj.Type == typeof(bool));
                        var keyValuePairsWithFormattedValues = deserializedDictionary.Where(obj => obj.Key.Contains($"@{_odataFormattedValue}", StringComparison.OrdinalIgnoreCase)).ToList();
                        foreach (var kvpwf in keyValuePairsWithFormattedValues)
                        {
                            string key = kvpwf.Key.Replace($"@{_odataFormattedValue}", "");
                            if (!listOfBooleans.Any(obj => obj.Name == key))
                            {
                                deserializedDictionary[key] = kvpwf.Value;
                            }
                        }
                    }
                    if (!_doNotStoreLastResponseInLogFile)
                    {
                        _totalResponseResult.Add(deserializedDictionary);
                    }
                    yield return deserializedDictionary;
                }
            }

            // If this is a pagination link store it for later pagination
            else if (item.Name.Contains(_nextPaginationUrlName, StringComparison.OrdinalIgnoreCase))
            {
                _paginationUrl = JsonSerializer.Deserialize<string>(item.Value.GetRawText());
            }
        }
        yield return null;
    }

    private bool HandleRequest(string url, string loggerInfo, IDictionary<string, string> headers, int retryCounter = 0)
    {
        if (CheckIfEndpointIsReadyForUse(url))
        {
            _logger?.Info(loggerInfo);
            Task task;
            var endpointAuthentication = _endpoint.Authentication;
            if (endpointAuthentication.IsTokenBased())
            {
                string token = GetToken(_endpoint, endpointAuthentication);
                task = RetryHelper.RetryOnExceptionAsync<Exception>(10, async () => { _httpRestClient.GetAsync(url, HandleStream, token, (Dictionary<string, string>)headers).Wait(new CancellationTokenSource(_timeoutInMilliseconds).Token); }, _logger);
            }
            else
            {
                task = RetryHelper.RetryOnExceptionAsync<Exception>(10, async () => { _httpRestClient.GetAsync(url, HandleStream, endpointAuthentication, (Dictionary<string, string>)headers).Wait(new CancellationTokenSource(_timeoutInMilliseconds).Token); }, _logger);
            }
            if (task.IsCanceled)
            {
                string aditionalErrorMSG = _requestTimedOutFromGlobalSettings ? "(To change go to global settings and look for TimeoutInMilliseconds)" : "";
                throw new TimeoutException($"Request has timed out with a wait of {_timeoutInMilliseconds} in milliseconds {aditionalErrorMSG}");
            }
            task.Wait();
            _logger?.Info("Data received, now processing data.");
            return true;
        }
        else
        {
            _logger?.Info($"Endpoint: '{_endpoint.Name}' is not ready for use on URL: '{url}'");
            if (retryCounter < 2)
            {
                retryCounter++;
                _logger?.Info($"Will wait and retry again in 5 seconds.");
                Thread.Sleep(5000);
                _logger?.Info($"This is retry {retryCounter} out of 2");
                HandleRequest(url, loggerInfo, headers, retryCounter);
            }

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
            if (string.IsNullOrWhiteSpace(_paginationUrl) || (_mode != null && _mode.Equals("First page", StringComparison.OrdinalIgnoreCase)))
            {
                FinishJob();
                return true;
            }
            if (HandleRequest(_paginationUrl, $"Paginating request to endpoint: '{_endpoint.Name}', using URL: '{_paginationUrl}'", GetAllHeaders()))
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

    internal static string GetEndpointUrlWithTop(string url)
    {
        if (url.Contains('?'))
        {
            bool urlContainsTop = url.Contains("$top=", StringComparison.OrdinalIgnoreCase);
            if (new Uri(url).Query.Any() && !urlContainsTop)
            {
                return $"{url}&$top=1";
            }
            else if (!urlContainsTop)
            {
                return $"{url}$top=1";
            }
            return url;
        }
        else
        {
            return $"{url}?$top=1";
        }
    }

    private bool CheckIfEndpointIsReadyForUse(string url)
    {
        string checkUrl = GetEndpointUrlWithTop(url);
        _logger?.Info($"Checking if endpoint: '{_endpoint.Name}' is ready for use on URL: '{checkUrl}'");
        bool result = false;
        Task task;
        var endpointAuthentication = _endpoint.Authentication;
        if (endpointAuthentication.IsTokenBased())
        {
            string token = GetToken(_endpoint, endpointAuthentication);
            task = RetryHelper.RetryOnExceptionAsync<Exception>(10, async () => { _httpRestClient.GetAsync(checkUrl, HandleResponse, token, (Dictionary<string, string>)GetAllHeaders()).Wait(new CancellationTokenSource(_timeoutInMilliseconds).Token); }, _logger);
        }
        else
        {
            task = RetryHelper.RetryOnExceptionAsync<Exception>(10, async () => { _httpRestClient.GetAsync(checkUrl, HandleResponse, endpointAuthentication, (Dictionary<string, string>)GetAllHeaders()).Wait(new CancellationTokenSource(_timeoutInMilliseconds).Token); }, _logger);
        }
        if (task.IsCanceled)
        {
            string aditionalErrorMSG = _requestTimedOutFromGlobalSettings ? "(To change go to global settings and look for TimeoutInMilliseconds)" : "";
            throw new TimeoutException($"Request has timed out with a wait of {_timeoutInMilliseconds} in milliseconds {aditionalErrorMSG}");
        }
        task.Wait();

        void HandleResponse(Stream responseStream, HttpStatusCode responseStatusCode, Dictionary<string, string> responseHeaders)
        {
            if (responseStatusCode == HttpStatusCode.OK)
            {
                result = true;
            }
            else
            {
                using var stream = new StreamReader(responseStream);
                var streamResponse = stream.ReadToEnd();

                if (_failJobOnEndpointIsBusy)
                {
                    throw new WebException($"{checkUrl} returned: {streamResponse} with the HttpStatusCode of: '{responseStatusCode}' ");
                }
                else
                {
                    _logger?.Info($"{checkUrl} returned: {streamResponse} with the HttpStatusCode of: '{responseStatusCode}' ");
                }
            }
        }
        return result;
    }

    private string GetToken(Endpoint endpoint, EndpointAuthentication endpointAuthentication)
    {
        string token = OAuthHelper.GetToken(endpoint, endpointAuthentication, out Exception exception);
        if (exception != null)
        {
            throw exception;
        }
        return token;
    }

    private int GetTimeOutInMilliseconds()
    {
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
        return timeoutInMilliseconds;
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
