using Dynamicweb.Core;
using Dynamicweb.DataIntegration.EndpointManagement;
using Dynamicweb.DataIntegration.Integration;
using Dynamicweb.DataIntegration.Integration.Interfaces;
using Dynamicweb.DataIntegration.Providers.ODataProvider.Model;
using Dynamicweb.Ecommerce;
using Dynamicweb.Ecommerce.Orders;
using Dynamicweb.Logging;
using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlTypes;
using System.Globalization;
using System.Linq;
using System.Net;
using System.Text.Json.Nodes;
using System.Threading.Tasks;

namespace Dynamicweb.DataIntegration.Providers.ODataProvider;

internal class ODataWriter : IDisposable, IDestinationWriter
{
    public readonly int RequestTimeout = 20;
    public readonly Endpoint Endpoint;
    private readonly ILogger Logger;
    public readonly ICredentials Credentials;
    public readonly EndpointAuthenticationService EndpointAuthenticationService;
    private Dictionary<string, Type> _destinationPrimaryKeyColumns;
    private readonly ColumnMappingCollection _responseMappings;
    private bool _continueOnError;
    public Mapping Mapping { get; }
    internal JsonObject PostBackObject { get; set; }
    private readonly ColumnMappingCollection _columnMappings;

    internal ODataWriter(ILogger logger, Mapping mapping, Endpoint endpoint, ICredentials credentials, bool continueOnError)
    {
        Logger = logger;
        Endpoint = endpoint;
        Credentials = credentials;
        Mapping = mapping;
        EndpointAuthenticationService = new EndpointAuthenticationService();
        var originalDestinationTables = Mapping.Destination.GetOriginalDestinationSchema().GetTables();
        var originalDestinationMappingTable = originalDestinationTables.FirstOrDefault(obj => obj.Name == Mapping.DestinationTable.Name);
        _destinationPrimaryKeyColumns = originalDestinationMappingTable?.Columns.Where(obj => obj.IsPrimaryKey)?.ToDictionary(obj => obj.Name, obj => obj.Type) ?? new Dictionary<string, Type>();
        _responseMappings = Mapping.GetResponseColumnMappings();
        _continueOnError = continueOnError;
        _columnMappings = Mapping.GetColumnMappings();
    }

    public void Write(Dictionary<string, object> Row)
    {
        string endpointURL = Endpoint.Url;
        string url = ODataSourceReader.GetEndpointURL(endpointURL, Mapping.DestinationTable.Name, "");

        var keyColumnValuesForFilter = GetKeyColumnValuesForFilter(Row);

        Task<RestResponse<JsonObject>> awaitResponseFromEndpoint;
        if (keyColumnValuesForFilter.Any())
        {
            string filter = string.Join(" and ", keyColumnValuesForFilter);
            IDictionary<string, string> parameters = new Dictionary<string, string>() { { "$filter", filter } };
            if (Endpoint.Parameters != null)
            {
                foreach (var item in Endpoint.Parameters)
                {
                    if (!parameters.ContainsKey(item.Key))
                    {
                        parameters.Add(item.Key, item.Value);
                    }
                }
            }
            url = ODataSourceReader.GetEndpointURL(endpointURL, Mapping.DestinationTable.Name, "", parameters);

            var responseFromEndpoint = GetFromEndpoint<JsonObject>(url, null);
            if (!string.IsNullOrEmpty(responseFromEndpoint?.Result?.Error))
            {
                LogError(Row, url, responseFromEndpoint.Result);
                if (!_continueOnError)
                {
                    throw new Exception(responseFromEndpoint.Result.Error);
                }
            }

            var response = responseFromEndpoint?.Result?.Content?.Value;

            url = ODataSourceReader.GetEndpointURL(endpointURL, Mapping.DestinationTable.Name, "");
            if (response != null && response.Count > 0)
            {
                if (response.Count > 1)
                {
                    throw new Exception("The filter returned too many records, please update or change filter.");
                }

                var jsonObject = response[0];
                Logger?.Info($"Received response from Endpoint = {jsonObject.ToJsonString()}");

                var patchJson = MapValuesToJson(Row, true);
                if (patchJson.Equals(new JsonObject().ToString()))
                {
                    Logger?.Info($"Skipped PATCH as no active column mappings is added for always apply.");
                    return;
                }

                Dictionary<string, string> headers = new Dictionary<string, string>() { { "Content-Type", "application/json; charset=utf-8" } };

                List<string> primaryKeyColumnValuesForPatch = new List<string>();
                foreach (var item in jsonObject)
                {
                    if (item.Key.Equals("@odata.etag", StringComparison.OrdinalIgnoreCase))
                    {
                        headers.Add("If-Match", item.Value.ToString());
                    }
                    else if (_destinationPrimaryKeyColumns.TryGetValue(item.Key, out Type columnKeyType))
                    {
                        if (columnKeyType == typeof(string))
                        {
                            primaryKeyColumnValuesForPatch.Add($"{item.Key}='{item.Value}'");
                        }
                        else if (columnKeyType == typeof(DateTime))
                        {
                            primaryKeyColumnValuesForPatch.Add($"{item.Key}={GetTheDateTimeInZeroTimeZone(item.Value, false)}");
                        }
                        else if (columnKeyType == typeof(DateOnly))
                        {
                            primaryKeyColumnValuesForPatch.Add($"{item.Key}={GetTheDateTimeInZeroTimeZone(item.Value, true)}");
                        }
                        else
                        {
                            primaryKeyColumnValuesForPatch.Add($"{item.Key}={item.Value}");
                        }
                    }
                }
                if (primaryKeyColumnValuesForPatch.Any())
                {
                    string patchURL = "(" + string.Join(",", primaryKeyColumnValuesForPatch) + ")";
                    url = ODataSourceReader.GetEndpointURL(endpointURL, Mapping.DestinationTable.Name, patchURL);
                }
                awaitResponseFromEndpoint = PostToEndpoint<JsonObject>(url, patchJson, headers, true);
            }
            else
            {
                awaitResponseFromEndpoint = PostToEndpoint<JsonObject>(url, MapValuesToJson(Row, false), null, false);
            }
        }
        else
        {
            awaitResponseFromEndpoint = PostToEndpoint<JsonObject>(url, MapValuesToJson(Row, false), null, false);
        }
        awaitResponseFromEndpoint.Wait();
        if (!string.IsNullOrEmpty(awaitResponseFromEndpoint?.Result?.Error))
        {
            Logger?.Error($"Error Url: {url}. Response Error: {awaitResponseFromEndpoint.Result.Error}. Status response code: {awaitResponseFromEndpoint.Result.Status}");
            if (!_continueOnError)
            {
                throw new Exception(awaitResponseFromEndpoint.Result.Error);
            }
        }

        PostBackObject = awaitResponseFromEndpoint?.Result?.Content;

        if (awaitResponseFromEndpoint?.Result?.Status != HttpStatusCode.NoContent)
        {
            Logger?.Info($"Received response from Endpoint = {PostBackObject?.ToJsonString()}");
        }
        else if (_responseMappings.Any())
        {
            Logger?.Info($"Endpoint returned no content so can not do response mappings on this record.");
        }
        else
        {
            Logger?.Info($"Received no response from Endpoint");
        }
    }

    private void LogError(Dictionary<string, object> row, string url, RestResponse<ResponseFromEndpoint<JsonObject>> response)
    {
        Logger?.Error($"Error Url: {url}. Response Error: {response.Error}. Status response code: {response.Status}");

        if (row is not null && Mapping?.SourceTable is not null && string.Equals(Mapping.SourceTable.Name, "EcomOrders", StringComparison.OrdinalIgnoreCase))
        {
            var key = row.Keys.FirstOrDefault(k => string.Equals(k, "OrderId", StringComparison.OrdinalIgnoreCase));
            if (!string.IsNullOrEmpty(key) && row[key] is not null)
            {
                string id = row[key].ToString();
                if (!string.IsNullOrEmpty(id))
                {
                    var order = Services.Orders.GetById(id);
                    if (order is not null)
                    {
                        Services.OrderDebuggingInfos.Save(order, $"{nameof(ODataProvider)}: Communication failed with error: {response.Error}", nameof(ODataProvider), DebuggingInfoType.Undefined);
                    }
                }
            }
        }
    }

    internal object GetPostBackValue(ColumnMapping columnMapping)
    {
        string result = null;
        try
        {
            foreach (var item in PostBackObject)
            {
                if (item.Key.Equals(columnMapping?.SourceColumn?.Name, StringComparison.OrdinalIgnoreCase))
                {
                    return columnMapping.ConvertInputValueToOutputValue(item.Value.ToString());
                }
            }
        }
        catch (Exception ex)
        {
            Logger?.Error($"Error GetPostBackValue", ex);
        }
        return result;
    }

    internal Task<RestResponse<T>> PostToEndpoint<T>(string URL, string jsonObject, Dictionary<string, string> header, bool patch)
    {
        var _client = new HttpRestClient(Credentials, RequestTimeout, Logger);
        EndpointAuthentication endpointAuthentication = Endpoint.Authentication;
        Task<RestResponse<T>> awaitResponseFromEndpoint;
        if (endpointAuthentication.IsTokenBased())
        {
            string token = OAuthHelper.GetToken(Endpoint, endpointAuthentication);
            if (!patch)
            {
                awaitResponseFromEndpoint = _client.PostAsync<string, T>(URL, jsonObject, token, header);
            }
            else
            {
                awaitResponseFromEndpoint = _client.PatchAsync<string, T>(URL, jsonObject, token, header);
            }
        }
        else
        {
            if (!patch)
            {
                awaitResponseFromEndpoint = _client.PostAsync<string, T>(URL, jsonObject, endpointAuthentication, header);
            }
            else
            {
                awaitResponseFromEndpoint = _client.PatchAsync<string, T>(URL, jsonObject, endpointAuthentication, header);
            }
        }
        return awaitResponseFromEndpoint;
    }

    internal Task<RestResponse<ResponseFromEndpoint<T>>> GetFromEndpoint<T>(string URL, Dictionary<string, string> header)
    {
        var _client = new HttpRestClient(Credentials, RequestTimeout, Logger);
        EndpointAuthentication endpointAuthentication = Endpoint.Authentication;
        Task<RestResponse<ResponseFromEndpoint<T>>> awaitResponseFromEndpoint;
        if (endpointAuthentication.IsTokenBased())
        {
            string token = OAuthHelper.GetToken(Endpoint, endpointAuthentication);
            awaitResponseFromEndpoint = _client.GetAsync<ResponseFromEndpoint<T>>(URL, token, header);
        }
        else
        {
            awaitResponseFromEndpoint = _client.GetAsync<ResponseFromEndpoint<T>>(URL, endpointAuthentication, header);
        }
        awaitResponseFromEndpoint.Wait();

        return awaitResponseFromEndpoint;
    }

    internal List<string> GetKeyColumnValuesForFilter(Dictionary<string, object> row)
    {
        var keyColumnValues = new List<string>();
        foreach (var keyMapping in _columnMappings.Where(cm => cm != null && cm.IsKey))
        {
            var keyMappingValue = keyMapping.HasScriptWithValue ? null : row.TryGetValue(keyMapping.SourceColumn?.Name ?? "", out var value) ? value : null;
            if (keyMapping.DestinationColumn.Type == typeof(string))
            {
                keyColumnValues.Add($"{keyMapping.DestinationColumn.Name} eq '{keyMapping.ConvertInputValueToOutputValue(keyMappingValue)}'");
            }
            else if (keyMapping.DestinationColumn.Type == typeof(DateTime))
            {
                keyColumnValues.Add($"{keyMapping.DestinationColumn.Name} eq {GetTheDateTimeInZeroTimeZone(keyMapping.ConvertInputValueToOutputValue(keyMappingValue), false)}");
            }
            else if (keyMapping.DestinationColumn.Type == typeof(DateOnly))
            {
                keyColumnValues.Add($"{keyMapping.DestinationColumn.Name} eq {GetTheDateTimeInZeroTimeZone(keyMapping.ConvertInputValueToOutputValue(keyMappingValue), true)}");
            }
            else
            {
                keyColumnValues.Add($"{keyMapping.DestinationColumn.Name} eq {keyMapping.ConvertInputValueToOutputValue(keyMappingValue)}");
            }
        }
        return keyColumnValues;
    }

    internal string MapValuesToJson(Dictionary<string, object> row, bool isPatchRequest)
    {
        var jsonObject = new JsonObject();

        foreach (ColumnMapping columnMapping in _columnMappings)
        {
            if (!columnMapping.Active || (columnMapping.ScriptValueForInsert && isPatchRequest))
                continue;

            if (columnMapping.HasScriptWithValue || row.ContainsKey(columnMapping.SourceColumn?.Name))
            {
                var columnValue = columnMapping.ConvertInputValueToOutputValue(columnMapping.HasScriptWithValue ? null : row.TryGetValue(columnMapping.SourceColumn?.Name ?? "", out var value) ? value : null);

                switch (columnMapping.DestinationColumn.Type.Name.ToLower())
                {
                    case "decimal":
                        jsonObject.Add(columnMapping.DestinationColumn.Name, Converter.ToDecimal(columnValue));
                        break;
                    case "int":
                        jsonObject.Add(columnMapping.DestinationColumn.Name, Converter.ToInt64(columnValue));
                        break;
                    case "double":
                        jsonObject.Add(columnMapping.DestinationColumn.Name, Converter.ToDecimal(columnValue));
                        break;
                    case "datetime":
                        jsonObject.Add(columnMapping.DestinationColumn.Name, GetTheDateTimeInZeroTimeZone(columnValue, false));
                        break;
                    case "dateonly":
                        jsonObject.Add(columnMapping.DestinationColumn.Name, GetTheDateTimeInZeroTimeZone(columnValue, true));
                        break;
                    default:
                        jsonObject.Add(columnMapping.DestinationColumn.Name, Converter.ToString(columnValue));
                        break;
                }
            }
        }
        return jsonObject.ToJsonString();
    }

    public static string GetTheDateTimeInZeroTimeZone(object dateTimeObject, bool isEdmDate)
    {
        var inputString = Convert.ToString(dateTimeObject, CultureInfo.InvariantCulture);
        if (DateTime.TryParse(inputString, CultureInfo.InvariantCulture, out var dateTime) ||
            DateTime.TryParseExact(inputString, "dd-MM-yyyy HH:mm:ss:fff", CultureInfo.InvariantCulture, DateTimeStyles.None, out dateTime))
        {
            if (dateTime <= SqlDateTime.MinValue.Value || dateTime <= DateTime.MinValue)
            {
                return null;
            }
            else if (dateTime >= SqlDateTime.MaxValue.Value || dateTime >= DateTime.MaxValue)
            {
                return null;
            }

            if (!isEdmDate)
            {
                var dateTimeInUtc = TimeZoneInfo.ConvertTimeToUtc(dateTime);
                return dateTimeInUtc.ToString("yyyy-MM-ddTHH:mm:ss.fff", CultureInfo.InvariantCulture) + "z";
            }
            else
            {
                return dateTime.ToString("yyyy-MM-dd", CultureInfo.InvariantCulture);
            }
        }
        return null;
    }

    public void Dispose() { }

    public void Close() { }
}
