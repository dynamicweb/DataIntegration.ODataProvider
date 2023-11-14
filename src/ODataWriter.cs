using Dynamicweb.Core;
using Dynamicweb.DataIntegration.EndpointManagement;
using Dynamicweb.DataIntegration.Integration;
using Dynamicweb.DataIntegration.Integration.Interfaces;
using Dynamicweb.DataIntegration.Providers.ODataProvider.Model;
using Dynamicweb.Logging;
using System;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using System.Net;
using System.Threading.Tasks;
using System.Text.Json.Nodes;

namespace Dynamicweb.DataIntegration.Providers.ODataProvider
{
    internal class ODataWriter : IDisposable, IDestinationWriter
    {
        public readonly int RequestTimeout = 20;
        public readonly Endpoint Endpoint;
        private readonly ILogger Logger;
        public readonly ICredentials Credentials;
        public readonly EndpointAuthenticationService EndpointAuthenticationService;
        private Dictionary<string, Type> _destinationPrimaryKeyColumns;
        private readonly ColumnMappingCollection _responseMappings;
        public Mapping Mapping { get; }
        internal JsonObject PostBackObject { get; set; }

        internal ODataWriter(ILogger logger, Mapping mapping, Endpoint endpoint, ICredentials credentials)
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
        }

        public void Write(Dictionary<string, object> Row)
        {
            string endpointURL = Endpoint.Url;
            string url = ODataSourceReader.GetEndpointURL(endpointURL, Mapping.DestinationTable.Name, "");

            var columnMappings = Mapping.GetColumnMappings();
            var keyColumnValuesForFilter = GetKeyColumnValuesForFilter(Row, columnMappings);

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
                    if (responseFromEndpoint.Result.Status == HttpStatusCode.Unauthorized)
                    {
                        throw new Exception(responseFromEndpoint.Result.Error);
                    }
                    Logger?.Warn($"Error Url: {url}. Response Error: {responseFromEndpoint.Result.Error}. Status response code: {responseFromEndpoint.Result.Status}");
                    return;
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
                    Logger?.Info($"Recieved response from Endpoint = {jsonObject.ToJsonString()}");

                    var patchJson = MapValuesToJSon(columnMappings, Row, true);
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
                    awaitResponseFromEndpoint = PostToEndpoint<JsonObject>(url, MapValuesToJSon(columnMappings, Row, false), null, false);
                }
            }
            else
            {
                awaitResponseFromEndpoint = PostToEndpoint<JsonObject>(url, MapValuesToJSon(columnMappings, Row, false), null, false);
            }
            awaitResponseFromEndpoint.Wait();
            if (!string.IsNullOrEmpty(awaitResponseFromEndpoint?.Result?.Error))
            {
                Logger?.Warn($"Error Url: {url}. Response Error: {awaitResponseFromEndpoint.Result.Error}. Status response code: {awaitResponseFromEndpoint.Result.Status}");
            }

            PostBackObject = awaitResponseFromEndpoint?.Result?.Content;

            if (awaitResponseFromEndpoint?.Result?.Status != HttpStatusCode.NoContent)
            {
                Logger?.Info($"Recieved response from Endpoint = {PostBackObject?.ToJsonString()}");
            }
            else if (_responseMappings.Any())
            {
                Logger?.Info($"Endpoint returned no content so can not do response mappings on this record.");
            }
            else
            {
                Logger?.Info($"Recieved no response from Endpoint");
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
            catch
            {
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

        internal List<string> GetKeyColumnValuesForFilter(Dictionary<string, object> row, ColumnMappingCollection columnMappings)
        {
            var keyColumnValues = new List<string>();
            foreach (var keyMapping in columnMappings.Where(cm => cm != null && cm.IsKey))
            {
                if (keyMapping.DestinationColumn.Type == typeof(string))
                {
                    keyColumnValues.Add($"{keyMapping.DestinationColumn.Name} eq '{keyMapping.ConvertInputValueToOutputValue(row[keyMapping.SourceColumn?.Name] ?? null)}'");
                }
                else if (keyMapping.DestinationColumn.Type == typeof(DateTime))
                {
                    keyColumnValues.Add($"{keyMapping.DestinationColumn.Name} eq {keyMapping.ConvertInputValueToOutputValue(GetTheDateTimeInZeroTimeZone(row[keyMapping.SourceColumn?.Name], false))}");
                }
                else
                {
                    keyColumnValues.Add($"{keyMapping.DestinationColumn.Name} eq {keyMapping.ConvertInputValueToOutputValue(row[keyMapping.SourceColumn?.Name] ?? null)}");
                }
            }
            return keyColumnValues;
        }

        internal string MapValuesToJSon(ColumnMappingCollection columnMappings, Dictionary<string, object> row, bool isPatchRequest)
        {
            var jsonObject = new JsonObject();

            foreach (ColumnMapping columnMapping in columnMappings)
            {
                if (!columnMapping.Active || (columnMapping.ScriptValueForInsert && isPatchRequest))
                    continue;

                if (columnMapping.HasScriptWithValue || row.ContainsKey(columnMapping.SourceColumn?.Name))
                {
                    var columnValue = columnMapping.ConvertInputValueToOutputValue(row[columnMapping.SourceColumn?.Name] ?? null);

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
                        default:
                            jsonObject.Add(columnMapping.DestinationColumn.Name, Converter.ToString(columnValue));
                            break;
                    }
                }
            }
            return jsonObject.ToString();
        }

        public static string GetTheDateTimeInZeroTimeZone(object dateTimeObject, bool isEdmDate)
        {
            var dateTime = Converter.ToDateTime(dateTimeObject);
            DateTime dateTimeInUtc = TimeZoneInfo.ConvertTimeToUtc(dateTime);
            if (dateTimeInUtc.TimeOfDay.TotalMilliseconds > 0 && !isEdmDate)
            {
                return dateTimeInUtc.ToString("yyyy-MM-ddTHH:mm:ss.fff", CultureInfo.InvariantCulture) + "z";
            }
            else
            {
                return dateTimeInUtc.ToString("yyyy-MM-dd", CultureInfo.InvariantCulture) + "z";
            }
        }

        public void Dispose() { }

        public void Close() { }
    }
}
