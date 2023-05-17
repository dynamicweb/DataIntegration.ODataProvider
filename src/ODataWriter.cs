﻿using Dynamicweb.Core;
using Dynamicweb.DataIntegration.EndpointManagement;
using Dynamicweb.DataIntegration.Integration;
using Dynamicweb.DataIntegration.Integration.Interfaces;
using Dynamicweb.DataIntegration.Providers.ODataProvider.Model;
using Dynamicweb.Logging;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Text.Json.Nodes;
using System.Threading.Tasks;

namespace Dynamicweb.DataIntegration.Providers.ODataProvider
{
    internal class ODataWriter : IDisposable, IDestinationWriter
    {
        public readonly int RequestTimeout = 20;
        public readonly Endpoint Endpoint;
        private readonly ILogger Logger;
        public readonly ICredentials Credentials;
        private Dictionary<string, Type> _destinationPrimaryKeyColumns;
        public Mapping Mapping { get; }

        internal ODataWriter(ILogger logger, Mapping mapping, Endpoint endpoint, ICredentials credentials)
        {
            Logger = logger;
            Endpoint = endpoint;
            Credentials = credentials;
            Mapping = mapping;
            var originalDestinationTables = Mapping.Destination.GetOriginalDestinationSchema().GetTables();
            var originalDestinationMappingTable = originalDestinationTables.FirstOrDefault(obj => obj.Name == Mapping.DestinationTable.Name);
            _destinationPrimaryKeyColumns = originalDestinationMappingTable?.Columns.Where(obj => obj.IsPrimaryKey)?.ToDictionary(obj => obj.Name, obj => obj.Type) ?? new Dictionary<string, Type>();
        }

        public void Write(Dictionary<string, object> Row)
        {
            if (!Mapping.Conditionals.CheckConditionals(Row))
            {
                return;
            }

            string endpointURL = Endpoint.Url;
            string url = ODataSourceReader.GetEndpointURL(endpointURL, Mapping.DestinationTable.Name, "");

            var columnMappings = Mapping.GetColumnMappings();
            var keyColumnValuesForFilter = GetKeyColumnValuesForFilter(Row, columnMappings);
            string request = MapValuesToJSon(columnMappings, Row);

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
            }

            var response = GetFromEndpoint<JsonObject>(url, null);
            url = ODataSourceReader.GetEndpointURL(endpointURL, Mapping.DestinationTable.Name, "");
            Task<RestResponse<string>> awaitResponseFromERP;
            if (response != null && response.Count > 0)
            {
                if (response.Count > 1)
                {
                    Logger?.Error($"The filter returned too many records, please update or change filter.");
                    return;
                }
                var jObject = response[0];
                Logger?.Info($"Recieved response from ERP = {jObject.ToJsonString()}");
                Dictionary<string, string> headers = new Dictionary<string, string>() { { "Content-Type", "application/json; charset=utf-8" } };

                List<string> primaryKeyColumnValuesForPatch = new List<string>();
                foreach (var item in jObject)
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
                awaitResponseFromERP = PostToEndpoint<string>(url, request, headers, true);
            }
            else
            {
                awaitResponseFromERP = PostToEndpoint<string>(url, request, null, false);
            }
            awaitResponseFromERP.Wait();
            if (!string.IsNullOrEmpty(awaitResponseFromERP.Result.Error))
            {
                Logger.Warn($"Error Url: {url}. Response Error: {awaitResponseFromERP.Result.Error}");
            }
        }

        internal Task<RestResponse<T>> PostToEndpoint<T>(string URL, string jsonObject, Dictionary<string, string> header, bool patch)
        {
            var _client = new HttpRestClient(Credentials, RequestTimeout, Logger);
            var endpointAuthentication = Endpoint.Authentication;
            Task<RestResponse<T>> awaitResponseFromBC;
            if (endpointAuthentication.IsTokenBased())
            {
                string token = OAuthHelper.GetToken(Endpoint, endpointAuthentication);
                if (!patch)
                {
                    awaitResponseFromBC = _client.PostAsync<string, T>(URL, jsonObject, token, header);
                }
                else
                {
                    awaitResponseFromBC = _client.PatchAsync<string, T>(URL, jsonObject, token, header);
                }
            }
            else
            {
                if (!patch)
                {
                    awaitResponseFromBC = _client.PostAsync<string, T>(URL, jsonObject, endpointAuthentication, header);
                }
                else
                {
                    awaitResponseFromBC = _client.PatchAsync<string, T>(URL, jsonObject, endpointAuthentication, header);
                }
            }
            return awaitResponseFromBC;
        }

        internal List<T> GetFromEndpoint<T>(string URL, Dictionary<string, string> header)
        {
            var _client = new HttpRestClient(Credentials, RequestTimeout, Logger);
            var endpointAuthentication = Endpoint.Authentication;
            Task<RestResponse<ResponseFromERP<T>>> awaitResponseFromBC;
            if (endpointAuthentication.IsTokenBased())
            {
                string token = OAuthHelper.GetToken(Endpoint, endpointAuthentication);
                awaitResponseFromBC = _client.GetAsync<ResponseFromERP<T>>(URL, token, header);
            }
            else
            {
                awaitResponseFromBC = _client.GetAsync<ResponseFromERP<T>>(URL, endpointAuthentication, header);
            }
            awaitResponseFromBC.Wait();
            return awaitResponseFromBC.Result.Content.Value;
        }

        public static string HandleScriptTypeForColumnMapping(ColumnMapping columnMapping, object columnValue)
        {
            string result = Converter.ToString(columnValue);
            switch (columnMapping.ScriptType)
            {
                case ScriptType.Append:
                    result = columnMapping.ConvertInputToOutputFormat(columnValue).ToString() + columnMapping.ScriptValue;
                    break;
                case ScriptType.Prepend:
                    result = columnMapping.ScriptValue + columnMapping.ConvertInputToOutputFormat(columnValue).ToString();
                    break;
                case ScriptType.Constant:
                    result = columnMapping.ScriptValue;
                    break;
                case ScriptType.NewGuid:
                    result = columnMapping.GetScriptValue();
                    break;
            }
            return result;
        }

        internal List<string> GetKeyColumnValuesForFilter(Dictionary<string, object> row, ColumnMappingCollection columnMappings)
        {
            var keyColumnValues = new List<string>();
            foreach (var keyMapping in columnMappings.Where(cm => cm != null && cm.IsKey))
            {
                if (keyMapping.DestinationColumn.Type == typeof(string))
                {
                    keyColumnValues.Add($"{keyMapping.DestinationColumn.Name} eq '{HandleScriptTypeForColumnMapping(keyMapping, row[keyMapping.SourceColumn.Name])}'");
                }
                else
                {
                    keyColumnValues.Add($"{keyMapping.DestinationColumn.Name} eq {HandleScriptTypeForColumnMapping(keyMapping, row[keyMapping.SourceColumn.Name])}");
                }
            }
            return keyColumnValues;
        }

        internal string MapValuesToJSon(ColumnMappingCollection columnMappings, Dictionary<string, object> row)
        {
            var jsonObject = new JsonObject();

            foreach (ColumnMapping columnMapping in columnMappings)
            {
                if (!columnMapping.Active)
                    continue;

                if (columnMapping.SourceColumn != null)
                {
                    if (columnMapping.HasScriptWithValue || row.ContainsKey(columnMapping.SourceColumn.Name))
                    {
                        var columnValue = HandleScriptTypeForColumnMapping(columnMapping, row[columnMapping.SourceColumn.Name]);

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
                            default:
                                jsonObject.Add(columnMapping.DestinationColumn.Name, Converter.ToString(columnValue));
                                break;
                        }
                    }
                }
            }
            return jsonObject.ToString();
        }

        public void Dispose() { }

        public void Close() { }
    }
}
