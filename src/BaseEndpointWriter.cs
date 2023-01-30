using Dynamicweb.Core;
using Dynamicweb.DataIntegration.EndpointManagement;
using Dynamicweb.DataIntegration.Integration;
using Dynamicweb.DataIntegration.Integration.Interfaces;
using Dynamicweb.DataIntegration.Providers.ODataProvider.Model;
using Dynamicweb.Logging;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Threading.Tasks;

namespace Dynamicweb.DataIntegration.Providers.ODataProvider
{
    internal class BaseEndpointWriter : BaseProvider
    {
        public readonly int RequestTimeout = 20;
        public readonly Endpoint Endpoint;
        public readonly ICredentials Credentials;
        public readonly EndpointAuthenticationService EndpointAuthenticationService;
        private AuthenticationHelper AuthenticationHelper = new AuthenticationHelper();
        private Dictionary<string, Type> _destinationPrimaryKeyColumns;
        public Mapping Mapping { get; }

        internal BaseEndpointWriter(ILogger logger, Mapping mapping, Endpoint endpoint, ICredentials credentials, EndpointAuthenticationService endpointAuthenticationService)
        {
            Logger = logger;
            Endpoint = endpoint;
            Credentials = credentials;
            Mapping = mapping;
            EndpointAuthenticationService = endpointAuthenticationService;
            var originalDestinationTables = Mapping.Destination.GetOriginalDestinationSchema().GetTables();
            var originalDestinationMappingTable = originalDestinationTables.FirstOrDefault(obj => obj.Name == Mapping.DestinationTable.Name);
            _destinationPrimaryKeyColumns = originalDestinationMappingTable?.Columns.Where(obj => obj.IsPrimaryKey)?.ToDictionary(obj => obj.Name, obj => obj.Type) ?? new Dictionary<string, Type>();
        }

        public void WriteToERP(Dictionary<string, object> Row, string writerName)
        {
            string url = GetEndpointURL(Endpoint, "");

            if (!Mapping.Conditionals.CheckConditionals(Row))
            {
                return;
            }

            var columnMappings = Mapping.GetColumnMappings();
            var keyColumnValuesForFilter = GetKeyColumnValuesForFilter(Row, columnMappings);
            string request = MapValuesToJSon(columnMappings, Row);

            if (keyColumnValuesForFilter.Any())
            {
                string filter = string.Join(" and ", keyColumnValuesForFilter);
                Endpoint.Parameters = EndpointSourceReader.AddOrUpdateParameter(Endpoint.Parameters, "$filter", filter);
                url = GetEndpointURL(Endpoint, "");
                Endpoint.Parameters = EndpointSourceReader.RemoveParameter(Endpoint.Parameters, "$filter", filter);
            }

            var response = GetFromEndpoint<JObject>(url, null);
            url = GetEndpointURL(Endpoint, "");
            Task<RestResponse<string>> awaitResponseFromERP;
            if (response != null && response.Count > 0)
            {
                if(response.Count > 1)
                {
                    Logger?.Error($"The filter returned too many records, please update or change filter.");
                    return;
                }

                Logger?.Info($"Recieved response from ERP = {response[0].ToString(Formatting.None)}");
                Dictionary<string, string> headers = new Dictionary<string, string>() { { "Content-Type", "application/json; charset=utf-8" } };

                List<string> primaryKeyColumnValuesForPatch = new List<string>();
                foreach (KeyValuePair<string, JToken> item in response[0])
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
                    url = GetEndpointURL(Endpoint, patchURL);
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
                Logger.Warn($"Error {writerName} Url: {url}. Response Error: {awaitResponseFromERP.Result.Error}");
            }
        }

        internal string GetEndpointURL(Endpoint endpoint, string patchValue)
        {
            var oldEndpointUrl = endpoint.Url;
            string result = "";
            if (BaseEndpointProvider.EndpointIsLoadAllEntities(Endpoint.Url))
            {
                endpoint.Url = new Uri(new Uri(Endpoint.Url), Mapping.DestinationTable.Name).AbsoluteUri;
            }
            if (!string.IsNullOrEmpty(patchValue))
            {
                endpoint.Url += patchValue;
            }
            result = endpoint.FullUrl;
            Endpoint.Url = oldEndpointUrl;
            return result;
        }

        internal Task<RestResponse<T>> PostToEndpoint<T>(string URL, string jsonObject, Dictionary<string, string> header, bool patch)
        {
            var _client = new HttpRestClient(Credentials, RequestTimeout, Logger);
            EndpointAuthentication endpointAuthentication = GetEndpointAuthentication();
            Task<RestResponse<T>> awaitResponseFromBC;
            if (AuthenticationHelper.IsTokenBased(endpointAuthentication))
            {
                string token = AuthenticationHelper.GetToken(Endpoint, endpointAuthentication);
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
            EndpointAuthentication endpointAuthentication = GetEndpointAuthentication();
            Task<RestResponse<ResponseFromERP<T>>> awaitResponseFromBC;
            if (AuthenticationHelper.IsTokenBased(endpointAuthentication))
            {
                string token = AuthenticationHelper.GetToken(Endpoint, endpointAuthentication);
                awaitResponseFromBC = _client.GetAsync<ResponseFromERP<T>>(URL, token, header);
            }
            else
            {
                awaitResponseFromBC = _client.GetAsync<ResponseFromERP<T>>(URL, endpointAuthentication, header);
            }
            awaitResponseFromBC.Wait();
            return awaitResponseFromBC.Result.Content.Value;
        }

        private EndpointAuthentication GetEndpointAuthentication()
        {
            return EndpointAuthenticationService.GetEndpointAuthenticationById(Endpoint.AuthenticationId);
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
            }

            if (columnMapping.HasNewGuidScript())
            {
                result = columnMapping.GetScriptValue();
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
            JObject jObject = new JObject();

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
                                jObject.Add(columnMapping.DestinationColumn.Name, Converter.ToDecimal(columnValue));
                                break;
                            case "int":
                                jObject.Add(columnMapping.DestinationColumn.Name, Converter.ToInt64(columnValue));
                                break;
                            case "double":
                                jObject.Add(columnMapping.DestinationColumn.Name, Converter.ToDecimal(columnValue));
                                break;
                            default:
                                jObject.Add(columnMapping.DestinationColumn.Name, Converter.ToString(columnValue));
                                break;
                        }
                    }
                }
            }
            return jObject.ToString();
        }

        internal void WriteData(ISourceReader sourceReader, IDestinationWriter writer)
        {
            Logger?.Log($"Begin synchronizing '{Mapping.SourceTable.Name}' to '{Mapping.DestinationTable.Name}'.");
            try
            {
                while (!sourceReader.IsDone())
                {
                    var sourceRow = sourceReader.GetNext();
                    ProcessInputRow(Mapping, sourceRow);
                    writer.Write(sourceRow);
                }
            }
            catch (Exception e)
            {
                Logger?.Log(e.ToString());
                throw e;
            }
            Logger?.Log($"End synchronizing '{Mapping.SourceTable.Name}' to '{Mapping.DestinationTable.Name}'.");
        }
    }
}
