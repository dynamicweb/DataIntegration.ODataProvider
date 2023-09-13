using Dynamicweb.Configuration;
using Dynamicweb.DataIntegration.EndpointManagement;
using Dynamicweb.DataIntegration.Providers.ODataProvider.Interfaces;
using Dynamicweb.Logging;
using Newtonsoft.Json.Linq;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Net;
using System.Net.Http;
using System.Text;
using System.Text.Json;
using System.Threading.Tasks;

namespace Dynamicweb.DataIntegration.Providers.ODataProvider.Model
{
    /// <inheritdoc />
    internal class HttpRestClient : IHttpRestClient
    {
        private readonly HttpClient _client;
        private readonly Func<string, HttpClient> _factory;
        private readonly ILogger _logger;

        // Default HTTP headers for empty HTTP request.
        private static readonly Dictionary<string, string> DefaultHeadersEmptyRequest =
            new Dictionary<string, string> {
                { "Accept", "application/json" },
            };

        // Default HTTP headers for HTTP request with a payload.
        private static readonly Dictionary<string, string> DefaultHeadersRequest =
            new Dictionary<string, string> {
                { "Content-Type", "application/json" },
                { "Accept", "application/json" },
            };

        /// <summary>
        /// Initializes a new instance of the <see cref="HttpRestClient"/> class with no credentials and no logger.
        /// </summary>
        internal HttpRestClient()
        {
            _factory = url => _client;
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="HttpRestClient"/> class with the specified logger.
        /// </summary>
        /// <param name="logger">Logger to log every request and errors to.</param>
        internal HttpRestClient(ILogger logger) : this()
        {
            _logger = logger ?? throw new ArgumentNullException(nameof(logger));
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="HttpRestClient"/> class with the specified credentials.
        /// </summary>
        /// <param name="credentials">Credentials to access endpoint.</param>
        internal HttpRestClient(ICredentials credentials, int timeout) : this()
        {
            var clientHandler = new HttpClientHandler
            {
                Credentials = credentials ?? throw new ArgumentNullException(nameof(credentials)),
                PreAuthenticate = true
            };
			if (SystemConfiguration.Instance.GetBoolean("/Globalsettings/Modules/EndpointManagement/SkipCertificateValidation"))
            {
                clientHandler.ServerCertificateCustomValidationCallback =
                    (httpRequestMessage, cert, cetChain, policyErrors) =>
                    {
                        return true;
                    };
            }
            _client = new HttpClient(clientHandler);
            _client.Timeout = TimeSpan.FromMinutes(timeout);
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="HttpRestClient"/> class with the specified credentials and the specified logger.
        /// </summary>
        /// <param name="credentials">Credentials to access endpoint.</param>
        /// <param name="logger">Logger to log every request and errors to.</param>
        internal HttpRestClient(ICredentials credentials, int timeout, ILogger logger) : this(logger)
        {
            var clientHandler = new HttpClientHandler
            {
                Credentials = credentials ?? throw new ArgumentNullException(nameof(credentials)),
                PreAuthenticate = true
            };
            if (SystemConfiguration.Instance.GetBoolean("/Globalsettings/Modules/EndpointManagement/SkipCertificateValidation"))
            {
                clientHandler.ServerCertificateCustomValidationCallback =
                    (httpRequestMessage, cert, cetChain, policyErrors) =>
                    {
                        return true;
                    };
            }
            _client = new HttpClient(clientHandler);
            _client.Timeout = TimeSpan.FromMinutes(timeout);
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="HttpRestClient"/> class with the specified HttpClient factory function but no logging.
        /// </summary>
        /// <param name="factory">Factory function to use to create .Net HttpClient</param>
        internal HttpRestClient(Func<string, HttpClient> factory) : this()
        {
            _factory = factory ?? throw new ArgumentNullException(nameof(factory));
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="HttpRestClient"/> class with the specified HttpClient factory function, and the specified logger.
        /// </summary>
        /// <param name="logger">Logger to log every request and errors to</param>
        /// <param name="factory">Factory function to use to create .Net HttpClient
        /// instances from.</param>
        internal HttpRestClient(Func<string, HttpClient> factory, ILogger logger) : this(logger)
        {
            _factory = factory ?? throw new ArgumentNullException(nameof(factory));
        }

        /// <inheritdoc />
        public async Task<RestResponse<TResponse>> PostAsync<TRequest, TResponse>(string url, TRequest request, EndpointAuthentication endpointAuthentication, Dictionary<string, string> headers = null)
        {
            _logger?.Info($"HttpRestClient: '{url}' invoked with POST and '{request}'");

            return await CreateContentRequest<TResponse>(url, HttpMethod.Post, request, GetDefaultBasicHeaders(endpointAuthentication, headers ?? DefaultHeadersRequest)).ConfigureAwait(false);
        }

        /// <inheritdoc />
        public async Task<RestResponse<TResponse>> PostAsync<TRequest, TResponse>(string url, TRequest request, string token, Dictionary<string, string> headers = null)
        {
            _logger?.Info($"HttpRestClient: '{url}' invoked with POST, '{request}' and Bearer token of '{token}'");

            return await CreateContentRequest<TResponse>(url, HttpMethod.Post, request, GetDefaultBearerTokenHeaders(token, headers ?? DefaultHeadersRequest)).ConfigureAwait(false);
        }
        public async Task<RestResponse<TResponse>> PatchAsync<TRequest, TResponse>(string url, TRequest request, EndpointAuthentication endpointAuthentication, Dictionary<string, string> headers = null)
        {
            _logger?.Info($"HttpRestClient: '{url}' invoked with PATCH and '{request}'");

            return await CreateContentRequest<TResponse>(url, new HttpMethod("PATCH"), request, GetDefaultBasicHeaders(endpointAuthentication, headers ?? DefaultHeadersRequest)).ConfigureAwait(false);
        }
        public async Task<RestResponse<TResponse>> PatchAsync<TRequest, TResponse>(string url, TRequest request, string token, Dictionary<string, string> headers = null)
        {
            _logger?.Info($"HttpRestClient: '{url}' invoked with PATCH and '{request}'");

            return await CreateContentRequest<TResponse>(url, new HttpMethod("PATCH"), request, GetDefaultBearerTokenHeaders(token, headers ?? DefaultHeadersRequest)).ConfigureAwait(false);
        }
        /// <inheritdoc />
        public async Task<RestResponse<TResponse>> PutAsync<TRequest, TResponse>(string url, TRequest request, EndpointAuthentication endpointAuthentication, Dictionary<string, string> headers = null)
        {
            _logger?.Info($"HttpRestClient: '{url}' invoked with PUT and '{request}'");

            return await CreateContentRequest<TResponse>(url, HttpMethod.Put, request, GetDefaultBasicHeaders(endpointAuthentication, headers ?? DefaultHeadersRequest)).ConfigureAwait(false);
        }

        /// <inheritdoc />
        public async Task<RestResponse<TResponse>> PutAsync<TRequest, TResponse>(string url, TRequest request, string token, Dictionary<string, string> headers = null)
        {
            _logger?.Info($"HttpRestClient: '{url}' invoked with PUT, '{request}' and Bearer token of '{token}'");

            return await CreateContentRequest<TResponse>(url, HttpMethod.Put, request, GetDefaultBearerTokenHeaders(token, headers ?? DefaultHeadersRequest)).ConfigureAwait(false);
        }

        /// <inheritdoc />
        public async Task<RestResponse<TResponse>> GetAsync<TResponse>(string url, EndpointAuthentication endpointAuthentication, Dictionary<string, string> headers = null)
        {
            _logger?.Info($"HttpRestClient: '{url}' invoked with GET");

            return await CreateEmptyRequest<TResponse>(url, HttpMethod.Get, GetDefaultBasicHeaders(endpointAuthentication, headers ?? DefaultHeadersEmptyRequest)).ConfigureAwait(false);
        }

        /// <inheritdoc />
        public async Task<RestResponse<TResponse>> GetAsync<TResponse>(string url, string token, Dictionary<string, string> headers = null)
        {
            _logger?.Info($"HttpRestClient: '{url}' invoked with GET and Bearer token of '{token}'");

            return await CreateEmptyRequest<TResponse>(url, HttpMethod.Get, GetDefaultBearerTokenHeaders(token, headers ?? DefaultHeadersEmptyRequest)).ConfigureAwait(false);
        }

        /// <inheritdoc />
        public async Task GetAsync(string url, Action<Stream, HttpStatusCode, Dictionary<string, string>> functor, EndpointAuthentication endpointAuthentication, Dictionary<string, string> headers = null)
        {
            _logger?.Info($"HttpRestClient: '{url}' invoked with GET for Stream response");

            await CreateEmptyRequestStreamResponse(url, HttpMethod.Get, functor, GetDefaultBasicHeaders(endpointAuthentication, headers ?? DefaultHeadersEmptyRequest)).ConfigureAwait(false);
        }

        /// <inheritdoc />
        public async Task GetAsync(string url, Action<Stream, HttpStatusCode, Dictionary<string, string>> functor, string token, Dictionary<string, string> headers = null)
        {
            _logger?.Info($"HttpRestClient: '{url}' invoked with GET and Bearer token of '{token}'");

            await CreateEmptyRequestStreamResponse(url, HttpMethod.Get, functor, GetDefaultBearerTokenHeaders(token, headers ?? DefaultHeadersEmptyRequest)).ConfigureAwait(false);
        }

        /// <inheritdoc />
        public async Task<RestResponse<TResponse>> DeleteAsync<TResponse>(string url, EndpointAuthentication endpointAuthentication, Dictionary<string, string> headers = null)
        {
            _logger?.Info($"HttpRestClient: '{url}' invoked with DELETE");

            return await CreateEmptyRequest<TResponse>(url, HttpMethod.Delete, GetDefaultBasicHeaders(endpointAuthentication, headers ?? DefaultHeadersRequest)).ConfigureAwait(false);
        }

        /// <inheritdoc />
        public async Task<RestResponse<TResponse>> DeleteAsync<TResponse>(string url, string token, Dictionary<string, string> headers = null)
        {
            _logger?.Info($"HttpRestClient: '{url}' invoked with DELETE and Bearer token '{token}'");

            return await CreateEmptyRequest<TResponse>(url, HttpMethod.Delete, GetDefaultBearerTokenHeaders(token, headers ?? DefaultHeadersRequest)).ConfigureAwait(false);
        }

        /// <summary>
        /// Responsible for creating an HTTP request of specified type. Only used during
        /// GET and DELETE requests, since you cannot apply a payload to your request.
        /// </summary>
        /// <typeparam name="TResponse">Type of response.</typeparam>
        /// <param name="url">URL of your request.</param>
        /// <param name="method">HTTP method or verb to create your request as.</param>
        /// <param name="headers">HTTP headers for your request.</param>
        /// <returns>Object returned from your request.</returns>
        protected virtual async Task<RestResponse<TResponse>> CreateEmptyRequest<TResponse>(string url, HttpMethod method, Dictionary<string, string> headers)
        {
            using (var msg = CreateRequestMessage(method, url, headers))
            {
                return await GetResult<TResponse>(url, msg);
            }
        }

        /// <summary>
        /// Responsible for creating a request of the specified type. Used
        /// only during POST and PUT since it requires a payload to be provided.
        /// </summary>
        /// <typeparam name="TResponse">Type of response.</typeparam>
        /// <param name="url">URL of your request.</param>
        /// <param name="method">HTTP method or verb to create your request as.</param>
        /// <param name="input">Payload for your request.</param>
        /// <param name="headers">HTTP headers for your request.</param>
        /// <returns>Object returned from your request.</returns>
        protected virtual async Task<RestResponse<TResponse>> CreateContentRequest<TResponse>(string url, HttpMethod method, object input, Dictionary<string, string> headers)
        {
            using (var msg = CreateRequestMessage(method, url, headers))
            {
                if (input is Stream stream)
                {
                    using (var content = new StreamContent(stream))
                    {
                        AddContentHeaders(content, headers);
                        msg.Content = content;
                        return await GetResult<TResponse>(url, msg);
                    }
                }

                var stringContent = input is string strInput ?
                    strInput :
                    JsonSerializer.Serialize(input);

                using (var content = new StringContent(stringContent))
                {
                    AddContentHeaders(content, headers);
                    msg.Content = content;
                    return await GetResult<TResponse>(url, msg);
                }
            }
        }

        /// <summary>
        /// Responsible for creating requests of type GET where the caller wants to directly
        /// access the HTTP response stream, instead of having a typed callback returned to him.
        /// </summary>
        /// <param name="url">URL of your request.</param>
        /// <param name="method">HTTP method or verb to create your request as.</param>
        /// <param name="callback">Callback function that will be invoked with the response
        /// stream when it is ready.</param>
        /// <param name="headers">HTTP headers for your request.</param>
        /// <returns>An async Task</returns>
        protected virtual async Task CreateEmptyRequestStreamResponse(
            string url,
            HttpMethod method,
            Action<Stream, HttpStatusCode, Dictionary<string, string>> callback,
            Dictionary<string, string> headers
            )
        {
            using (var msg = CreateRequestMessage(method, url, headers))
            {
                var response = await _factory(url).SendAsync(msg);
                {
                    using (var content = response.Content)
                    {
                        {
                            // Retrieve HTTP headers, both response and content headers.
                            var responseHeaders = GetHeaders(response, content);

                            // Clone the stream since we will be using this in synchrounous methods earlier in the callstack meaning we cannot have the stream disposed too soon
                            Stream contentStream = new MemoryStream();
                            await content.CopyToAsync(contentStream);
                            contentStream.Position = 0;

                            // Check if request was successful, and if not, throw an exception.
                            if (!response.IsSuccessStatusCode)
                            {
                                var statusText = await content.ReadAsStringAsync();
                                _logger?.Error($"HttpRestClient: '{url}' invoked with '{method}' returned {response.StatusCode} and '{statusText}'");
                                callback(contentStream, response.StatusCode, responseHeaders);
                            }
                            else
                            {
                                callback(contentStream, response.StatusCode, responseHeaders);
                            }
                        }
                    }
                }
            }
        }

        /// <summary>
        /// Responsible for sending and retrieving your HTTP request and response.
        /// Only invoked if you are requesting a non Stream result.
        /// </summary>
        /// <typeparam name="TResponse">Response type from endpoint.</typeparam>
        /// <param name="url">URL for your request.</param>
        /// <param name="msg">HTTP request message.</param>
        /// <returns>Object returned from your request.</returns>
        protected virtual async Task<RestResponse<TResponse>> GetResult<TResponse>(string url, HttpRequestMessage msg)
        {
            using (var response = await _factory(url).SendAsync(msg))
            {
                using (var content = response.Content)
                {
                    // Retrieve HTTP headers, both response and content headers.
                    var responseHeaders = GetHeaders(response, content);

                    // Retrieve actual content.
                    var responseContent = await content.ReadAsStringAsync();

                    // Check if request was successful, and if not, throw an exception.
                    if (!response.IsSuccessStatusCode)
                    {
                        var responseResult = new RestResponse<TResponse>
                        {
                            Error = responseContent,
                            Status = response.StatusCode,
                            Headers = responseHeaders,
                        };
                        var listOfHeaders = responseHeaders.Select(obj => $"{obj.Key}:{obj.Value}").ToList();
                        _logger?.Error($"Endpoint error: {responseContent} Status: {response.StatusCode} Headers: {string.Join(",", listOfHeaders)}");
                        return responseResult;
                    }
                    else
                    {
                        var responseResult = new RestResponse<TResponse>
                        {
                            Status = response.StatusCode,
                            Headers = responseHeaders,
                        };

                        // Check if caller wants a string type of return
                        if (typeof(TResponse) == typeof(string))
                        {
                            responseResult.Content = (TResponse)(object)responseContent;
                        }
                        else if (typeof(IConvertible).IsAssignableFrom(typeof(TResponse)))
                        {
                            /*
                             * Check if Response type implements IConvertible, at which point we simply convert the
                             * response instead of parsing it using JSON conversion.
                             *
                             * This might be used if caller is requesting an integer, or some other object
                             * that has automatic conversion from string to itself.
                             */
                            responseResult.Content = (TResponse)Convert.ChangeType(responseContent, typeof(TResponse));
                        }
                        else
                        {
                            bool isEmptyPatchRequestResponse = response.StatusCode == HttpStatusCode.NoContent && string.IsNullOrEmpty(responseContent);
                            if (!isEmptyPatchRequestResponse)
                            {
                                /*
                                 * Check if the caller is interested in some sort of JContainer, such as a JArray or JObject,
                                 * at which point we simply return the above object immediately as such.
                                 */
                                var objResult = JToken.Parse(responseContent);
                                if (typeof(TResponse) == typeof(JContainer))
                                {
                                    responseResult.Content = (TResponse)(object)objResult;
                                }

                                //Converting above JContainer to instance of requested type, and returns object to caller.
                                responseResult.Content = objResult.ToObject<TResponse>();
                            }
                        }

                        // Finally, we can return result to caller.
                        return responseResult;
                    }
                }
            }
        }

        //Create a new request message, and decorates it with the relevant HTTP headers.
        private static HttpRequestMessage CreateRequestMessage(HttpMethod method, string url, Dictionary<string, string> headers)
        {
            var msg = new HttpRequestMessage
            {
                RequestUri = new Uri(url),
                Method = method
            };

            foreach (var idx in headers.Keys)
            {
                /*
                 * We ignore all headers that belongs to the content, and add all other headers to the request.
                 * This is because all HttpContent headers are added later, but only if content is being transmitted.
                 * This allows support for any HTTP headers, including custom headers.
                 */
                switch (idx)
                {
                    case "Allow":
                    case "Content-Disposition":
                    case "Content-Encoding":
                    case "Content-Language":
                    case "Content-Length":
                    case "Content-Location":
                    case "Content-MD5":
                    case "Content-Range":
                    case "Content-Type":
                    case "Expires":
                    case "Last-Modified":
                        break;
                    default:
                        msg.Headers.Add(idx, headers[idx]);
                        break;
                }
            }
            return msg;
        }

        // Decorate the HTTP content with the relevant HTTP headers from the specified dictionary.
        private static void AddContentHeaders(HttpContent content, Dictionary<string, string> headers)
        {
            foreach (var idx in headers.Keys)
            {
                // Adding all Content HTTP headers, and ignoring the rest
                switch (idx)
                {
                    case "Allow":
                    case "Content-Disposition":
                    case "Content-Encoding":
                    case "Content-Language":
                    case "Content-Length":
                    case "Content-Location":
                    case "Content-MD5":
                    case "Content-Range":
                    case "Content-Type":
                    case "Expires":
                    case "Last-Modified":
                        if (content.Headers.Contains(idx))
                        {
                            content.Headers.Remove(idx);
                        }

                        content.Headers.Add(idx, headers[idx]);
                        break;
                }
            }
        }

        private Dictionary<string, string> GetDefaultBearerTokenHeaders(string token, Dictionary<string, string> headers = null)
        {
            Dictionary<string, string> result = new Dictionary<string, string>
            {
                    { "Accept", "text/html,application/xhtml+xml,application/xml,application/json" },
                    { "Authorization", "Bearer " + (token ?? throw new ArgumentNullException(nameof(token))) }
            };
            if (headers != null)
            {
                foreach (KeyValuePair<string, string> item in headers)
                {
                    if (!result.TryGetValue(item.Key, out var value))
                    {
                        result.Add(item.Key, item.Value);
                    }
                }
            }
            return result;
        }
        private Dictionary<string, string> GetDefaultBasicHeaders(EndpointAuthentication _endpointAuthentication, Dictionary<string, string> headers = null)
        {
            NetworkCredential networkCredential = _endpointAuthentication.GetNetworkCredential();
            Dictionary<string, string> result = new Dictionary<string, string>
            {
                    { "Accept", "text/html,application/xhtml+xml,application/xml,application/json" },
                    { "Authorization", "Basic " + Convert.ToBase64String(Encoding.ASCII.GetBytes(networkCredential.UserName + ":" + networkCredential.Password)) }
            };
            if (headers != null)
            {
                foreach (KeyValuePair<string, string> item in headers)
                {
                    if (!result.TryGetValue(item.Key, out var value))
                    {
                        result.Add(item.Key, item.Value);
                    }
                }
            }
            return result;
        }

        private static Dictionary<string, string> GetHeaders(HttpResponseMessage response, HttpContent content)
        {
            var headers = new Dictionary<string, string>();
            foreach (var idx in response.Headers)
            {
                headers.Add(idx.Key, string.Join(";", idx.Value));
            }
            foreach (var idx in content.Headers)
            {
                headers.Add(idx.Key, string.Join(";", idx.Value));
            }

            return headers;
        }
    }
}