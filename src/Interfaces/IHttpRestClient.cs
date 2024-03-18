using Dynamicweb.DataIntegration.EndpointManagement;
using Dynamicweb.DataIntegration.Providers.ODataProvider.Model;
using System;
using System.Collections.Generic;
using System.IO;
using System.Net;
using System.Threading.Tasks;

namespace Dynamicweb.DataIntegration.Providers.ODataProvider.Interfaces;

/// <summary>
/// Allows you to invoke HTTP REST APIs with a single line of code.
///
/// If a non-successful HTTP response is returned from your endpoint, an
/// HttpException will be thrown, from where you can retrieve the StatusCode
/// and the error message.
/// </summary>
internal interface IHttpRestClient
{
    /// <summary>
    /// Post an object asynchronously to the specified URL. You can supply a Stream as your request,
    /// and the service will intelligently determine it's a stream, and serialize it directly on to the HTTP
    /// request stream.
    /// </summary>
    /// <typeparam name="TRequest">Type of request.</typeparam>
    /// <typeparam name="TResponse">Type of response.</typeparam>
    /// <param name="url">URL of your request.</param>
    /// <param name="request">Payload of your request.</param>
    /// <param name="headers">HTTP headers for your request.</param>
    /// <returns>Object returned from your request.</returns>
    Task<RestResponse<TResponse>> PostAsync<TRequest, TResponse>(string url, TRequest request, EndpointAuthentication endpointAuthentication, Dictionary<string, string> headers = null);

    /// <summary>
    /// Post an object asynchronously to the specified URL with the specified Bearer token.
    /// You can supply a Stream as your request, and the service will intelligently
    /// determine it's a stream, and serialize it directly on to the HTTP request stream.
    /// </summary>
    /// <typeparam name="TRequest">Type of request.</typeparam>
    /// <typeparam name="TResponse">Type of response.</typeparam>
    /// <param name="url">URL of your request.</param>
    /// <param name="request">Payload of your request.</param>
    /// <param name="token">Bearer token for your request.</param>
    /// <returns>Object returned from your request.</returns>
    Task<RestResponse<TResponse>> PostAsync<TRequest, TResponse>(string url, TRequest request, string token, Dictionary<string, string> headers = null);
    Task<RestResponse<TResponse>> PatchAsync<TRequest, TResponse>(string url, TRequest request, EndpointAuthentication endpointAuthentication, Dictionary<string, string> headers = null);
    Task<RestResponse<TResponse>> PatchAsync<TRequest, TResponse>(string url, TRequest request, string token, Dictionary<string, string> headers = null);

    /// <summary>
    /// Put an object asynchronously to the specified URL. You can supply a Stream as your request,
    /// and the service will intelligently determine it's a stream, and serialize it directly on to the HTTP
    /// request stream.
    /// </summary>
    /// <typeparam name="TRequest">Type of request.</typeparam>
    /// <typeparam name="TResponse">Type of response.</typeparam>
    /// <param name="url">URL of your request.</param>
    /// <param name="request">Payload of your request.</param>
    /// <param name="headers">HTTP headers for your request.</param>
    /// <returns>Object returned from your request.</returns>
    Task<RestResponse<TResponse>> PutAsync<TRequest, TResponse>(string url, TRequest request, EndpointAuthentication endpointAuthentication, Dictionary<string, string> headers = null);

    /// <summary>
    /// Put an object asynchronously to the specified URL with the specified Bearer token.
    /// You can supply a Stream as your request, and the service will intelligently
    /// determine it's a stream, and serialize it directly on to the HTTP request stream.
    /// </summary>
    /// <typeparam name="TRequest">Type of request.</typeparam>
    /// <typeparam name="TResponse">Type of response.</typeparam>
    /// <param name="url">URL of your request.</param>
    /// <param name="request">Payload of your request.</param>
    /// <param name="token">Bearer token for your request.</param>
    /// <returns>Object returned from your request.</returns>
    Task<RestResponse<TResponse>> PutAsync<TRequest, TResponse>(string url, TRequest request, string token, Dictionary<string, string> headers = null);

    /// <summary>
    /// Get a resource from some URL.
    /// </summary>
    /// <typeparam name="TResponse">Type of response.</typeparam>
    /// <param name="url">URL of your request.</param>
    /// <param name="headers">HTTP headers for your request.</param>
    /// <returns>Object returned from your request.</returns>
    Task<RestResponse<TResponse>> GetAsync<TResponse>(string url, EndpointAuthentication endpointAuthentication, Dictionary<string, string> headers = null);

    /// <summary>
    /// Get a resource from a URL with the specified Bearer token.
    /// </summary>
    /// <typeparam name="TResponse">Type of response.</typeparam>
    /// <param name="url">URL of your request.</param>
    /// <param name="token">Bearer token for your request.</param>
    /// <returns>Object returned from your request.</returns>
    Task<RestResponse<TResponse>> GetAsync<TResponse>(string url, string token, Dictionary<string, string> headers = null);

    /// <summary>
    /// Get a resource from a URL. This overload requires you to supply
    /// an Action taking a Stream as its input, from whence you can directly access the response content,
    /// without having to load it into memory. This is useful for downloading larger documents from a URL.
    /// </summary>
    /// <param name="url">URL of your request.</param>
    /// <param name="functor">Action lambda function given the response Stream for you to do whatever you wish
    /// with once the request returns.</param>
    /// <param name="headers">HTTP headers for your request.</param>
    /// <returns>Async void Task</returns>
    Task GetAsync(string url, Action<Stream, HttpStatusCode, Dictionary<string, string>> functor, EndpointAuthentication endpointAuthentication, Dictionary<string, string> headers = null);

    /// <summary>
    /// Get a resource from a URL with the specified Bearer token.
    /// This overload requires you to supply an Action taking a Stream as its input,
    /// from whence you can directly access the response content, without having to load it
    /// into memory. This is useful for downloading larger documents from a URL.
    /// </summary>
    /// <param name="url">URL of your request.</param>
    /// <param name="functor">Action lambda function given the response Stream for you to do whatever you wish
    /// with once the request returns.</param>
    /// <param name="token">Bearer token for your request.</param>
    /// <returns>Async void Task</returns>
    Task GetAsync(string url, Action<Stream, HttpStatusCode, Dictionary<string, string>> functor, string token, Dictionary<string, string> headers = null);

    /// <summary>
    /// Delete a resource.
    /// </summary>
    /// <typeparam name="TResponse">Type of response.</typeparam>
    /// <param name="url">URL of your request.</param>
    /// <param name="headers">HTTP headers for your request.</param>
    /// <returns>Result of your request.</returns>
    Task<RestResponse<TResponse>> DeleteAsync<TResponse>(string url, EndpointAuthentication endpointAuthentication, Dictionary<string, string> headers = null);

    /// <summary>
    /// Delete a resource with the specified Bearer token.
    /// </summary>
    /// <typeparam name="TResponse">Type of response.</typeparam>
    /// <param name="url">URL of your request.</param>
    /// <param name="token">Bearer token for your request.</param>
    /// <returns>Result of your request.</returns>
    Task<RestResponse<TResponse>> DeleteAsync<TResponse>(string url, string token, Dictionary<string, string> headers = null);
}