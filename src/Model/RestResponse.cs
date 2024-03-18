using System.Collections.Generic;
using System.Net;

namespace Dynamicweb.DataIntegration.Providers.ODataProvider.Model;

/// <summary>
/// Wrap an HTTP response, with headers, content and status code.
/// </summary>
internal class RestResponse<T>
{
    /// <summary>
    /// Content of the response.
    /// </summary>
    internal T Content { get; set; }

    /// <summary>
    /// HTTP headers of the response.
    /// </summary>
    internal Dictionary<string, string> Headers { get; set; }

    /// <summary>
    /// Status response code for the request.
    /// </summary>
    internal HttpStatusCode Status { get; set; }

    /// <summary>
    /// Will only be populated if request is not successful, at which point the
    /// response content can be found in this property.
    /// </summary>
    internal string Error { get; set; }
}