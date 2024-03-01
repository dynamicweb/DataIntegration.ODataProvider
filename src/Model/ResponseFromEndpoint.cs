using System.Collections.Generic;
using System.Text.Json.Serialization;

namespace Dynamicweb.DataIntegration.Providers.ODataProvider.Model;

internal class ResponseFromEndpoint<T>
{
    [JsonPropertyName("@odata.context")]
    public string Odata { get; set; }
    [JsonPropertyName("value")]
    public List<T> Value { get; set; }
    [JsonPropertyName("@odata.nextLink")]
    public string OdataNextLink { get; set; }
}
