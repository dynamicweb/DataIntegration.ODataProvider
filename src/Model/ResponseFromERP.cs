using Newtonsoft.Json;
using System.Collections.Generic;

namespace Dynamicweb.DataIntegration.Providers.ODataProvider.Model
{
    internal class ResponseFromERP<T>
    {
        [JsonProperty("@odata.context")]
        public string Odata { get; set; }
        [JsonProperty("value")]
        public List<T> Value { get; set; }
        [JsonProperty("@odata.nextLink")]
        public string OdataNextLink { get; set; }
    }
}
