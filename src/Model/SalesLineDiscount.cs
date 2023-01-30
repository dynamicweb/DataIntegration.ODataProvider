using Newtonsoft.Json;

namespace Dynamicweb.DataIntegration.Providers.ODataProvider.Model
{
    internal class SalesLineDiscount
    {
        [JsonProperty("@odata.etag")]
        public string OdataEtag { get; set; }
        public string Type { get; set; }
        public string Code { get; set; }
        public string SalesType { get; set; }
        public string SalesCode { get; set; }
        public string Starting_Date { get; set; }
        public string Currency_Code { get; set; }
        public string Variant_Code { get; set; }
        public string Unit_of_Measure_Code { get; set; }
        public double Minimum_Quantity { get; set; }
        public string SalesTypeFilter { get; set; }
        public string SalesCodeFilterCtrl { get; set; }
        public string StartingDateFilter { get; set; }
        public string ItemTypeFilter { get; set; }
        public string CodeFilterCtrl { get; set; }
        public string SalesCodeFilterCtrl2 { get; set; }
        public string FilterDescription { get; set; }
        public double Line_Discount_Percent { get; set; }
        public string Ending_Date { get; set; }
    }
}
