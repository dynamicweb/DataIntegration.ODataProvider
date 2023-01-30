using Newtonsoft.Json;

namespace Dynamicweb.DataIntegration.Providers.ODataProvider.Model
{
    internal class SalesPrice
    {
        [JsonProperty("@odata.etag")]
        public string OdataEtag { get; set; }
        public string Item_No { get; set; }
        public string Sales_Type { get; set; }
        public string Sales_Code { get; set; }
        public string Starting_Date { get; set; }
        public string Currency_Code { get; set; }
        public string Variant_Code { get; set; }
        public string Unit_of_Measure_Code { get; set; }
        public double Minimum_Quantity { get; set; }
        public string SalesTypeFilter { get; set; }
        public string SalesCodeFilterCtrl { get; set; }
        public string ItemNoFilterCtrl { get; set; }
        public string StartingDateFilter { get; set; }
        public string CurrencyCodeFilterCtrl { get; set; }
        public string FilterDescription { get; set; }
        public double Unit_Price { get; set; }
        public string Ending_Date { get; set; }
        public bool Price_Includes_VAT { get; set; }
        public bool Allow_Line_Disc { get; set; }
        public bool Allow_Invoice_Disc { get; set; }
        public string VAT_Bus_Posting_Gr_Price { get; set; }
    }
}
