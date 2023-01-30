using Newtonsoft.Json;
using System;
using System.Collections.Generic;
using System.Text;

namespace Dynamicweb.DataIntegration.Providers.ODataProvider.Model
{
    internal class SalesPriceListLine
    {
        [JsonProperty("@odata.etag")]
        public string odataetag { get; set; }
        public string Price_List_Code { get; set; }
        public int Line_No { get; set; }
        public string SourceType { get; set; }
        public string JobSourceType { get; set; }
        public string ParentSourceNo { get; set; }
        public string AssignToParentNo { get; set; }
        public string SourceNo { get; set; }
        public string AssignToNo { get; set; }
        public string CurrencyCode { get; set; }
        public DateTime StartingDate { get; set; }
        public DateTime EndingDate { get; set; }
        public string Asset_Type { get; set; }
        public string Asset_No { get; set; }
        public string Product_No { get; set; }
        public string Description { get; set; }
        public string Variant_Code { get; set; }
        public string Variant_Code_Lookup { get; set; }
        public string Work_Type_Code { get; set; }
        public string Unit_of_Measure_Code { get; set; }
        public string Unit_of_Measure_Code_Lookup { get; set; }
        public double Minimum_Quantity { get; set; }
        public string Amount_Type { get; set; }
        public double Unit_Price { get; set; }
        public decimal Cost_Factor { get; set; }
        public bool Allow_Line_Disc { get; set; }
        public double Line_Discount_Percent { get; set; }
        public bool Allow_Invoice_Disc { get; set; }
        public bool PriceIncludesVAT { get; set; }
        public string VATBusPostingGrPrice { get; set; }
    }
}
