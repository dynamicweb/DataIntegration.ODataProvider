using Dynamicweb.Core;
using Dynamicweb.DataIntegration.Integration;
using Dynamicweb.DataIntegration.Integration.Interfaces;
using Dynamicweb.DataIntegration.Providers.ODataProvider.Interfaces;
using Dynamicweb.DataIntegration.Providers.ODataProvider.Model;
using Dynamicweb.Ecommerce.Prices;
using Dynamicweb.Security.UserManagement;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;

namespace Dynamicweb.DataIntegration.Providers.ODataProvider
{
    public class ODataPriceWriter : IDestinationWriter, IDisposable, IODataDestinationWriter
    {
        private readonly string _shopId;
        private readonly ICurrencyReader _currencyReader;
        private ODataDestinationWriter _odataDestinationWriter;
        private bool _salesTypeCustomer;
        private bool _salesTypeAllCustomer;
        private bool _salesTypeCustomerPriceGroup;
        private string _defaultCurrencyCode;

        public Mapping Mapping { get; }
        internal ODataPriceWriter(ODataDestinationWriter odataDestinationWriter, ICurrencyReader currencyReader, string shopId, bool salesTypeCustomer, bool salesTypeAllCustomer, bool salesTypeCustomerPriceGroup)
        {
            _shopId = shopId;
            _currencyReader = currencyReader;
            _odataDestinationWriter = odataDestinationWriter;
            Mapping = _odataDestinationWriter.Mapping;
            _salesTypeCustomer = salesTypeCustomer;
            _salesTypeAllCustomer = salesTypeAllCustomer;
            _salesTypeCustomerPriceGroup = salesTypeCustomerPriceGroup;
            _defaultCurrencyCode = _currencyReader.GetDefaultCurrency().Code;
            if (_odataDestinationWriter.DeleteMissingRows)
            {
                _odataDestinationWriter.ItemsToBeDeleted = _odataDestinationWriter.GetAllSimplePriceByShopIdAndLanguageId(shopId, _odataDestinationWriter.LanguageId, 360).Select(obj => obj.Id.ToString()).ToList();
            }
        }
        public void Write(Dictionary<string, object> row)
        {
            var columnMappings = Mapping.GetColumnMappings();
            if (_salesTypeCustomer)
            {
                MapValuesToObject(GetSalesPricesFromBC("Customer"), columnMappings, true, false);
            }
            if (_salesTypeAllCustomer)
            {
                MapValuesToObject(GetSalesPricesFromBC("All Customers"), columnMappings, false, false);
            }
            if (_salesTypeCustomerPriceGroup)
            {
                MapValuesToObject(GetSalesPricesFromBC("Customer Price Group"), columnMappings, false, true);
            }
        }
        private List<SalesPrice> GetSalesPricesFromBC(string salesType)
        {
            List<SalesPrice> result = new List<SalesPrice>();
            try
            {
                string tempUrl = _odataDestinationWriter.Endpoint.Url;
                if (_odataDestinationWriter.Endpoint.Url.Contains("?$"))
                {
                    tempUrl = _odataDestinationWriter.Endpoint.Url.Substring(0, _odataDestinationWriter.Endpoint.Url.IndexOf("?"));
                }
                string URL = tempUrl + "?$filter=Sales_Type eq '" + salesType + "'";
                var header = new Dictionary<string, string>
                {
                    {"Accept", "application/xml"},
                };
                result = _odataDestinationWriter.GetFromBC<SalesPrice>(URL, header, _odataDestinationWriter.Endpoint);
            }
            catch (Exception ex)
            {
                _odataDestinationWriter.Logger?.Error("Something went wrong! " + ex.ToString());
            }
            return result;
        }
        private void MapValuesToObject(List<SalesPrice> salesPrices, ColumnMappingCollection columnMappings, bool salesTypeCustomer, bool salesTypeCustomerPriceGroup)
        {
            foreach (SalesPrice item in salesPrices)
            {
                string variantId = item.Variant_Code;
                if (!string.IsNullOrWhiteSpace(variantId))
                {
                    variantId = item.Item_No + variantId;
                }

                string startingDate = item.Starting_Date;
                string endingDate = item.Ending_Date;
                string unitofMeasureCode = item.Unit_of_Measure_Code;
                double amount = item.Unit_Price;
                double quantity = item.Minimum_Quantity;
                bool isWithVar = Converter.ToBoolean(item.Price_Includes_VAT);
                string salesCode = item.Sales_Code;
                string etag = item.OdataEtag;
                Price price;
                List<Price> existingPrice = _odataDestinationWriter.GetPriceByExternalId(etag);
                if (existingPrice.Any())
                {
                    price = existingPrice[0];
                    if (_odataDestinationWriter.DeleteMissingRows)
                    {
                        _odataDestinationWriter.ItemsToBeDeleted.Remove(price.Id.ToString());
                        _odataDestinationWriter.RowsToBeDeleted = _odataDestinationWriter.ItemsToBeDeleted.Count > 0;
                    }
                }
                else
                {
                    price = new Price
                    {
                        ShopId = _shopId,
                        LanguageId = _odataDestinationWriter.LanguageId,
                        VariantId = variantId,
                        ExternalId = etag
                    };
                }
                price.UserId = "";
                price.UserGroupId = "";
                price.UserCustomerNumber = "";
                price.CurrencyCode = !string.IsNullOrWhiteSpace(item.Currency_Code) ? item.Currency_Code : _defaultCurrencyCode;
                if (salesTypeCustomer)
                {
                    price.UserCustomerNumber = salesCode;
                }
                else if (salesTypeCustomerPriceGroup)
                {
                    Group group = Group.GetGroups().Where(obj => obj.Name == "Price_" + salesCode).FirstOrDefault();
                    if (group is object)
                    {
                        price.UserGroupId = group.ID.ToString();
                    }
                    else
                    {
                        return;
                    }
                }
                if (_odataDestinationWriter.ImportAll)
                {
                    price.VariantId = variantId;
                    if (Converter.ToDateTime(startingDate) != DateTime.MinValue)
                    {
                        price.ValidFrom = Converter.ToDateTime(startingDate);
                    }
                    if (Converter.ToDateTime(endingDate) != DateTime.MinValue)
                    {
                        price.ValidTo = Converter.ToDateTime(endingDate);
                    }
                    price.ShopId = _shopId;
                    price.LanguageId = _odataDestinationWriter.LanguageId;
                    price.ProductId = item.Item_No;
                    price.UnitId = unitofMeasureCode;
                    price.Amount = amount;
                    price.Quantity = quantity;
                    price.IsWithVat = isWithVar;
                }
                else
                {
                    foreach (var column in columnMappings)
                    {
                        if (column.Active)
                        {
                            IList<PropertyInfo> props = new List<PropertyInfo>(item.GetType().GetProperties());
                            PropertyInfo propertyInfo = props.Where(obj => obj.Name == column.SourceColumn.Name).FirstOrDefault();
                            if (propertyInfo != null)
                            {
                                var columnValue = BaseEndpointWriter.HandleScriptTypeForColumnMapping(column, propertyInfo.GetValue(item, null));

                                switch (column.DestinationColumn.Name)
                                {
                                    case "EcomPrices.PriceProductId":
                                        price.ProductId = columnValue;
                                        break;
                                    case "EcomPrices.PriceQuantity":
                                        price.Quantity = Converter.ToDouble(columnValue);
                                        break;
                                    case "EcomPrices.PriceAmount":
                                        price.Amount = Converter.ToDouble(columnValue);
                                        break;
                                    case "EcomPrices.PriceUnitId":
                                        price.UnitId = columnValue;
                                        break;
                                    case "EcomPrices.PriceValidFrom":
                                        if (Converter.ToDateTime(columnValue) != DateTime.MinValue)
                                        {
                                            price.ValidFrom = Converter.ToDateTime(columnValue);
                                        }
                                        break;
                                    case "EcomPrices.PriceValidTo":
                                        if (Converter.ToDateTime(columnValue) != DateTime.MinValue)
                                        {
                                            price.ValidTo = Converter.ToDateTime(columnValue);
                                        }
                                        break;
                                    case "EcomPrices.PriceIsWithVat":
                                        price.IsWithVat = Converter.ToBoolean(columnValue);
                                        break;
                                }
                            }
                        }
                    }
                }
                Price.SavePrices(new List<Price> { price });
            }
        }
        public void RemoveRowsNotInEndpoint()
        {
            if (_odataDestinationWriter.DeleteMissingRows)
            {
                foreach (string item in _odataDestinationWriter.ItemsToBeDeleted)
                {
                    Price price = _odataDestinationWriter.GetPriceById(item);
                    Price.DeletePrices(new List<Price> { price });
                    _odataDestinationWriter.Logger?.Info($"Detected that price on product {price.ProductId} for customer {price.UserCustomerNumber} is not part of the endpoint, and therefore is deleted.");
                }
            }
        }
        public void Close()
        {

        }

        public void Dispose()
        {
            Close();
        }
    }
}