using Dynamicweb.Core;
using Dynamicweb.DataIntegration.Integration;
using Dynamicweb.DataIntegration.Integration.Interfaces;
using Dynamicweb.DataIntegration.Providers.ODataProvider.Interfaces;
using Dynamicweb.DataIntegration.Providers.ODataProvider.Model;
using Dynamicweb.Ecommerce.International;
using Dynamicweb.Ecommerce.Orders;
using Dynamicweb.Security.UserManagement;
using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Reflection;

namespace Dynamicweb.DataIntegration.Providers.ODataProvider
{
    public class ODataOrderWriter : IDestinationWriter, IDisposable, IODataDestinationWriter
    {
        private readonly string _shopId;
        private string _anonymousUser;
        private ODataDestinationWriter _odataDestinationWriter;
        private bool _firstRunOfImport;
        private readonly Mapping _ordelineMapping;
        private readonly StockLocationReader _stockLocationReader;
        private readonly CurrencyService _currencyService;
        private readonly CountryService _countryService;
        private readonly LanguageService _languageService;

        public Mapping Mapping { get; }
        internal ODataOrderWriter(ODataDestinationWriter odataDestinationWriter, StockLocationReader stockLocationReader,
            CurrencyService currencyService, CountryService countryService, LanguageService languageService,
            string shopId, string anonymousUser, Mapping ordelineMapping)
        {
            _shopId = shopId;
            _anonymousUser = anonymousUser;
            _odataDestinationWriter = odataDestinationWriter;
            Mapping = _odataDestinationWriter.Mapping;
            _firstRunOfImport = true;
            _ordelineMapping = ordelineMapping;
            _stockLocationReader = stockLocationReader;
            _currencyService = currencyService;
            _countryService = countryService;
            _languageService = languageService;
        }
        public void Write(Dictionary<string, object> row)
        {
            if (row == null || !Mapping.Conditionals.CheckConditionals(row))
            {
                return;
            }
            var orderHeaderColumnMappings = Mapping.GetColumnMappings();
            if (!row.TryGetValue("No", out var Code))
            {
                _odataDestinationWriter.Logger?.Error("Provided data does not contain a Product number ('No')");
                return;
            }
            else if (row["Sell_to_Customer_No"].ToString() == _anonymousUser)
            {
                return;
            }
            else
            {
                bool orderComesFromDW = false;
                Order order = null;
                if (row.TryGetValue("Pre_Assigned_No", out var Pre_Assigned_No))
                {
                    order = _odataDestinationWriter.GetOrderByOrderIntegrationOrderId(Pre_Assigned_No.ToString());
                }
                if (order == null && row.TryGetValue("External_Document_No", out var External_Document_No))
                {
                    order = _odataDestinationWriter.GetOrderByExternalDocumentNo(External_Document_No.ToString());
                    orderComesFromDW = order != null;
                }
                if (order == null && row.TryGetValue("Order_No", out var Order_No))
                {
                    order = _odataDestinationWriter.GetOrderByOrderIntegrationOrderId(Order_No.ToString());
                }
                if (order == null && row.TryGetValue("No", out var No))
                {
                    order = _odataDestinationWriter.GetOrderByOrderIntegrationOrderId(No.ToString());
                }
                if (_odataDestinationWriter.DeleteMissingRows && _firstRunOfImport)
                {
                    _odataDestinationWriter.ItemsToBeDeleted = _odataDestinationWriter.GetAllOrderIDsFormShopAndLedgerEntry(_shopId, row.TryGetValue("Order_No", out var orderIsLedgerEntry), 360);
                    _firstRunOfImport = false;
                }
                MapValuesToObject(row, orderHeaderColumnMappings, order, Code.ToString(), orderComesFromDW);
            }
        }
        private void MapValuesToObject(Dictionary<string, object> row, ColumnMappingCollection columnMappings, Order order, string documentNo, bool orderComesFromDW)
        {
            Currency currency;
            if (string.IsNullOrWhiteSpace(row["Currency_Code"].ToString()))
            {
                currency = _currencyService.GetDefaultCurrency();
            }
            else
            {
                currency = _currencyService.GetCurrency(row["Currency_Code"].ToString());
            }
            Country country = _countryService.GetCountries().Where(obj => obj.CurrencyCode == currency.Code).FirstOrDefault();
            Language language = _languageService.GetLanguage(_odataDestinationWriter.LanguageId);
            if (order is null)
            {
                User user = null;
                if (row.TryGetValue("Sell_to_Contact_No", out var contactNo))
                {
                    user = _odataDestinationWriter.GetUserByExternalID(contactNo.ToString());
                }
                if (user == null)
                {
                    user = _odataDestinationWriter.GetUserByExternalID(row["Sell_to_Customer_No"].ToString());
                }
                if (user == null)
                {
                    return;
                }
                order = new Order(currency, country, language)
                {
                    CustomerAccessUserId = user.ID,
                    CustomerNumber = user.CustomerNumber,
                    CustomerCompany = user.Company,
                    CustomerCountry = user.Country,
                    CustomerCell = user.PhoneMobile,
                    CustomerTitle = user.Title,
                    Complete = !orderComesFromDW,
                    IsLedgerEntry = orderComesFromDW,
                    CustomerAccessUserUserName = user.UserName,
                    CustomerCountryCode = user.CountryCode,
                    Ip = "ERP Import",
                    IsExported = false,
                    CheckoutPageId = -1,
                };
            }
            order.ShopId = _shopId;
            List<OrderLines> orderLines = _odataDestinationWriter.GetOrderLinesFromBC(documentNo, order.IsLedgerEntry);
            if (orderLines.Count == 0)
            {
                _odataDestinationWriter.Logger?.Info($"Skipped order {documentNo} because it had no order lines");
                return;
            }
            if (_odataDestinationWriter.ImportAll)
            {
                order.CustomerName = order.DeliveryName = row["Sell_to_Customer_Name"].ToString();
                order.CustomerAddress = row["Bill_to_Address"].ToString();
                order.CustomerAddress2 = row["Bill_to_Address_2"].ToString();
                order.CustomerZip = row["Bill_to_Post_Code"].ToString();
                order.CustomerCity = row["Bill_to_City"].ToString();
                order.CustomerPhone = row["Sell_to_Phone_No"].ToString();
                order.CustomerEmail = row["Sell_to_E_Mail"].ToString();
                order.DeliveryAddress = row["Sell_to_Address"].ToString();
                order.DeliveryAddress2 = row["Sell_to_Address_2"].ToString();
                order.DeliveryZip = row["Sell_to_Post_Code"].ToString();
                order.DeliveryCity = row["Sell_to_City"].ToString();
                order.DeliveryCountryCode = row["Sell_to_Country_Region_Code"].ToString();
                order.DeliveryCountry = row[""].ToString();
                order.DiscountPercentage = Converter.ToDouble(row["Payment_Discount_Percent"]);
                order.IntegrationOrderId = row["No"].ToString();
                order.CompletedDate = Converter.ToDateTime(row["Order_Date"]);
                order.Date = Converter.ToDateTime(row["Order_Date"]);
            }
            else
            {
                foreach (var column in columnMappings)
                {
                    if (column.Active)
                    {
                        if (row.ContainsKey(column.SourceColumn.Name))
                        {
                            var columnValue = BaseEndpointWriter.HandleScriptTypeForColumnMapping(column, row[column.SourceColumn.Name]);

                            switch (column.DestinationColumn.Name)
                            {
                                case "EcomOrders.OrderCustomerName":
                                    order.CustomerName = columnValue;
                                    order.DeliveryName = columnValue;
                                    break;
                                case "EcomOrders.OrderCustomerAddress":
                                    order.CustomerAddress = columnValue;
                                    break;
                                case "EcomOrders.OrderCustomerAddress2":
                                    order.CustomerAddress2 = columnValue;
                                    break;
                                case "EcomOrders.OrderCustomerZip":
                                    order.CustomerZip = columnValue;
                                    break;
                                case "EcomOrders.OrderCustomerCity":
                                    order.CustomerCity = columnValue;
                                    break;
                                case "ObjectTypeOrderSelltoPhoneNo":
                                    order.CustomerPhone = columnValue;
                                    break;
                                case "ObjectTypeOrderSelltoEMail":
                                    order.CustomerEmail = columnValue;
                                    break;
                                case "EcomOrders.OrderDeliveryAddress":
                                    order.DeliveryAddress = columnValue;
                                    break;
                                case "EcomOrders.OrderDeliveryAddress2":
                                    order.DeliveryAddress2 = columnValue;
                                    break;
                                case "EcomOrders.OrderDeliveryZip":
                                    order.DeliveryZip = columnValue;
                                    break;
                                case "EcomOrders.OrderDeliveryCity":
                                    order.DeliveryCity = columnValue;
                                    break;
                                case "EcomOrders.OrderCustomerCountryCode":
                                    order.DeliveryCountryCode = columnValue;
                                    order.DeliveryCountry = _odataDestinationWriter.GetCountryDisplayName(columnValue);
                                    break;
                                case "EcomOrders.OrderDiscountPercentage":
                                    order.DiscountPercentage = Converter.ToDouble(columnValue);
                                    break;
                                case "ObjectTypeOrderNo":
                                    order.IntegrationOrderId = columnValue;
                                    break;
                                case "ObjectTypeOrderOrderDate":
                                    order.CompletedDate = Convert.ToDateTime(columnValue);
                                    order.Date = Convert.ToDateTime(columnValue);
                                    break;
                                case "ObjectTypeOrderPostingDate":
                                    order.CompletedDate = Convert.ToDateTime(columnValue);
                                    order.Date = Convert.ToDateTime(columnValue);
                                    break;
                                default:
                                    foreach (var item in order.OrderFieldValues)
                                    {
                                        if ("EcomOrders." + item.OrderField.SystemName == column.DestinationColumn.Name)
                                        {
                                            item.Value = columnValue;
                                        }
                                    }
                                    break;
                            }
                        }
                    }
                }
            }
            _odataDestinationWriter.OrderService.Save(order);
            IList<OrderLine> ordersOrderLines = order.OrderLines;
            for (int i = 0; i < ordersOrderLines.Count(); i++)
            {
                order.OrderLines.RemoveLine(ordersOrderLines[i].Id);
            }
            bool pricesIncludingVAT = Converter.ToBoolean(row["Prices_Including_VAT"]);
            var orderLineColumnMappings = _ordelineMapping.GetColumnMappings();
            double PriceWithoutVAT = 0;
            double VAT = 0;
            double PriceWithVAT = 0;
            foreach (var item in orderLines)
            {
                OrderLine orderLine = MapValuesToOrderline(orderLineColumnMappings, item, order, pricesIncludingVAT, order.VatCountry);
                PriceWithoutVAT += orderLine.Price.PriceWithoutVAT;
                VAT += orderLine.Price.VAT;
                PriceWithVAT += orderLine.Price.PriceWithVAT;
                order.OrderLines.Add(orderLine);
            }
            order = _odataDestinationWriter.GetOrderByOrderIntegrationOrderId(order.IntegrationOrderId);
            order.OrderLines.Save(order.Id);
            order.Price.PriceWithoutVAT = PriceWithoutVAT;
            order.Price.VAT = VAT;
            order.Price.PriceWithVAT = PriceWithVAT;
            order.Price.VATPercent = order.VatCountry.Vat;
            order.PriceBeforeFees.PriceWithoutVAT = PriceWithoutVAT;
            order.PriceBeforeFees.VAT = VAT;
            order.PriceBeforeFees.PriceWithVAT = PriceWithVAT;
            order.PriceBeforeFees.VATPercent = order.VatCountry.Vat;
            _odataDestinationWriter.OrderService.Save(order);
            if (_odataDestinationWriter.DeleteMissingRows)
            {
                order = _odataDestinationWriter.GetOrderByOrderIntegrationOrderId(order.IntegrationOrderId);
                _odataDestinationWriter.ItemsToBeDeleted.Remove(order.Id);
                _odataDestinationWriter.RowsToBeDeleted = _odataDestinationWriter.ItemsToBeDeleted.Count > 0;
            }
        }
        private OrderLine MapValuesToOrderline(ColumnMappingCollection columnMappings, OrderLines orderLines, Order order, bool pricesIncludingVAT, Country country)
        {
            OrderLine result = new OrderLine(order)
            {
                AllowOverridePrices = true,
                ParentLineId = "",
                Reference = "",
                BomItemId = "",
                DiscountId = "",
                GiftCardCode = ""
            };
            if (_odataDestinationWriter.ImportAll)
            {
                result.ProductNumber = orderLines.No;
                result.ProductId = orderLines.No;
                result.ProductName = orderLines.Description;
                if (!string.IsNullOrWhiteSpace(Converter.ToString(orderLines.Variant_Code)))
                {
                    result.ProductVariantText = orderLines.Description;
                }
                else
                {
                    result.ProductVariantText = "";
                }
                result.Price.VAT = orderLines.Total_VAT_Amount;
                result.UnitPrice.VAT = orderLines.Total_VAT_Amount / orderLines.Quantity;
                result.UnitId = orderLines.Unit_of_Measure_Code;
                result.Price.PriceWithoutVAT = orderLines.Line_Amount;
                result.UnitPrice.PriceWithoutVAT = orderLines.Line_Amount / orderLines.Quantity;
                result.TotalDiscount.PriceWithoutVAT = orderLines.Line_Discount_Amount;
                result.TotalDiscount.VATPercent = orderLines.Line_Discount_Percent;
                result.UnitPrice.PriceWithoutVAT = orderLines.Unit_Price;
                if (!string.IsNullOrWhiteSpace(orderLines.Location_Code))
                {
                    result.StockLocationId = _stockLocationReader.GetStockLocation(orderLines.Location_Code, _odataDestinationWriter.LanguageId).ID;
                }
                if (!string.IsNullOrWhiteSpace(Converter.ToString(orderLines.Variant_Code)))
                {
                    result.ProductVariantId = orderLines.No + orderLines.Variant_Code;
                    result.ProductVariantText = orderLines.Description;
                }
                else
                {
                    result.ProductVariantId = "";
                    result.ProductVariantText = "";
                }
                result.Quantity = orderLines.Quantity;
            }
            else
            {
                foreach (var column in columnMappings)
                {
                    if (column.Active)
                    {
                        IList<PropertyInfo> props = new List<PropertyInfo>(orderLines.GetType().GetProperties());
                        PropertyInfo propertyInfo = props.Where(obj => obj.Name == column.SourceColumn.Name).FirstOrDefault();
                        if (propertyInfo != null)
                        {
                            var columnValue = BaseEndpointWriter.HandleScriptTypeForColumnMapping(column, propertyInfo.GetValue(orderLines, null));
                            switch (column.DestinationColumn.Name)
                            {
                                case "ObjectTypeOrderLinesNo":
                                    result.ProductNumber = columnValue;
                                    result.ProductId = columnValue;
                                    break;
                                case "ObjectTypeOrderLinesDescription":
                                    result.ProductName = columnValue;
                                    if (!string.IsNullOrWhiteSpace(Converter.ToString(orderLines.Variant_Code)))
                                    {
                                        result.ProductVariantText = columnValue;
                                    }
                                    else
                                    {
                                        result.ProductVariantText = "";
                                    }
                                    break;
                                case "ObjectTypeOrderLinesTotalVATAmount":
                                    result.Price.VAT = Converter.ToDouble(columnValue);
                                    result.UnitPrice.VAT = Converter.ToDouble(columnValue) / orderLines.Quantity;
                                    break;
                                case "EcomOrderLines.OrderLineUnitId":
                                    result.UnitId = columnValue;
                                    break;
                                case "ObjectTypeOrderLinesLineAmount":
                                    result.Price.PriceWithoutVAT = Converter.ToDouble(columnValue);
                                    result.UnitPrice.PriceWithoutVAT = Converter.ToDouble(columnValue) / orderLines.Quantity;
                                    break;
                                case "ObjectTypeOrderLinesLineDiscountAmount":
                                    result.TotalDiscount.PriceWithoutVAT = Converter.ToDouble(columnValue);
                                    break;
                                case "ObjectTypeOrderLinesLineDiscountPercent":
                                    result.TotalDiscount.VATPercent = Converter.ToDouble(columnValue);
                                    break;
                                case "ObjectTypeOrderLinesUnitPrice":
                                    result.UnitPrice.PriceWithoutVAT = Converter.ToDouble(columnValue);
                                    break;
                                case "EcomOrderLines.OrderLineStockLocationId":
                                    if (!string.IsNullOrWhiteSpace(columnValue))
                                    {
                                        result.StockLocationId = _stockLocationReader.GetStockLocation(columnValue, _odataDestinationWriter.LanguageId).ID;
                                    }
                                    break;
                                case "ObjectTypeOrderLinesVariantCode":
                                    if (!string.IsNullOrWhiteSpace(columnValue))
                                    {
                                        result.ProductVariantId = orderLines.No + columnValue;
                                        result.ProductVariantText = orderLines.Description;
                                    }
                                    else
                                    {
                                        result.ProductVariantId = "";
                                        result.ProductVariantText = "";
                                    }
                                    break;
                                case "ObjectTypeOrderLinesQuantity":
                                    result.Quantity = Converter.ToDouble(columnValue);
                                    break;
                                default:
                                    foreach (var item in result.OrderLineFieldValues)
                                    {
                                        if ("EcomOrders." + item.OrderLineFieldSystemName == column.DestinationColumn.Name)
                                        {
                                            item.Value = columnValue;
                                        }
                                    }
                                    break;
                            }
                        }
                    }
                }
            }
            if (!pricesIncludingVAT)
            {
                result.UnitPrice.PriceWithoutVAT = orderLines.Unit_Price;
                result.UnitPrice.VAT = orderLines.Unit_Price / 100 * country.Vat;
                result.UnitPrice.PriceWithVAT = orderLines.Unit_Price + orderLines.Unit_Price / 100 * country.Vat;
                result.UnitPrice.VATPercent = country.Vat;
                result.Price.PriceWithoutVAT = orderLines.Unit_Price * orderLines.Quantity;
                result.Price.VAT = orderLines.Unit_Price / 100 * country.Vat * orderLines.Quantity;
                result.Price.PriceWithVAT = (orderLines.Unit_Price + orderLines.Unit_Price / 100 * country.Vat) * orderLines.Quantity;
                result.Price.VATPercent = country.Vat;
            }
            return result;
        }
        public void RemoveRowsNotInEndpoint()
        {
            if (_odataDestinationWriter.DeleteMissingRows)
            {
                foreach (string item in _odataDestinationWriter.ItemsToBeDeleted)
                {
                    Order order = _odataDestinationWriter.OrderService.GetById(item);
                    _odataDestinationWriter.OrderService.Delete(item);
                    _odataDestinationWriter.Logger?.Info($"Detected that Order {order.Id} ('{order.IntegrationOrderId}') is not part of the endpoint, and therefore is deleted.");
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
