using Dynamicweb.Core;
using Dynamicweb.DataIntegration.Integration;
using Dynamicweb.DataIntegration.Integration.Interfaces;
using Dynamicweb.DataIntegration.Providers.ODataProvider.Interfaces;
using Dynamicweb.DataIntegration.Providers.ODataProvider.Model;
using Dynamicweb.Ecommerce.Orders.Discounts;
using Dynamicweb.Ecommerce.Prices;
using Dynamicweb.Ecommerce.Products;
using Dynamicweb.Ecommerce.Stocks;
using Dynamicweb.Security.UserManagement;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;

namespace Dynamicweb.DataIntegration.Providers.ODataProvider
{
    public class ODataPriceAndDiscountWriter : IDestinationWriter, IDisposable, IODataDestinationWriter
    {
        private readonly string _shopId;
        private readonly string _discountNamePrefix;
        private readonly bool _mergeDiscounts;
        private readonly DiscountService _discountService;
        private readonly IProductUnitRelationReader _unitReader;
        private ODataDestinationWriter _odataDestinationWriter;
        private List<Discount> _allDiscounts = new List<Discount>();
        private Dictionary<int, string> _discountsToBeUpdated = new Dictionary<int, string>();
        private readonly ICurrencyReader _currencyReader;
        private string _defaultCurrencyCode;
        public List<string> _discountsToBeDeleted;
        public List<string> _pricesToBeDeleted;

        public Mapping Mapping { get; }
        internal ODataPriceAndDiscountWriter(ODataDestinationWriter odataDestinationWriter, ICurrencyReader currencyReader, string shopId, IProductUnitRelationReader unitReader,
            DiscountService discountService, string discountNamePrefix, string mergeDiscounts)
        {
            _shopId = shopId;
            _unitReader = unitReader;
            _discountService = discountService;
            _discountNamePrefix = discountNamePrefix;
            _mergeDiscounts = mergeDiscounts == "Yes";
            _odataDestinationWriter = odataDestinationWriter;
            Mapping = _odataDestinationWriter.Mapping;
            _currencyReader = currencyReader;
            _defaultCurrencyCode = _currencyReader.GetDefaultCurrency().Code;
            _discountService.ClearCache();
            if (_odataDestinationWriter.CheckIfDiscountExternalIdExistsInDatabase())
            {
                _allDiscounts = _discountService.GetDiscounts().Where(obj => obj.ShopId == _shopId && obj.LanguageId == _odataDestinationWriter.LanguageId && obj.ExternalId == _odataDestinationWriter.DatabaseDiscountExternalId).ToList();
            }
            else
            {
                _allDiscounts = _discountService.GetDiscounts().Where(obj => obj.ShopId == _shopId && obj.LanguageId == _odataDestinationWriter.LanguageId).ToList();
            }
            _discountsToBeUpdated = _allDiscounts.ToDictionary(obj => obj.ID, obj => obj.ProductsAndGroupsIds);
            if (_odataDestinationWriter.DeleteMissingRows)
            {
                _discountsToBeDeleted = _allDiscounts.Select(obj => obj.ID.ToString()).ToList();
                _pricesToBeDeleted = _odataDestinationWriter.GetAllSimplePriceByShopIdAndLanguageId(shopId, _odataDestinationWriter.LanguageId, 360).Select(obj => obj.Id.ToString()).ToList();
            }
        }
        public void Write(Dictionary<string, object> row)
        {
            var columnMappings = Mapping.GetColumnMappings();

            List<SalesPriceListLine> salesPriceListLines = _odataDestinationWriter.GetSalesPriceListLinesFromBC();
            var allGroups = Security.UserManagement.Group.GetGroups();
            var allUsers = Security.UserManagement.User.GetUsers();
            foreach (var salesPriceListLine in salesPriceListLines)
            {
                if (salesPriceListLine.Amount_Type.Contains("Price"))
                {
                    MapValuesToPrice(salesPriceListLine, columnMappings, allGroups, allUsers);
                }
                else if (salesPriceListLine.Amount_Type.Contains("Discount"))
                {
                    MapValuesToDiscount(salesPriceListLine, columnMappings, allGroups, allUsers);
                }
            }
        }

        private void MapValuesToPrice(SalesPriceListLine salesPriceListLine, ColumnMappingCollection columnMappings, Security.UserManagement.GroupCollection allGroups, UserCollection allUsers)
        {
            string variantId = salesPriceListLine.Variant_Code;
            if (!string.IsNullOrWhiteSpace(variantId))
            {
                variantId = salesPriceListLine.Product_No + variantId;
            }

            DateTime startingDate = salesPriceListLine.StartingDate;
            DateTime endingDate = salesPriceListLine.EndingDate;
            string unitofMeasureCode = salesPriceListLine.Unit_of_Measure_Code;
            double amount = salesPriceListLine.Unit_Price;
            double quantity = salesPriceListLine.Minimum_Quantity;
            bool isWithVat = Converter.ToBoolean(salesPriceListLine.PriceIncludesVAT);
            string salesCode = salesPriceListLine.AssignToNo;
            string etag = salesPriceListLine.odataetag;
            Price price;
            List<Price> existingPrice = _odataDestinationWriter.GetPriceByExternalId(etag);
            if (existingPrice.Any())
            {
                price = existingPrice[0];
                if (_odataDestinationWriter.DeleteMissingRows)
                {
                    _pricesToBeDeleted.Remove(price.Id.ToString());
                    _odataDestinationWriter.RowsToBeDeleted = _pricesToBeDeleted.Count > 0 || _discountsToBeDeleted.Count > 0;
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
            price.CurrencyCode = !string.IsNullOrWhiteSpace(salesPriceListLine.CurrencyCode) ? salesPriceListLine.CurrencyCode : _defaultCurrencyCode;

            var priceTypeIsGroup = allGroups.Where(obj => obj.Name == "Price_" + salesCode)?.FirstOrDefault();
            if (priceTypeIsGroup is object)
            {
                Security.UserManagement.Group group = allGroups.Where(obj => obj.Name == "Price_" + salesCode).FirstOrDefault();
                price.UserGroupId = group.ID.ToString();
            }
            else if (!string.IsNullOrEmpty(salesCode))
            {
                var priceTypeIsUser = allUsers.Where(obj => obj.CustomerNumber == salesCode)?.FirstOrDefault();
                if (priceTypeIsUser is object)
                {
                    price.UserCustomerNumber = salesCode;
                }
                else
                {
                    return;
                }
            }

            if (_odataDestinationWriter.ImportAll)
            {
                price.VariantId = variantId;
                if (startingDate != DateTime.MinValue)
                {
                    price.ValidFrom = startingDate;
                }
                if (endingDate != DateTime.MinValue)
                {
                    price.ValidTo = endingDate;
                }
                price.ShopId = _shopId;
                price.LanguageId = _odataDestinationWriter.LanguageId;
                price.ProductId = salesPriceListLine.Product_No;
                price.UnitId = unitofMeasureCode;
                price.Amount = amount;
                price.Quantity = quantity;
                price.IsWithVat = isWithVat;
            }
            else
            {
                foreach (var column in columnMappings)
                {
                    if (column.Active)
                    {
                        IList<PropertyInfo> props = new List<PropertyInfo>(salesPriceListLine.GetType().GetProperties());
                        PropertyInfo propertyInfo = props.Where(obj => obj.Name == column.SourceColumn.Name).FirstOrDefault();
                        if (propertyInfo != null)
                        {
                            var columnValue = BaseEndpointWriter.HandleScriptTypeForColumnMapping(column, propertyInfo.GetValue(salesPriceListLine, null));

                            switch (column.DestinationColumn.Name)
                            {
                                case "ObjectTypePriceAndDiscountProductNo":
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
                                case "ObjectTypePriceAndDiscountStartingDate":
                                    if (Converter.ToDateTime(columnValue) != DateTime.MinValue)
                                    {
                                        price.ValidFrom = Converter.ToDateTime(columnValue);
                                    }
                                    break;
                                case "ObjectTypePriceAndDiscountEndingDate":
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
        private void MapValuesToDiscount(SalesPriceListLine salesPriceListLine, ColumnMappingCollection columnMappings, Security.UserManagement.GroupCollection allGroups, UserCollection allUsers)
        {
            string variantId = salesPriceListLine.Variant_Code;
            if (!string.IsNullOrWhiteSpace(variantId))
            {
                variantId = "," + salesPriceListLine.Product_No + variantId;
            }
            else
            {
                variantId = ",";
            }
            string productToAdd = "[p:" + salesPriceListLine.Product_No + variantId + "]";
            DateTime startingDate = salesPriceListLine.StartingDate;
            DateTime endingDate = salesPriceListLine.EndingDate;
            string productsAndGroups = "[some]";
            double percentage = salesPriceListLine.Line_Discount_Percent;
            double scaleMinimumQuantity = salesPriceListLine.Minimum_Quantity;
            List<UnitOfMeasure> unitOfMeasures = _unitReader.GetUnitOfMeasures(salesPriceListLine.Product_No).Where(obj => obj.UnitId == salesPriceListLine.Unit_of_Measure_Code).ToList();
            if (unitOfMeasures.Any())
            {
                scaleMinimumQuantity *= unitOfMeasures[0].QuantityPerUnit;
            }
            string salesCode = salesPriceListLine.AssignToNo;
            Discount discount;
            List<Discount> existingDiscounts;
            if (_mergeDiscounts)
            {
                existingDiscounts = _allDiscounts.Where(obj => obj.ValidFrom == startingDate
                    && obj.ValidTo == endingDate
                    && obj.ShopId == _shopId
                    && obj.LanguageId == _odataDestinationWriter.LanguageId
                    && obj.Percentage == percentage
                    && obj.ProductQuantity == scaleMinimumQuantity).ToList();
            }
            else
            {
                existingDiscounts = _allDiscounts.Where(obj => obj.ValidFrom == startingDate
                    && obj.ValidTo == endingDate
                    && obj.ShopId == _shopId
                    && obj.LanguageId == _odataDestinationWriter.LanguageId
                    && obj.Percentage == percentage
                    && obj.ProductQuantity == scaleMinimumQuantity
                    && obj.ProductsAndGroupsIds.Equals(productsAndGroups + productToAdd)).ToList();
            }

            var discountTypeIsGroup = allGroups.Where(obj => obj.Name == "Disc_" + salesCode)?.FirstOrDefault();
            if (discountTypeIsGroup is object)
            {
                Security.UserManagement.Group group = allGroups.Where(obj => obj.Name == "Disc_" + salesCode).FirstOrDefault();
                int userGroupId = group.ID;
                existingDiscounts = existingDiscounts.Where(obj => obj.UserGroupId == userGroupId).ToList();
            }
            else if (!string.IsNullOrEmpty(salesCode))
            {
                User user = allUsers.Where(obj => obj.CustomerNumber == salesCode).FirstOrDefault();
                if (user is object)
                {
                    existingDiscounts = existingDiscounts.Where(obj => obj.UserCustomerNumber == salesCode).ToList();
                }
                else
                {
                    return;
                }
            }
            if (existingDiscounts.Any())
            {
                discount = existingDiscounts[0];
                _allDiscounts.Remove(discount);
                if (!discount.ProductsAndGroupsIds.Contains(productToAdd))
                {
                    discount.ProductsAndGroupsIds += productToAdd;
                }
                _allDiscounts.Add(discount);
                if (_odataDestinationWriter.DeleteMissingRows)
                {
                    _discountsToBeDeleted.Remove(discount.ID.ToString());
                    _odataDestinationWriter.RowsToBeDeleted = _discountsToBeDeleted.Count > 0 || _pricesToBeDeleted.Count > 0;
                }
                if (_discountsToBeUpdated.TryGetValue(discount.ID, out string productsAndGroupsIds))
                {
                    _discountsToBeUpdated[discount.ID] = productsAndGroupsIds.Replace(productToAdd, "");
                }
            }
            else
            {
                discount = new Discount
                {
                    DiscountType = DiscountTypes.Percentage,
                    ProductQuantification = ProductQuantification.Same,
                    DiscountApplyType = DiscountApplyType.OrderLineDiscount,
                    DiscountApplyToProduct = DiscountApplyToProduct.AllProducts,
                    Active = true,
                    ShopId = _shopId,
                    LanguageId = _odataDestinationWriter.LanguageId,
                    CountryCode2 = "",
                    ShippingId = "",
                    PaymentId = "",
                    OrderFieldName = "",
                    ExcludedProductsAndGroupsIds = "[some]",
                    Description = "DataIntegration import",
                    ProductIdByDiscount = "",
                    AmountProductFieldName = "",
                    ExtenderSettings = "",
                    OrderTotalPriceCurrencyCode = "",
                    ProductQuantity = scaleMinimumQuantity,
                    ProductsAndGroupsIds = productsAndGroups + productToAdd,
                    ExternalId = _odataDestinationWriter.DatabaseDiscountExternalId
                };
                discount.UserCustomerNumber = "";
                discount.UserGroupId = 0;
                if (discountTypeIsGroup is object)
                {
                    Security.UserManagement.Group group = allGroups.Where(obj => obj.Name == "Disc_" + salesCode).FirstOrDefault();
                    if (group is object)
                    {
                        discount.UserGroupId = group.ID;
                    }
                }
                else if (!string.IsNullOrEmpty(salesCode))
                {
                    User user = allUsers.Where(obj => obj.CustomerNumber == salesCode).FirstOrDefault();
                    discount.UserCustomerNumber = salesCode;
                }
                if (_odataDestinationWriter.ImportAll)
                {
                    if (startingDate != DateTime.MinValue)
                    {
                        discount.ValidFrom = startingDate;
                    }
                    if (endingDate != DateTime.MinValue)
                    {
                        discount.ValidTo = endingDate;
                    }
                    discount.Percentage = percentage;
                }
                else
                {
                    foreach (var column in columnMappings)
                    {
                        if (column.Active)
                        {
                            IList<PropertyInfo> props = new List<PropertyInfo>(salesPriceListLine.GetType().GetProperties());
                            PropertyInfo propertyInfo = props.Where(obj => obj.Name == column.SourceColumn.Name).FirstOrDefault();
                            if (propertyInfo != null)
                            {
                                var columnValue = BaseEndpointWriter.HandleScriptTypeForColumnMapping(column, propertyInfo.GetValue(salesPriceListLine, null));

                                switch (column.DestinationColumn.Name)
                                {
                                    case "ObjectTypePriceAndDiscountStartingDate":
                                        if (Converter.ToDateTime(columnValue) != DateTime.MinValue)
                                        {
                                            discount.ValidFrom = Converter.ToDateTime(columnValue);
                                        }
                                        break;
                                    case "EcomDiscount.DiscountPercentage":
                                        discount.Percentage = Converter.ToDouble(columnValue);
                                        break;
                                    case "ObjectTypePriceAndDiscountEndingDate":
                                        if (Converter.ToDateTime(columnValue) != DateTime.MinValue)
                                        {
                                            discount.ValidTo = Converter.ToDateTime(columnValue);
                                        }
                                        break;
                                }
                            }
                        }
                    }
                    discount.Name = $"{_discountNamePrefix} {discount.Percentage}%";
                }
                if (!_allDiscounts.Contains(discount))
                {
                    _allDiscounts.Add(discount);
                }
            }
            _discountService.Save(discount);
        }
        public void RemoveRowsNotInEndpoint()
        {
            if (_odataDestinationWriter.DeleteMissingRows)
            {
                foreach (string item in _pricesToBeDeleted)
                {
                    Price price = _odataDestinationWriter.GetPriceById(item);
                    Price.DeletePrices(new List<Price> { price });
                    _odataDestinationWriter.Logger?.Info($"Detected that price on product {price.ProductId} for customer {price.UserCustomerNumber} is not part of the endpoint, and therefore is deleted.");
                }

                foreach (string item in _discountsToBeDeleted)
                {
                    Discount discount = _discountService.GetDiscount(Convert.ToInt32(item));
                    _discountService.Delete(discount.ID);
                    _odataDestinationWriter.Logger?.Info($"Detected that Discount {discount.Name} ('{discount.ID}') is not part of the endpoint, and therefore is deleted.");
                }
                foreach (KeyValuePair<int, string> discountsToBeUpdated in _discountsToBeUpdated)
                {
                    Discount discount = _discountService.GetDiscount(discountsToBeUpdated.Key);
                    if (!string.IsNullOrWhiteSpace(discountsToBeUpdated.Value.Replace("[some]", "")))
                    {
                        foreach (string productStrings in discountsToBeUpdated.Value.Split(']'))
                        {
                            if (productStrings != "[some" && !string.IsNullOrWhiteSpace(productStrings))
                            {
                                discount.ProductsAndGroupsIds = discount.ProductsAndGroupsIds.Replace(productStrings + "]", "");
                            }
                        }
                        _discountService.Save(discount);
                    }
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
