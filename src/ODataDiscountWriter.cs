using Dynamicweb.Core;
using Dynamicweb.DataIntegration.Integration;
using Dynamicweb.DataIntegration.Integration.Interfaces;
using Dynamicweb.DataIntegration.Providers.ODataProvider.Interfaces;
using Dynamicweb.DataIntegration.Providers.ODataProvider.Model;
using Dynamicweb.Ecommerce.Orders.Discounts;
using Dynamicweb.Ecommerce.Products;
using Dynamicweb.Ecommerce.Stocks;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;

namespace Dynamicweb.DataIntegration.Providers.ODataProvider
{
    public class ODataDiscountWriter : IDestinationWriter, IDisposable, IODataDestinationWriter
    {
        private readonly string _shopId;
        private readonly string _discountNamePrefix;
        private readonly bool _mergeDiscounts;
        private readonly DiscountService _discountService;
        private readonly IProductUnitRelationReader _unitReader;
        private ODataDestinationWriter _odataDestinationWriter;
        private List<Discount> _allDiscounts = new List<Discount>();
        private Dictionary<int, string> _discountsToBeUpdated = new Dictionary<int, string>();
        private bool _salesTypeCustomer;
        private bool _salesTypeAllCustomer;
        private bool _salesTypeCustomerDiscountGroup;
        public Mapping Mapping { get; }
        internal ODataDiscountWriter(ODataDestinationWriter odataDestinationWriter, IProductUnitRelationReader unitReader,
            DiscountService discountService, string shopId, string discountNamePrefix, string mergeDiscounts, bool salesTypeCustomer, bool salesTypeAllCustomer, bool salesTypeCustomerDiscountGroup)
        {
            _shopId = shopId;
            _unitReader = unitReader;
            _discountService = discountService;
            _discountNamePrefix = discountNamePrefix;
            _mergeDiscounts = mergeDiscounts == "Yes";
            _odataDestinationWriter = odataDestinationWriter;
            Mapping = _odataDestinationWriter.Mapping;
            _salesTypeCustomer = salesTypeCustomer;
            _salesTypeAllCustomer = salesTypeAllCustomer;
            _salesTypeCustomerDiscountGroup = salesTypeCustomerDiscountGroup;
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
                _odataDestinationWriter.ItemsToBeDeleted = _allDiscounts.Select(obj => obj.ID.ToString()).ToList();
            }
        }
        public void Write(Dictionary<string, object> row)
        {
            var columnMappings = Mapping.GetColumnMappings();
            if (_salesTypeCustomer)
            {
                MapValuesToObject(_odataDestinationWriter.GetSalesLineDiscountsFromBC("Customer"), columnMappings, true, false);
            }
            if (_salesTypeAllCustomer)
            {
                MapValuesToObject(_odataDestinationWriter.GetSalesLineDiscountsFromBC("All Customers"), columnMappings, false, false);
            }
            if (_salesTypeCustomerDiscountGroup)
            {
                MapValuesToObject(_odataDestinationWriter.GetSalesLineDiscountsFromBC("Customer Disc. Group"), columnMappings, false, true);
            }
        }

        private void MapValuesToObject(List<SalesLineDiscount> salesLineDiscount, ColumnMappingCollection columnMappings, bool salesTypeCustomer, bool salesTypeCustomerDiscountGroup)
        {
            foreach (SalesLineDiscount item in salesLineDiscount)
            {
                string variantId = item.Variant_Code;
                if (!string.IsNullOrWhiteSpace(variantId))
                {
                    variantId = "," + item.Code + variantId;
                }
                else
                {
                    variantId = ",";
                }
                string productToAdd = "[p:" + item.Code + variantId + "]";
                string startingDate = item.Starting_Date;
                string endingDate = item.Ending_Date;
                string productsAndGroups = "[some]";
                double percentage = item.Line_Discount_Percent;
                double scaleMinimumQuantity = item.Minimum_Quantity;
                List<UnitOfMeasure> unitOfMeasures = _unitReader.GetUnitOfMeasures(item.Code).Where(obj => obj.UnitId == item.Unit_of_Measure_Code).ToList();
                if (unitOfMeasures.Any())
                {
                    scaleMinimumQuantity *= unitOfMeasures[0].QuantityPerUnit;
                }
                string salesCode = item.SalesCode;
                Discount discount;
                List<Discount> existingDiscounts;
                if (_mergeDiscounts)
                {
                    existingDiscounts = _allDiscounts.Where(obj => obj.ValidFrom == Converter.ToDateTime(startingDate)
                        && obj.ValidTo == Convert.ToDateTime(endingDate)
                        && obj.ShopId == _shopId
                        && obj.LanguageId == _odataDestinationWriter.LanguageId
                        && obj.Percentage == percentage
                        && obj.ProductQuantity == scaleMinimumQuantity).ToList();
                }
                else
                {
                    existingDiscounts = _allDiscounts.Where(obj => obj.ValidFrom == Converter.ToDateTime(startingDate)
                        && obj.ValidTo == Convert.ToDateTime(endingDate)
                        && obj.ShopId == _shopId
                        && obj.LanguageId == _odataDestinationWriter.LanguageId
                        && obj.Percentage == percentage
                        && obj.ProductQuantity == scaleMinimumQuantity
                        && obj.ProductsAndGroupsIds.Equals(productsAndGroups + productToAdd)).ToList();
                }
                if (salesTypeCustomer)
                {
                    existingDiscounts = existingDiscounts.Where(obj => obj.UserCustomerNumber == salesCode).ToList();
                }
                else if (salesTypeCustomerDiscountGroup)
                {
                    Security.UserManagement.Group group = Security.UserManagement.Group.GetGroups().Where(obj => obj.Name == "Disc_" + salesCode).FirstOrDefault();
                    if (group is object)
                    {
                        int userGroupId = group.ID;
                        existingDiscounts = existingDiscounts.Where(obj => obj.UserGroupId == userGroupId).ToList();
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
                        _odataDestinationWriter.ItemsToBeDeleted.Remove(discount.ID.ToString());
                        _odataDestinationWriter.RowsToBeDeleted = _odataDestinationWriter.ItemsToBeDeleted.Count > 0;
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
                    if (salesTypeCustomer)
                    {
                        discount.UserCustomerNumber = salesCode;
                    }
                    else if (salesTypeCustomerDiscountGroup)
                    {
                        Security.UserManagement.Group group = Security.UserManagement.Group.GetGroups().Where(obj => obj.Name == "Disc_" + salesCode).FirstOrDefault();
                        if (group is object)
                        {
                            discount.UserGroupId = group.ID;
                        } 
                    }
                    if (_odataDestinationWriter.ImportAll)
                    {
                        discount.ValidFrom = Converter.ToDateTime(item.Starting_Date);
                        discount.Percentage = percentage;
                        discount.ValidTo = Converter.ToDateTime(item.Ending_Date);
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
                                        case "EcomDiscount.DiscountValidFrom":
                                            discount.ValidFrom = Converter.ToDateTime(columnValue);
                                            break;
                                        case "EcomDiscount.DiscountPercentage":
                                            discount.Percentage = Converter.ToDouble(columnValue);
                                            break;
                                        case "EcomDiscount.DiscountValidTo":
                                            discount.ValidTo = Converter.ToDateTime(columnValue);
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
        }
        public void RemoveRowsNotInEndpoint()
        {
            if (_odataDestinationWriter.DeleteMissingRows)
            {
                foreach (string item in _odataDestinationWriter.ItemsToBeDeleted)
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
