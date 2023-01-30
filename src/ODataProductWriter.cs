using Dynamicweb.Core;
using Dynamicweb.DataIntegration.Integration;
using Dynamicweb.DataIntegration.Integration.Interfaces;
using Dynamicweb.DataIntegration.Providers.ODataProvider.Interfaces;
using Dynamicweb.Ecommerce.Products;
using System;
using System.Collections.Generic;

namespace Dynamicweb.DataIntegration.Providers.ODataProvider
{
    public class ODataProductWriter : IDestinationWriter, IDisposable, IODataDestinationWriter
    {
        private readonly IProductWriter _productWriter;
        private readonly IProductReader _productReader;
        private readonly string _defaultShopId;
        private readonly Group _group;
        private readonly Dictionary<string, ProductField> _productFields;
        private ODataDestinationWriter _odataDestinationWriter;
        public Mapping Mapping { get; }
        internal ODataProductWriter(ODataDestinationWriter odataDestinationWriter, IProductWriter productWriter, IProductReader productReader, string defaultShopId, Group group)
        {
            _odataDestinationWriter = odataDestinationWriter;
            Mapping = _odataDestinationWriter.Mapping;
            _productWriter = productWriter;
            _productReader = productReader;
            _defaultShopId = defaultShopId;
            _group = group;
            _productFields = _productReader.GetCustomProductFields();
            if (_odataDestinationWriter.DeleteMissingRows)
            {
                _odataDestinationWriter.ItemsToBeDeleted = _odataDestinationWriter.GetAllProductIDs(_odataDestinationWriter.LanguageId, false, 360);
            }
        }

        public void Write(Dictionary<string, object> row)
        {
            if (row == null || !Mapping.Conditionals.CheckConditionals(row))
            {
                return;
            }
            var columnMappings = Mapping.GetColumnMappings();
            MapValuesToObject(row, columnMappings);
        }
        private void MapValuesToObject(Dictionary<string, object> row, ColumnMappingCollection columnMappings)
        {
            if (!row.TryGetValue("No", out var Code))
            {
                _odataDestinationWriter.Logger?.Error("Provided data does not contain a Product number ('No')");
            }
            else
            {
                string productCode = Code.ToString();
                var product = _productReader.GetProductByNumber(productCode, _odataDestinationWriter.LanguageId);
                bool productIsCombined = false;
                if (product != null && product.Id != product.Number)
                {
                    productIsCombined = true;
                }
                if (product == null || product.LanguageId != _odataDestinationWriter.LanguageId)
                {
                    product = _productReader.GetNewProduct();
                    product.DefaultShopId = _defaultShopId;
                    product.LanguageId = _odataDestinationWriter.LanguageId;
                }
                product.VariantId = "";
                product.Type = ProductType.Stock;
                if (_odataDestinationWriter.ImportAll)
                {
                    if (!productIsCombined)
                    {
                        product.Id = product.Number = row["No"].ToString();
                    }
                    product.Name = row["Description"].ToString();
                    product.EAN = row["GTIN"].ToString();
                    product.DefaultPrice = Converter.ToDouble(row["Unit_Price"]);
                    product.Weight = Converter.ToDouble(row["Gross_Weight"]);
                    product.DefaultUnitId = row["Base_Unit_of_Measure"].ToString();
                    product.Volume = Converter.ToDouble(row["Unit_Volume"]);
                    product.Cost = Converter.ToDouble(row["Unit_Cost"]);
                    product.PurchaseMinimumQuantity = Converter.ToDouble(row["Minimum_Order_Quantity"]);
                    product.ManufacturerId = row["Vendor_No"].ToString();
                    foreach (KeyValuePair<string, ProductField> item in _productFields)
                    {
                        if (row.TryGetValue(item.Value.SystemName, out var productFieldValue))
                        {
                            product.ProductFieldValues.GetProductFieldValue(item.Value.SystemName).Value = productFieldValue;
                        }
                    }
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
                                    case "EcomProducts.ProductNumber":
                                        if (!productIsCombined)
                                        {
                                            product.Id = product.Number = columnValue;
                                        }
                                        else
                                        {
                                            _odataDestinationWriter.Logger?.Info($"Product is a part of a combined product, so the id and number is not updated for product ({product.Number})");
                                        }
                                        break;
                                    case "EcomProducts.ProductName":
                                        product.Name = columnValue;
                                        break;
                                    case "EcomProducts.ProductDefaultUnitId":
                                        product.DefaultUnitId = columnValue;
                                        break;
                                    case "EcomProducts.ProductEAN":
                                        product.EAN = columnValue;
                                        break;
                                    case "EcomProducts.ProductPrice":
                                        product.DefaultPrice = Converter.ToDouble(columnValue);
                                        break;
                                    case "EcomProducts.ProductWeight":
                                        product.Weight = Converter.ToDouble(columnValue);
                                        break;
                                    case "EcomProducts.ProductVolume":
                                        product.Volume = Converter.ToDouble(columnValue);
                                        break;
                                    case "EcomProducts.ProductCost":
                                        product.Cost = Converter.ToDouble(columnValue);
                                        break;
                                    case "EcomProducts.ProductPurchaseMinimumQuantity":
                                        product.PurchaseMinimumQuantity = Converter.ToDouble(columnValue);
                                        break;
                                    case "EcomProducts.ProductManufacturerId":
                                        product.ManufacturerId = columnValue;
                                        break;
                                    case "EcomProducts.ProductActive":
                                        product.Active = Converter.ToBoolean(columnValue);
                                        break;
                                    default:
                                        foreach (KeyValuePair<string, ProductField> item in _productFields)
                                        {
                                            if (column.DestinationColumn.Name == "EcomProducts." + item.Value.SystemName)
                                            {
                                                product.ProductFieldValues.GetProductFieldValue(item.Value.SystemName).Value = columnValue;
                                                break;
                                            }
                                        }
                                        break;
                                }
                            }
                        }
                    }
                }
                if (row.TryGetValue("Blocked", out var productIsBlocked))
                {
                    product.Active = !Converter.ToBoolean(productIsBlocked);
                }
                _productWriter.SaveOrUpdate(product);
                if (_odataDestinationWriter.DeleteMissingRows)
                {
                    _odataDestinationWriter.ItemsToBeDeleted.Remove(product.Id);
                    _odataDestinationWriter.RowsToBeDeleted = _odataDestinationWriter.ItemsToBeDeleted.Count > 0;
                }
                if (_group != null)
                {
                    var productGroup = _productReader.GetProductGroup(product, _group);
                    if (productGroup == null)
                    {
                        _productWriter.SaveProductGroup(product, _group);
                    }
                }
                else if (row.TryGetValue("Item_Category_Code", out var groupID))
                {
                    var productGroup = _productReader.GetGroup(groupID.ToString(), _odataDestinationWriter.LanguageId);
                    if (productGroup != null)
                    {
                        _productWriter.SaveProductGroup(product, productGroup);
                    }
                }
            }
        }
        public void RemoveRowsNotInEndpoint()
        {
            if (_odataDestinationWriter.DeleteMissingRows)
            {
                foreach (string item in _odataDestinationWriter.ItemsToBeDeleted)
                {
                    Product product = _productReader.GetProduct(item);
                    _odataDestinationWriter.Logger?.Info($"Detected that Product {product.Name} ('{product.Number}') is not part of the endpoint, and therefore is deleted.");
                    _productWriter.DeleteProduct(item, "", _odataDestinationWriter.LanguageId);
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