using Dynamicweb.Core;
using Dynamicweb.DataIntegration.Integration;
using Dynamicweb.DataIntegration.Integration.Interfaces;
using Dynamicweb.DataIntegration.Providers.ODataProvider.Interfaces;
using Dynamicweb.Ecommerce.Stocks;
using System;
using System.Collections.Generic;
using System.Linq;

namespace Dynamicweb.DataIntegration.Providers.ODataProvider
{
    public class ODataStockAmountWriter : IDestinationWriter, IDisposable, IODataDestinationWriter
    {
        private readonly IProductVariantReader _variantReader;
        private readonly IStockLocationReader _locationReader;
        private readonly IProductUnitRelationWriter _unitWriter;
        private readonly IProductUnitRelationReader _unitReader;
        private readonly IProductReader _productReader;
        private ODataDestinationWriter _odataDestinationWriter;
        public Mapping Mapping { get; }
        internal ODataStockAmountWriter(ODataDestinationWriter odataDestinationWriter, IProductVariantReader variantReader, IStockLocationReader locationReader, IProductUnitRelationWriter unitWriter, IProductUnitRelationReader unitReader, IProductReader productReader)
        {
            _variantReader = variantReader;
            _locationReader = locationReader;
            _unitWriter = unitWriter;
            _unitReader = unitReader;
            _productReader = productReader;
            _odataDestinationWriter = odataDestinationWriter;
            Mapping = _odataDestinationWriter.Mapping;
        }
        public void Write(Dictionary<string, object> row)
        {
            if (row == null || !Mapping.Conditionals.CheckConditionals(row))
            {
                return;
            }
            var columnMappings = Mapping.GetColumnMappings();
            GetStockKeepingUnitInformation(row, columnMappings, out var locationCode, out var itemNo, out var variantCode, out var inventory);
            string variantID = itemNo + variantCode;
            var stockUnits = _unitReader.GetStockUnit(itemNo);
            var stockLocation = _locationReader.GetStockLocation(locationCode, _odataDestinationWriter.LanguageId);
            if (!string.IsNullOrEmpty(stockLocation?.GetName(_odataDestinationWriter.LanguageId)))
            {
                var variantOption = _variantReader.GetVariantOption(variantID);
                if (!string.IsNullOrEmpty(variantOption?.Id))
                {
                    var product = _productReader.GetProduct(itemNo);
                    List<StockUnit> stockUnit = stockUnits.Where(obj => obj.StockLocationId == stockLocation.ID && obj.VariantId == variantID && obj.UnitId == product.DefaultUnitId).ToList();
                    if (stockUnit.Count == 0)
                    {
                        StockUnit newStockUnit = new StockUnit
                        {
                            ProductId = itemNo,
                            Description = product.DefaultUnitId,
                            VariantId = variantID,
                            ProductNumber = itemNo,
                            UnitId = product.DefaultUnitId,
                            StockLocationId = stockLocation.ID,
                            StockQuantity = inventory
                        };
                        _unitWriter.SaveOrUpdate(newStockUnit);
                    }
                    else
                    {
                        foreach (var item in stockUnit)
                        {
                            item.StockQuantity = inventory;
                            _unitWriter.SaveOrUpdate(item);
                        }
                    }
                }
            }
        }
        private void GetStockKeepingUnitInformation(Dictionary<string, object> row, ColumnMappingCollection columnMappings, out string locationCode, out string itemNo, out string variantCode,
            out double inventory)
        {
            locationCode = "";
            itemNo = "";
            variantCode = "";
            inventory = 0;
            if (_odataDestinationWriter.ImportAll)
            {
                locationCode = row["Location_Code"].ToString();
                itemNo = row["Item_No"].ToString();
                variantCode = row["Variant_Code"].ToString();
                inventory = Converter.ToDouble(row["Inventory"]);
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
                                case "ObjectTypeStockAmountLocationCode":
                                    locationCode = columnValue;
                                    break;
                                case "ObjectTypeStockAmountItemNo":
                                    itemNo = columnValue;
                                    break;
                                case "ObjectTypeStockAmountVariantCode":
                                    variantCode = columnValue;
                                    break;
                                case "ObjectTypeStockAmountInventory":
                                    inventory = Converter.ToDouble(columnValue);
                                    break;
                            }
                        }
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

        public void RemoveRowsNotInEndpoint()
        {
        }
    }
}
