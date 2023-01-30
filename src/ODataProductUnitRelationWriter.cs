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
    public class ODataProductUnitRelationWriter : IDestinationWriter, IDisposable, IODataDestinationWriter
    {
        private readonly IProductUnitRelationWriter _unitWriter;
        private readonly IProductUnitRelationReader _unitReader;
        private readonly IProductVariantReader _variantReader;
        private readonly IProductReader _productReader;
        private ODataDestinationWriter _odataDestinationWriter;

        public Mapping Mapping { get; }

        internal ODataProductUnitRelationWriter(ODataDestinationWriter odataDestinationWriter, IProductUnitRelationWriter unitWriter, IProductUnitRelationReader unitReader, IProductVariantReader variantReader, IProductReader productReader)
        {
            _unitWriter = unitWriter;
            _unitReader = unitReader;
            _variantReader = variantReader;
            _productReader = productReader;
            _odataDestinationWriter = odataDestinationWriter;
            Mapping = _odataDestinationWriter.Mapping;
        }

        public void Write(Dictionary<string, object> row)
        {
            if (!Mapping.Conditionals.CheckConditionals(row))
            {
                return;
            }
            var columnMappings = Mapping.GetColumnMappings();
            GetItemUnitVariantInformation(row, columnMappings, out string productCode, out string unitVariantCode,
            out double Qty_per_Unit_of_Measure);
            var variantOption = _unitReader.GetVariant(unitVariantCode);
            if (variantOption != null)
            {
                var stockLocations = _unitReader.GetStockLocations();
                foreach (var stockLocationsItems in stockLocations)
                {
                    var variantOptionList = _variantReader.GetVariantOptionForProduct(productCode);
                    if (variantOptionList != null)
                    {
                        string modifyedID = productCode.Replace("-", "");
                        variantOptionList = variantOptionList.Where(obj => obj.GroupId == modifyedID).ToList();
                        foreach (var variantOptionListItems in variantOptionList)
                        {
                            if (variantOptionListItems.GetName(_odataDestinationWriter.LanguageId) != "Master")
                            {
                                SaveOrUpdateStockUnit(productCode, Qty_per_Unit_of_Measure, unitVariantCode);
                            }
                        }
                        if (variantOptionList.Count() == 0)
                        {
                            SaveOrUpdateStockUnit(productCode, Qty_per_Unit_of_Measure, unitVariantCode);
                        }
                    }
                }
                if (stockLocations.Count() == 0)
                {
                    _odataDestinationWriter.Logger?.Error("There is no StockLocation added in the database, all records is skipped.");
                }
            }
            else
            {
                _odataDestinationWriter.Logger?.Info("Variantoption: " + unitVariantCode + " does not exists in the database, and is skipped.");
            }
        }

        private void SaveOrUpdateStockUnit(string productCode, double qty_per_Unit_of_Measure, string unitId)
        {
            var product = _productReader.GetProduct(productCode);

            List<UnitOfMeasure> unitOfMeasures = _unitReader.GetUnitOfMeasures(productCode).Where(obj => obj.UnitId == unitId).ToList();
            int autoId = unitOfMeasures.Count > 0 ? unitOfMeasures[0].AutoId : 0;
            UnitOfMeasure unitOfMeasure = new UnitOfMeasure
            {
                ProductId = productCode,
                UnitId = unitId,
                QuantityPerUnit = qty_per_Unit_of_Measure,
                AutoId = autoId,
                IsBaseUnit = product.DefaultUnitId == unitId
            };
            _unitWriter.SaveUnitOfMeasure(unitOfMeasure);
        }

        private void GetItemUnitVariantInformation(Dictionary<string, object> row, ColumnMappingCollection columnMappings, out string productCode, out string unitVariantCode,
            out double Qty_per_Unit_of_Measure)
        {
            productCode = null;
            unitVariantCode = null;
            Qty_per_Unit_of_Measure = 0.0;
            if (_odataDestinationWriter.ImportAll)
            {
                productCode = row["Item_No"].ToString();
                unitVariantCode = row["Code"].ToString();
                Qty_per_Unit_of_Measure = Converter.ToDouble(row["Qty_per_Unit_of_Measure"]);
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
                                case "EcomStockUnit.StockUnitProductId":
                                    productCode = columnValue;
                                    break;
                                case "EcomStockUnit.StockUnitDescription":
                                    unitVariantCode = columnValue;
                                    break;
                                case "EcomStockUnit.StockUnitQuantity":
                                    Qty_per_Unit_of_Measure = Convert.ToDouble(columnValue);
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