using Dynamicweb.Core;
using Dynamicweb.DataIntegration.Integration;
using Dynamicweb.DataIntegration.Integration.Interfaces;
using Dynamicweb.DataIntegration.Providers.ODataProvider.Interfaces;
using Dynamicweb.Ecommerce.Products;
using Dynamicweb.Ecommerce.Variants;
using System;
using System.Collections.Generic;
using System.Linq;

namespace Dynamicweb.DataIntegration.Providers.ODataProvider
{
    public class ODataProductVariantWriter : IDestinationWriter, IDisposable, IODataDestinationWriter
    {
        private readonly IProductVariantReader _variantReader;
        private readonly IProductVariantWriter _variantWriter;
        private readonly IProductReader _productReader;
        private readonly IProductWriter _productWriter;
        private ODataDestinationWriter _odataDestinationWriter;

        public Mapping Mapping { get; }

        internal ODataProductVariantWriter(ODataDestinationWriter odataDestinationWriter, IProductVariantReader variantReader, IProductVariantWriter variantWriter, IProductReader productReader, IProductWriter productWriter)
        {
            _odataDestinationWriter = odataDestinationWriter;
            Mapping = _odataDestinationWriter.Mapping;
            _variantReader = variantReader;
            _variantWriter = variantWriter;
            _productReader = productReader;
            _productWriter = productWriter;
            if (_odataDestinationWriter.DeleteMissingRows)
            {
                _odataDestinationWriter.ItemsToBeDeleted = _odataDestinationWriter.GetAllProductIDs(_odataDestinationWriter.LanguageId, true, 360);
            }
        }
        public void Write(Dictionary<string, object> row)
        {
            if (!Mapping.Conditionals.CheckConditionals(row))
            {
                return;
            }
            if (!row.TryGetValue("Item_No", out var masterId))
            {
                _odataDestinationWriter.Logger.Error("Provided data does not contain a master item ID ('Item_No')");
                return;
            }
            var columnMappings = Mapping.GetColumnMappings();

            // Get the values of the imported data. This should probably be expanded into a container class instead of too many out vars once beyond POC.
            GetProductVariantInformation(row, columnMappings, out var productCode, out var variantCode, out var description);

            if (string.IsNullOrEmpty(variantCode))
            {
                _odataDestinationWriter.Logger.Info($"VariantCode of imported data is invalid: '{variantCode}'. Variant not stored at this time.");
                return;
            }

            // Get the master product and ensure it exists already. Otherwise skip this line for import this time.
            Product product = _productReader.GetProduct(Converter.ToString(masterId), _odataDestinationWriter.LanguageId);
            if (product == null || product.LanguageId != _odataDestinationWriter.LanguageId)
            {
                _odataDestinationWriter.Logger.Info($"Master product with ID: '{masterId}' and LanguageID: '{_odataDestinationWriter.LanguageId}' was not found. Variant not stored at this time.");
                return;
            }
            if (description == "")
            {
                description = product.Name;
            }

            // Get the variant if it already exists in DW DB
            var variant = _variantReader.GetVariant(productCode, variantCode, _odataDestinationWriter.LanguageId);

            if (string.IsNullOrEmpty(variant?.VariantId))
            {
                EnsureVariantGroupAndVariantOptionExists(variantCode, _odataDestinationWriter.LanguageId, product, description);

                product.VariantId = product.Id + variantCode;
                product.Number = product.Id;
                product.Name = description;
                string[] variantOptionIds = new string[] { product.Id + variantCode };
                product.ShortDescription = "";
                _variantWriter.CreateExtendedVariant(product, variantOptionIds);
                if (_odataDestinationWriter.DeleteMissingRows)
                {
                    _odataDestinationWriter.ItemsToBeDeleted.Remove(product.Id + ";" + product.VariantId);
                    _odataDestinationWriter.RowsToBeDeleted = _odataDestinationWriter.ItemsToBeDeleted.Count > 0;
                }
            }
        }
        public void RemoveRowsNotInEndpoint()
        {
            if (_odataDestinationWriter.DeleteMissingRows)
            {
                foreach (string item in _odataDestinationWriter.ItemsToBeDeleted)
                {
                    string[] productIdAndProductVariantId = item.Split(';');
                    string productId = productIdAndProductVariantId[0];
                    string productVariantId = Converter.ToString(productIdAndProductVariantId[1]);
                    Product product = _productReader.GetProduct(productId);
                    _odataDestinationWriter.Logger?.Info($"Detected that ProductTranslation {product.Name} ('{product.Number}') with variant {productVariantId} is not part of the endpoint, and therefore is deleted.");
                    _productWriter.DeleteProduct(productId, productVariantId, _odataDestinationWriter.LanguageId);
                }
            }
        }
        private void EnsureVariantGroupAndVariantOptionExists(string optionId, string languageId, Product product, string description)
        {
            var variantGroup = _variantReader.GetVariantGroup(product.Id);
            string modifyedID = product.Id.Replace("-", "");
            if (variantGroup == null)
            {
                variantGroup = new VariantGroup { Id = modifyedID, Family = true };
                variantGroup.SetName(languageId, product.Name);
                variantGroup.SetLabel(languageId, "ERP Variant");
                _variantWriter.SaveVariantGroup(variantGroup);
            }
            var variantGroupRelation = _variantReader.GetVariantGroupProductRelation(product.Id);
            if (variantGroupRelation.Count() == 0)
            {
                _variantWriter.CreateProductToVariantGroupRelation(product.Id, modifyedID);
            }
            var newVariantOption = new VariantOption { Id = product.Id + optionId, GroupId = modifyedID };
            newVariantOption.SetName(languageId, description);
            _variantWriter.SaveVariantOption(newVariantOption);
        }

        private void GetProductVariantInformation(Dictionary<string, object> row, ColumnMappingCollection columnMappings, out string productCode, out string variantCode,
            out string description)
        {
            productCode = null;
            variantCode = "";
            description = "";
            if (_odataDestinationWriter.ImportAll)
            {
                productCode = row["Item_No"].ToString();
                variantCode = row["Code"].ToString();
                description = row["Description"].ToString();
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
                                case "ObjectTypeProductVariantItemNo":
                                    productCode = columnValue;
                                    break;
                                case "ObjectTypeProductVariantCode":
                                    variantCode = columnValue;
                                    break;
                                case "ObjectTypeProductVariantDescription":
                                    description = columnValue;
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
    }
}