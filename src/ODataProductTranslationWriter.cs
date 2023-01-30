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
    public class ODataProductTranslationWriter : IDestinationWriter, IDisposable, IODataDestinationWriter
    {
        private readonly string _sourceLanguageId;
        private readonly IProductVariantReader _variantReader;
        private readonly IProductVariantWriter _variantWriter;
        private readonly IProductReader _productReader;
        private readonly IProductWriter _productWriter;
        private ODataDestinationWriter _odataDestinationWriter;
        public Mapping Mapping { get; }
        internal ODataProductTranslationWriter(ODataDestinationWriter odataDestinationWriter, IProductVariantReader variantReader, IProductVariantWriter variantWriter, IProductReader productReader, IProductWriter productWriter, string sourceLanguageId)
        {
            _odataDestinationWriter = odataDestinationWriter;
            Mapping = _odataDestinationWriter.Mapping;
            _variantReader = variantReader;
            _variantWriter = variantWriter;
            _productReader = productReader;
            _productWriter = productWriter;
            _sourceLanguageId = sourceLanguageId; 
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
            var columnMappings = Mapping.GetColumnMappings();
            GetItemTranslationInformation(row, columnMappings, out string productCode, out string variant_Code, out string language_Code, out string description);
            if (language_Code != _sourceLanguageId)
            {
                return;
            }
            var product = _productReader.GetProduct(productCode);

            if (product != null)
            {
                if (description == "")
                {
                    description = product.Name;
                }
                if (variant_Code == "")
                {
                    var productTranslated = _productReader.GetProduct(productCode, _odataDestinationWriter.LanguageId);
                    if (productTranslated == null)
                    {
                        productTranslated = product;
                    }
                    if (description == "")
                    {
                        description = productTranslated.Name;
                    }
                    productTranslated.VariantId = "";
                    productTranslated.Number = product.Id;
                    productTranslated.Name = description;
                    productTranslated.ShortDescription = "";
                    productTranslated.LanguageId = _odataDestinationWriter.LanguageId;
                    _productWriter.SaveOrUpdate(productTranslated);
                }
                else
                {
                    product.VariantId = product.Id + variant_Code;
                    product.Number = product.Id;
                    product.Name = description;
                    string[] variantOptionIds = new[] { product.Id + variant_Code };
                    product.ShortDescription = "";
                    product.LanguageId = _odataDestinationWriter.LanguageId;
                    EnsureVariantGroupAndVariantOptionExists(variant_Code, _odataDestinationWriter.LanguageId, product, description);
                    _variantWriter.CreateExtendedVariant(product, variantOptionIds);
                }
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
                    string productVariantId = "";
                    Product product = _productReader.GetProduct(productId, _odataDestinationWriter.LanguageId);
                    if (!item.EndsWith(";"))
                    {
                        productVariantId = Converter.ToString(productIdAndProductVariantId[1]);
                        _odataDestinationWriter.Logger?.Info($"Detected that ProductTranslation {product.Name} ('{product.Number}') with variant {productVariantId} is not part of the endpoint, and therefore is deleted.");
                    }
                    else
                    {
                        _odataDestinationWriter.Logger?.Info($"Detected that ProductTranslation {product.Name} ('{product.Number}') is not part of the endpoint, and therefore is deleted.");
                    }
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
            var newVariantOption = new VariantOption { Id = product.Id + optionId, GroupId = modifyedID};
            newVariantOption.SetName(languageId, description);
            _variantWriter.SaveVariantOption(newVariantOption);
        }
        private void GetItemTranslationInformation(Dictionary<string, object> row, ColumnMappingCollection columnMappings, out string productCode, out string variant_Code,
            out string language_Code, out string description)
        {
            productCode = null;
            variant_Code = null;
            language_Code = "";
            description = "";
            if (_odataDestinationWriter.ImportAll)
            {
                productCode = row["Item_No"].ToString();
                variant_Code = row["Variant_Code"].ToString();
                language_Code = row["Language_Code"].ToString();
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
                                case "ObjectTypeProductTranslationItemNo":
                                    productCode = columnValue;
                                    break;
                                case "ObjectTypeProductTranslationCode":
                                    variant_Code = columnValue;
                                    break;
                                case "ObjectTypeProductTranslationLanguageCode":
                                    language_Code = columnValue;
                                    break;
                                case "ObjectTypeProductTranslationDescription":
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
