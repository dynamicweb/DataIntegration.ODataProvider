﻿using Dynamicweb.Core;
using Dynamicweb.DataIntegration.Integration;
using Dynamicweb.DataIntegration.Integration.Interfaces;
using Dynamicweb.DataIntegration.Providers.ODataProvider.Interfaces;
using Dynamicweb.Ecommerce.Variants;
using System;
using System.Collections.Generic;
using System.Linq;

namespace Dynamicweb.DataIntegration.Providers.ODataProvider
{
    public class ODataProductUnitTranslationWriter : IDestinationWriter, IDisposable, IODataDestinationWriter
    {
        private readonly string _sourceLanguageId;
        private readonly IProductUnitRelationWriter _unitWriter;
        private readonly IProductUnitRelationReader _unitReader;
        private ODataDestinationWriter _odataDestinationWriter;
        public Mapping Mapping { get; }

        internal ODataProductUnitTranslationWriter(ODataDestinationWriter odataDestinationWriter, IProductUnitRelationWriter unitWriter, IProductUnitRelationReader unitReader, string sourceLanguageId)
        {
            _unitWriter = unitWriter;
            _unitReader = unitReader;
            _sourceLanguageId = sourceLanguageId;
            _odataDestinationWriter = odataDestinationWriter;
            Mapping = _odataDestinationWriter.Mapping;
            if (_odataDestinationWriter.DeleteMissingRows)
            {
                _odataDestinationWriter.ItemsToBeDeleted = _unitReader.GetVariantOptions().Select(obj => obj.Id).ToList();
            }
        }

        public void Write(Dictionary<string, object> row)
        {
            if (!Mapping.Conditionals.CheckConditionals(row))
            {
                return;
            }
            var columnMappings = Mapping.GetColumnMappings();
            GetUnitTranslationsInformation(row, columnMappings, out string code, out string language_Code, out string description);
            if (language_Code != _sourceLanguageId)
            {
                return;
            }
            VariantOption variantOption = _unitReader.GetNewVariant();
            variantOption.Id = code;
            variantOption.GroupId = "NavUnits";
            variantOption.SetName(_odataDestinationWriter.LanguageId, description);
            _unitWriter.SaveOrUpdate(variantOption);
            if (_odataDestinationWriter.DeleteMissingRows)
            {
                _odataDestinationWriter.ItemsToBeDeleted.Remove(variantOption.Id);
                _odataDestinationWriter.RowsToBeDeleted = _odataDestinationWriter.ItemsToBeDeleted.Count > 0;
            }
        }
        public void RemoveRowsNotInEndpoint()
        {
            if (_odataDestinationWriter.DeleteMissingRows)
            {
                foreach (string item in _odataDestinationWriter.ItemsToBeDeleted)
                {
                    VariantOption variantOption = _unitReader.GetVariant(item);
                    _unitWriter.DeleteVariantOption(item);
                    _odataDestinationWriter.Logger?.Info($"Detected that ProductUnitTranslation {variantOption.GetName(_odataDestinationWriter.LanguageId)} ('{variantOption.Id}') on language {_odataDestinationWriter.LanguageId} is not part of the endpoint, and therefore is deleted.");
                }
            }
        }
        private void GetUnitTranslationsInformation(Dictionary<string, object> row, ColumnMappingCollection columnMappings, out string code, out string language_Code, out string description)
        {
            code = null;
            language_Code = "";
            description = "";
            if (_odataDestinationWriter.ImportAll)
            {
                code = row["Code"].ToString();
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
                                case "EcomVariantsOptions.VariantOptionId":
                                    code = columnValue;
                                    break;
                                case "EcomVariantsOptions.VariantOptionLanguageId":
                                    language_Code = columnValue;
                                    break;
                                case "EcomVariantsOptions.VariantOptionName":
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
