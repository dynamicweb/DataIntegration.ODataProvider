using Dynamicweb.Core;
using Dynamicweb.DataIntegration.Integration;
using Dynamicweb.DataIntegration.Integration.Interfaces;
using Dynamicweb.DataIntegration.Providers.ODataProvider.Interfaces;
using Dynamicweb.Ecommerce.Variants;
using System;
using System.Collections.Generic;
using System.Linq;

namespace Dynamicweb.DataIntegration.Providers.ODataProvider
{
    public class ODataProductUnitWriter : IDestinationWriter, IDisposable, IODataDestinationWriter
    {
        private readonly IProductUnitRelationWriter _unitWriter;
        private readonly IProductUnitRelationReader _unitReader;
        private ODataDestinationWriter _odataDestinationWriter;
        public Mapping Mapping { get; }

        internal ODataProductUnitWriter(ODataDestinationWriter odataDestinationWriter, IProductUnitRelationWriter unitWriter, IProductUnitRelationReader unitReader)
        {
            _unitWriter = unitWriter;
            _unitReader = unitReader;
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
            GetUnitVariantInformation(row, columnMappings, out string unitVariantCode, out string description);
            VariantOption variantOption;
            variantOption = _unitReader.GetVariant(unitVariantCode);
            if (variantOption == null)
            {
                variantOption = _unitReader.GetNewVariant();
                variantOption.Id = unitVariantCode;
            }
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
                    _odataDestinationWriter.Logger?.Info($"Detected that ProductUnit {variantOption.GetName(_odataDestinationWriter.LanguageId)} ('{variantOption.Id}') on language {_odataDestinationWriter.LanguageId} is not part of the endpoint, and therefore is deleted.");
                }
            }
        }
        private void GetUnitVariantInformation(Dictionary<string, object> row, ColumnMappingCollection columnMappings, out string unitVariantCode, out string Description)
        {
            unitVariantCode = null;
            Description = "";
            if (_odataDestinationWriter.ImportAll)
            {
                unitVariantCode = row["Code"].ToString();
                Description = row["Description"].ToString();
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
                                    unitVariantCode = columnValue;
                                    break;
                                case "EcomVariantsOptions.VariantOptionName":
                                    Description = columnValue;
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
