using Dynamicweb.Core;
using Dynamicweb.DataIntegration.Integration;
using Dynamicweb.DataIntegration.Integration.Interfaces;
using Dynamicweb.DataIntegration.Providers.ODataProvider.Interfaces;
using Dynamicweb.Ecommerce.International;
using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;

namespace Dynamicweb.DataIntegration.Providers.ODataProvider
{
    public class ODataCountryWriter : IDestinationWriter, IDisposable, IODataDestinationWriter
    {
        private readonly CountryService _countryService;
        private ODataDestinationWriter _odataDestinationWriter;
        public Mapping Mapping { get; }
        internal ODataCountryWriter(ODataDestinationWriter odataDestinationWriter, CountryService countryService)
        {
            _odataDestinationWriter = odataDestinationWriter;
            Mapping = _odataDestinationWriter.Mapping;
            _countryService = countryService;
            if (_odataDestinationWriter.DeleteMissingRows)
            {
                _odataDestinationWriter.ItemsToBeDeleted = _countryService.GetCountries().Select(obj => obj.Code2).ToList();
            }
        }
        public void Write(Dictionary<string, object> row)
        {
            if (row == null || !Mapping.Conditionals.CheckConditionals(row))
            {
                return;
            }
            var columnMappings = Mapping.GetColumnMappings();
            if (!row.TryGetValue("Code", out var countryCode))
            {
                _odataDestinationWriter.Logger?.Error("Provided data does not contain a Country Code");
                return;
            }
            if (countryCode.ToString().Length != 2)
            {
                _odataDestinationWriter.Logger?.Error("Provided datas Country Code is not ISO 3166-1 alpha-2. link: https://en.wikipedia.org/wiki/ISO_3166-1_alpha-2");
                return;
            }
            MapValuesToObject(row, columnMappings, countryCode.ToString());
        }
        private void MapValuesToObject(Dictionary<string, object> row, ColumnMappingCollection columnMappings, string countryCode)
        {
            Country country = new Country();
            if (_odataDestinationWriter.ImportAll)
            {
                country.SetName(_odataDestinationWriter.LanguageId, row["Name"].ToString());
                country.Code2 = row["Code"].ToString();
                country.Number = Converter.ToInt32(row["ISO_Numeric_Code"].ToString());
                string countryAddressFormat = row["Address_Format"].ToString().ToLower().Replace("post code", "zip").Replace("county", "regioncode").Replace("+", "} {");
                countryAddressFormat = "{" + countryAddressFormat.Trim() + "}";
                country.AddressDisplayFormat = countryAddressFormat;
                country.AddressEditFormat = countryAddressFormat;
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
                                case "EcomCountries.CountryCode2":
                                    country.Code2 = columnValue;
                                    break;
                                case "EcomCountryText.CountryTextName":
                                    country.SetName(_odataDestinationWriter.LanguageId, columnValue);
                                    break;
                                case "EcomCountries.CountryNumber":
                                    country.Number = Converter.ToInt32(columnValue);
                                    break;
                                case "ObjectTypeCountryAddressFormat":
                                    columnValue = columnValue.ToLower();
                                    columnValue = columnValue.Replace("post code", "zip");
                                    columnValue = columnValue.Replace("county", "regioncode");
                                    columnValue = columnValue.Replace("+", "} {");
                                    columnValue = "{" + columnValue.Trim() + "}";
                                    country.AddressDisplayFormat = columnValue;
                                    country.AddressEditFormat = columnValue;
                                    break;
                            }
                        }
                    }
                }
            }
            GlobalISO dwISO = GlobalISO.GetGlobalISOByISOCode2(countryCode);
            if (dwISO != null)
            {
                country.CurrencyCode = dwISO.CurrencySymbol;
                country.Vat = Converter.ToDouble(dwISO.Vat);
                country.Code3 = dwISO.Code3;
                country.CultureInfo = "";
                _countryService.Save(country);
            }
            if (_odataDestinationWriter.DeleteMissingRows)
            {
                _odataDestinationWriter.ItemsToBeDeleted.Remove(country.Code2);
                _odataDestinationWriter.RowsToBeDeleted = _odataDestinationWriter.ItemsToBeDeleted.Count > 0;
            }
        }
        public void RemoveRowsNotInEndpoint()
        {
            if (_odataDestinationWriter.DeleteMissingRows)
            {
                foreach (string item in _odataDestinationWriter.ItemsToBeDeleted)
                {
                    Country country = _countryService.GetCountry(item);
                    _countryService.Delete(country.Code2);
                    _odataDestinationWriter.Logger?.Info($"Detected that Country {country.GetName(_odataDestinationWriter.LanguageId)} ('{country.Code2}') is not part of the endpoint, and therefore is deleted.");
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
