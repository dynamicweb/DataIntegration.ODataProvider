using Dynamicweb.Core;
using Dynamicweb.DataIntegration.Integration;
using Dynamicweb.DataIntegration.Integration.Interfaces;
using Dynamicweb.DataIntegration.Providers.ODataProvider.Interfaces;
using Dynamicweb.DataIntegration.Providers.ODataProvider.Service;
using Dynamicweb.Ecommerce.International;
using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;

namespace Dynamicweb.DataIntegration.Providers.ODataProvider
{
    public class ODataCurrencyWriter : IDestinationWriter, IDisposable, IODataDestinationWriter
    {
        private readonly ICurrencyReader _currencyReader;
        private readonly ICurrencyWriter _currencyWriter;
        private ODataDestinationWriter _odataDestinationWriter;
        public Mapping Mapping { get; }
        internal ODataCurrencyWriter(ODataDestinationWriter odataDestinationWriter, CurrencyReader currencyReader, CurrencyWriter currencyWriter)
        {
            _currencyReader = currencyReader;
            _currencyWriter = currencyWriter;
            _odataDestinationWriter = odataDestinationWriter;
            Mapping = _odataDestinationWriter.Mapping;
            if (_odataDestinationWriter.DeleteMissingRows)
            {
                _odataDestinationWriter.ItemsToBeDeleted = _currencyReader.GetAllCurrencies().Where(obj => !obj.IsDefault).Select(obj => obj.Code).ToList();
            }
        }
        public void Write(Dictionary<string, object> row)
        {
            if (row == null || !Mapping.Conditionals.CheckConditionals(row))
            {
                return;
            }
            var columnMappings = Mapping.GetColumnMappings();
            row.TryGetValue("ISO_Numeric_Code", out var isoCode);
            if (string.IsNullOrEmpty(isoCode.ToString()) || Converter.ToInt32(isoCode) == _currencyReader.GetDefaultCurrency().PayGatewayCode)
            {
                if (string.IsNullOrEmpty(isoCode.ToString()))
                {
                    _odataDestinationWriter.Logger?.Error("Not able to import: " + row["Description"].ToString() + " (ISO_Numeric_Code is nothing)");
                }
                else
                {
                    _odataDestinationWriter.Logger?.Error("Not able to import: " + row["Description"].ToString() + " (this is the default currency!)");
                }
                return;
            }
            MapValuesToObject(row, columnMappings, isoCode.ToString());
        }
        private void MapValuesToObject(Dictionary<string, object> row, ColumnMappingCollection columnMappings, string isoCode)
        {
            Currency currency = _currencyReader.GetCurrencyFromCurrencyCode(isoCode);
            if (currency == null)
            {
                currency = new Currency();
            }
            if (_odataDestinationWriter.ImportAll)
            {
                currency.Code = row["Code"].ToString();
                currency.SetName(_odataDestinationWriter.LanguageId, row["Description"].ToString());
                currency.Rate = Converter.ToDouble(row["ExchangeRateAmt"]) * 100;
                currency.PayGatewayCode = Converter.ToInt32(row["ISO_Numeric_Code"]);
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
                                case "EcomCurrencies.CurrencyCode":
                                    currency.Code = columnValue;
                                    break;
                                case "EcomCurrencies.CurrencyName":
                                    currency.SetName(_odataDestinationWriter.LanguageId, columnValue);
                                    break;
                                case "EcomCurrencies.CurrencyRate":
                                    currency.Rate = Converter.ToDouble(columnValue) * 100;
                                    break;
                                case "EcomCurrencies.CurrencyPayGatewayCode":
                                    currency.PayGatewayCode = Converter.ToInt32(columnValue);
                                    break;
                            }
                        }
                    }
                }
            }
            _currencyWriter.Save(currency);
            if (_odataDestinationWriter.DeleteMissingRows)
            {
                _odataDestinationWriter.ItemsToBeDeleted.Remove(currency.Code);
                _odataDestinationWriter.RowsToBeDeleted = _odataDestinationWriter.ItemsToBeDeleted.Count > 0;
            }
        }
        public void RemoveRowsNotInEndpoint()
        {
            if (_odataDestinationWriter.DeleteMissingRows)
            {
                foreach (string item in _odataDestinationWriter.ItemsToBeDeleted)
                {
                    Currency currency = _currencyReader.GetCurrencyFromCurrencyCode(item);
                    _currencyWriter.Delete(currency.Code);
                    _odataDestinationWriter.Logger?.Info($"Detected that Currency {currency.GetName(_odataDestinationWriter.LanguageId)} ('{currency.Code}') is not part of the endpoint, and therefore is deleted.");
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
