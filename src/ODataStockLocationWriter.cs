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
	public class ODataStockLocationWriter : IDestinationWriter, IDisposable, IODataDestinationWriter
	{
		private readonly IStockLocationWriter _locationWriter;
		private readonly IStockLocationReader _locationReader;
		private List<StockLocation> _stockLocationToBeDeleted;
		private ODataDestinationWriter _odataDestinationWriter;
		public Mapping Mapping { get; }
		internal ODataStockLocationWriter(ODataDestinationWriter odataDestinationWriter, IStockLocationWriter locationWriter, IStockLocationReader locationReader)
		{
			_odataDestinationWriter = odataDestinationWriter;
			Mapping = _odataDestinationWriter.Mapping;
			_locationWriter = locationWriter;
			_locationReader = locationReader;
			if (_odataDestinationWriter.DeleteMissingRows)
			{
				_stockLocationToBeDeleted = _locationReader.GetStockLocations().ToList();
			}
		}
		public void Write(Dictionary<string, object> row)
		{
			if (row == null || !Mapping.Conditionals.CheckConditionals(row))
			{
				return;
			}
			var columnMappings = Mapping.GetColumnMappings();
			GetLocationsInformation(row, columnMappings, out var code, out var name);
			var stockLocation = _locationReader.GetStockLocation(code, _odataDestinationWriter.LanguageId);
			if (string.IsNullOrEmpty(stockLocation?.GetName(_odataDestinationWriter.LanguageId)))
			{
				StockLocation newStockLocation = new StockLocation
				{
					GroupID = stockLocation.GroupID + 1
				};
				newStockLocation.SetName(_odataDestinationWriter.LanguageId, code);
				newStockLocation.SetDescription(_odataDestinationWriter.LanguageId, name);

				_locationWriter.SaveStockLocation(newStockLocation);
			}
			if (_odataDestinationWriter.DeleteMissingRows)
			{
				_stockLocationToBeDeleted.Remove(_stockLocationToBeDeleted.Where(obj => obj.GetName(_odataDestinationWriter.LanguageId) == code && obj.GetDescription(_odataDestinationWriter.LanguageId) == name).FirstOrDefault());
				_odataDestinationWriter.RowsToBeDeleted = _stockLocationToBeDeleted.Count > 0;
			}
		}
		public void RemoveRowsNotInEndpoint()
		{
			if (_odataDestinationWriter.DeleteMissingRows)
			{
				foreach (StockLocation item in _stockLocationToBeDeleted)
				{
					_locationWriter.DeleteStockLocation(item);
					_odataDestinationWriter.Logger?.Info($"Detected that StockLocation {item.GetName(_odataDestinationWriter.LanguageId)} is not part of the endpoint, and therefore is deleted.");
				}
			}
		}
		private void GetLocationsInformation(Dictionary<string, object> row, ColumnMappingCollection columnMappings, out string code, out string name)
		{
			code = "";
			name = "";
			if (_odataDestinationWriter.ImportAll)
			{
				code = row["Code"].ToString();
				name = row["Name"].ToString();
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
								case "EcomStockLocation.StockLocationName":
									code = columnValue;
									break;
								case "EcomStockLocation.StockLocationDescription":
									name = columnValue;
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
