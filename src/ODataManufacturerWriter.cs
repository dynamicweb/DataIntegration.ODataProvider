using Dynamicweb.Core;
using Dynamicweb.DataIntegration.Integration;
using Dynamicweb.DataIntegration.Integration.Interfaces;
using Dynamicweb.DataIntegration.Providers.ODataProvider.Interfaces;
using Dynamicweb.Ecommerce.Products;
using System;
using System.Collections.Generic;
using System.Linq;

namespace Dynamicweb.DataIntegration.Providers.ODataProvider
{
    public class ODataManufacturerWriter : IDestinationWriter, IDisposable, IODataDestinationWriter
    {
        private ODataDestinationWriter _odataDestinationWriter;
        public Mapping Mapping { get; }
        internal ODataManufacturerWriter(ODataDestinationWriter odataDestinationWriter)
        {
            _odataDestinationWriter = odataDestinationWriter;
            Mapping = _odataDestinationWriter.Mapping;
            if (_odataDestinationWriter.DeleteMissingRows)
            {
                _odataDestinationWriter.ItemsToBeDeleted = Ecommerce.Services.Manufacturers.GetManufacturers().Select(obj => obj.Id).ToList();
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
            Manufacturer manufacturer = new Manufacturer();
            string address = "";
            string address_2 = "";
            if (_odataDestinationWriter.ImportAll)
            {
                manufacturer.Id = row["No"].ToString();
                manufacturer.Name = row["Name"].ToString();
                manufacturer.Address = row["Address"].ToString() + ", " + row["Address_2"].ToString();
                manufacturer.City = row["City"].ToString();
                manufacturer.ZipCode = row["Post_Code"].ToString();
                manufacturer.Country = row["Country_Region_Code"].ToString();
                manufacturer.Phone = row["Phone_No"].ToString();
                manufacturer.Email = row["E_Mail"].ToString();
                manufacturer.Fax = row["Fax_No"].ToString();
                manufacturer.Web = row["Home_Page"].ToString();
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
                                case "EcomManufacturers.ManufacturerId":
                                    manufacturer.Id = columnValue;
                                    break;
                                case "EcomManufacturers.ManufacturerName":
                                    manufacturer.Name = columnValue;
                                    break;
                                case "ObjectTypeManufacturerAddress":
                                    address = columnValue;
                                    break;
                                case "ObjectTypeManufacturerAddress2":
                                    address_2 = columnValue;
                                    break;
                                case "EcomManufacturers.ManufacturerCity":
                                    manufacturer.City = columnValue;
                                    break;
                                case "EcomManufacturers.ManufacturerZipCode":
                                    manufacturer.ZipCode = columnValue;
                                    break;
                                case "EcomManufacturers.ManufacturerCountry":
                                    manufacturer.Country = columnValue;
                                    break;
                                case "EcomManufacturers.ManufacturerPhone":
                                    manufacturer.Phone = columnValue;
                                    break;
                                case "EcomManufacturers.ManufacturerEmail":
                                    manufacturer.Email = columnValue;
                                    break;
                                case "EcomManufacturers.ManufacturerFax":
                                    manufacturer.Fax = columnValue;
                                    break;
                                case "EcomManufacturers.ManufacturerWeb":
                                    manufacturer.Web = columnValue;
                                    break;
                            }
                        }
                    }
                }
            }
            if (!_odataDestinationWriter.ImportAll)
            {
                manufacturer.Address = address + ", " + address_2;
            }
            Ecommerce.Services.Manufacturers.Save(manufacturer);
            if (_odataDestinationWriter.DeleteMissingRows)
            {
                _odataDestinationWriter.ItemsToBeDeleted.Remove(manufacturer.Id);
                _odataDestinationWriter.RowsToBeDeleted = _odataDestinationWriter.ItemsToBeDeleted.Count > 0;
            }
        }
        public void RemoveRowsNotInEndpoint()
        {
            if (_odataDestinationWriter.DeleteMissingRows)
            {
                foreach (string item in _odataDestinationWriter.ItemsToBeDeleted)
                {
                    Manufacturer manufacturer = Ecommerce.Services.Manufacturers.GetManufacturerById(item);
					Ecommerce.Services.Manufacturers.Delete(manufacturer);
                    _odataDestinationWriter.Logger?.Info($"Detected that Manufacturer {manufacturer.Name} ('{manufacturer.Id}') is not part of the endpoint, and therefore is deleted.");
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
