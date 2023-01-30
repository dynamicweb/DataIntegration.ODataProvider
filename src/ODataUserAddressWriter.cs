using Dynamicweb.Core;
using Dynamicweb.DataIntegration.Integration;
using Dynamicweb.DataIntegration.Integration.Interfaces;
using Dynamicweb.DataIntegration.Providers.ODataProvider.Interfaces;
using Dynamicweb.Security.UserManagement;
using System;
using System.Collections.Generic;
using System.Linq;

namespace Dynamicweb.DataIntegration.Providers.ODataProvider
{
    public class ODataUserAddressWriter : IDestinationWriter, IDisposable, IODataDestinationWriter
    {
        private ODataDestinationWriter _odataDestinationWriter;
        private List<UserAddress> _contactGroupUserAddresses = new List<UserAddress>();
        public Mapping Mapping { get; }
        internal ODataUserAddressWriter(ODataDestinationWriter odataDestinationWriter)
        {
            _odataDestinationWriter = odataDestinationWriter;
            Mapping = _odataDestinationWriter.Mapping;
            if (_odataDestinationWriter.DeleteMissingRows)
            {
                Group contactGroup = Group.GetGroups().Where(obj => obj.Name == "User").FirstOrDefault();
                if (contactGroup != null)
                {
                    _contactGroupUserAddresses = _odataDestinationWriter.GetAllUserAddressesByUserGroup(contactGroup.ID);
                    foreach (UserAddress userAddress in _contactGroupUserAddresses)
                    {
                        if (userAddress.Name != "Company address" && !_odataDestinationWriter.ItemsToBeDeleted.Contains(userAddress.ID.ToString()))
                        {
                            _odataDestinationWriter.ItemsToBeDeleted.Add(userAddress.ID.ToString());
                        }
                    }
                    RemoveCompanyAddresses();
                }
            }
        }
        private void RemoveCompanyAddresses()
        {
            Group customerGroup = Group.GetGroups().Where(obj => obj.Name == "Customer").FirstOrDefault();
            if (customerGroup != null)
            {
                foreach (UserAddress customerUserAddress in _odataDestinationWriter.GetAllUserAddressesByUserGroup(customerGroup.ID))
                {
                    List<UserAddress> customerCompanyAddresses = _contactGroupUserAddresses.Where(obj => obj.CountryCode == customerUserAddress.CountryCode &&
                    obj.Address == customerUserAddress.Address && obj.Address2 == customerUserAddress.Address2 && obj.City == customerUserAddress.City &&
                    obj.Zip == customerUserAddress.Zip && obj.Company == customerUserAddress.Company).ToList();
                    foreach (UserAddress OtherCompanyAddresses in customerCompanyAddresses)
                    {
                        _odataDestinationWriter.ItemsToBeDeleted.Remove(OtherCompanyAddresses.ID.ToString());
                    }
                }
            }
        }
        public void Write(Dictionary<string, object> row)
        {
            if (row == null || !Mapping.Conditionals.CheckConditionals(row))
            {
                return;
            }
            var columnMappings = Mapping.GetColumnMappings();
            if (!row.TryGetValue("Contact_No", out var contactNo))
            {
                _odataDestinationWriter.Logger?.Error("Provided data does not contain a Contact no ('Contact_No')");
                return;
            }
            MapValuesToObject(row, columnMappings, contactNo.ToString());
        }
        private void MapValuesToObject(Dictionary<string, object> row, ColumnMappingCollection columnMappings, string contactNo)
        {
            User user = _odataDestinationWriter.GetUserByExternalID(contactNo);
            if (user == null)
            {
                _odataDestinationWriter.Logger?.Info($"The contact ({contactNo}) does not exists in the database and therefor the addresses for the contact is skipped.");
                return;
            }
            UserAddress userAddress = new UserAddress
            {
                AddressType = false,
                UserID = user.ID,
                CustomerNumber = user.CustomerNumber
            };
            if (_odataDestinationWriter.ImportAll)
            {
                userAddress.Name = userAddress.CallName = row["Code"].ToString();
                userAddress.Company = row["Company_Name"].ToString();
                userAddress.Address = row["Address"].ToString();
                userAddress.Address2 = row["Address_2"].ToString();
                userAddress.City = row["City"].ToString();
                userAddress.Zip = row["Post_Code"].ToString();
                userAddress.CountryCode = row["Country_Region_Code"].ToString();
                userAddress.Phone = row["Phone_No"].ToString();
                userAddress.Cell = row["Mobile_Phone_No"].ToString();
                userAddress.Fax = row["Fax_No"].ToString();
                userAddress.Email = row["E_Mail"].ToString();
                userAddress.Country = _odataDestinationWriter.GetCountryDisplayName(userAddress.CountryCode);
                foreach (var item in userAddress.CustomFieldValues)
                {
                    if (row.TryGetValue("AccessUserAddress." + item.CustomField.SystemName, out var userCustomFieldValue))
                    {
                        item.Value = userCustomFieldValue;
                    }
                }
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
                                case "ObjectTypeUserAddressCode":
                                    userAddress.Name = userAddress.CallName = columnValue;
                                    break;
                                case "AccessUserAddress.AccessUserAddressCompany":
                                    userAddress.Company = columnValue;
                                    break;
                                case "AccessUserAddress.AccessUserAddressAddress":
                                    userAddress.Address = columnValue;
                                    break;
                                case "AccessUserAddress.AccessUserAddressAddress2":
                                    userAddress.Address2 = columnValue;
                                    break;
                                case "AccessUserAddress.AccessUserAddressCity":
                                    userAddress.City = columnValue;
                                    break;
                                case "AccessUserAddress.AccessUserAddressZip":
                                    userAddress.Zip = columnValue;
                                    break;
                                case "AccessUserAddress.AccessUserAddressCountryCode":
                                    userAddress.CountryCode = columnValue;
                                    userAddress.Country = _odataDestinationWriter.GetCountryDisplayName(columnValue);
                                    break;
                                case "AccessUserAddress.AccessUserAddressPhone":
                                    userAddress.Phone = columnValue;
                                    break;
                                case "AccessUserAddress.AccessUserAddressCell":
                                    userAddress.Cell = columnValue;
                                    break;
                                case "AccessUserAddress.AccessUserAddressFax":
                                    userAddress.Fax = columnValue;
                                    break;
                                case "AccessUserAddress.AccessUserAddressEmail":
                                    userAddress.Email = columnValue;
                                    break;
                                default:
                                    foreach (var item in userAddress.CustomFieldValues)
                                    {
                                        if ("AccessUserAddress." + item.CustomField.SystemName == column.DestinationColumn.Name)
                                        {
                                            item.Value = columnValue;
                                        }
                                    }
                                    break;
                            }
                        }
                    }
                }
            }
            List<UserAddress> userAddresses = _contactGroupUserAddresses.Where(obj => obj.CountryCode == userAddress.CountryCode &&
             obj.CallName == userAddress.CallName && obj.Address == userAddress.Address &&
             obj.Address2 == userAddress.Address2 && obj.City == userAddress.City &&
             obj.Zip == userAddress.Zip && obj.Company == userAddress.Company).ToList();
            if (userAddresses.Count() > 0)
            {
                if (_odataDestinationWriter.DeleteMissingRows)
                {
                    foreach (var item in userAddresses)
                    {
                        _odataDestinationWriter.ItemsToBeDeleted.Remove(item.ID.ToString());
                    }
                    _odataDestinationWriter.RowsToBeDeleted = _odataDestinationWriter.ItemsToBeDeleted.Count > 0;
                }
            }
            else
            {
                userAddress.Save();
            }
        }
        public void RemoveRowsNotInEndpoint()
        {
            if (_odataDestinationWriter.DeleteMissingRows)
            {
                foreach (string item in _odataDestinationWriter.ItemsToBeDeleted)
                {
                    UserAddress userAddress = UserAddress.GetUserAddressById(Convert.ToInt32(item));
                    UserAddress.Delete(Convert.ToInt32(item));
                    _odataDestinationWriter.Logger?.Info($"Detected that UserAddress {userAddress.Address} {userAddress.Address2}, {userAddress.Zip} {userAddress.City}({userAddress.CallName})" +
                        $"is not part of the endpoint, and therefore is deleted.");
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