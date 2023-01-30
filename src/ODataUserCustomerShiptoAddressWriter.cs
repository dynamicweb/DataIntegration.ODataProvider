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
    public class ODataUserCustomerShiptoAddressWriter : IDestinationWriter, IDisposable, IODataDestinationWriter
    {
        private ODataDestinationWriter _odataDestinationWriter;
        private readonly List<UserAddress> _shipToAddresses = new List<UserAddress>();
        public Mapping Mapping { get; }
        internal ODataUserCustomerShiptoAddressWriter(ODataDestinationWriter odataDestinationWriter)
        {
            _odataDestinationWriter = odataDestinationWriter;
            Mapping = _odataDestinationWriter.Mapping;
            if (_odataDestinationWriter.DeleteMissingRows)
            {
                Group customerGroup = Group.GetGroups().Where(obj => obj.Name == "Customer").FirstOrDefault();
                if (customerGroup != null)
                {
                    _shipToAddresses = _odataDestinationWriter.GetAllUserAddressesByUserGroup(customerGroup.ID);
                    foreach (UserAddress customerUserAddress in _shipToAddresses)
                    {
                        if (!_odataDestinationWriter.ItemsToBeDeleted.Contains(customerUserAddress.ID.ToString()))
                        {
                            _odataDestinationWriter.ItemsToBeDeleted.Add(customerUserAddress.ID.ToString());
                        }
                    }
                    AddCompanyAddressesFromContacts();
                }
            }
        }
        private void AddCompanyAddressesFromContacts()
        {
            Group contactGroup = Group.GetGroups().Where(obj => obj.Name == "User").FirstOrDefault();
            if (contactGroup != null)
            {
                foreach (UserAddress contactUserAddress in _odataDestinationWriter.GetAllUserAddressesByUserGroup(contactGroup.ID))
                {
                    List<UserAddress> contactShipToAddresses = _shipToAddresses.Where(obj => obj.CountryCode == contactUserAddress.CountryCode &&
                    obj.Address == contactUserAddress.Address && obj.Address2 == contactUserAddress.Address2 && obj.City == contactUserAddress.City &&
                    obj.Zip == contactUserAddress.Zip && obj.Company == contactUserAddress.Company).ToList();
                    foreach (UserAddress addresses in contactShipToAddresses)
                    {
                        if (!_odataDestinationWriter.ItemsToBeDeleted.Contains(addresses.ID.ToString()))
                        {
                            _odataDestinationWriter.ItemsToBeDeleted.Add(addresses.ID.ToString());
                        }
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
            if (!row.TryGetValue("Customer_No", out var customerNo))
            {
                _odataDestinationWriter.Logger?.Error("Provided data does not contain a customer number ('Customer_No')");
                return;
            }
            MapValuesToObject(row, columnMappings, customerNo.ToString());
        }
        private void MapValuesToObject(Dictionary<string, object> row, ColumnMappingCollection columnMappings, string customerNo)
        {
            User user = _odataDestinationWriter.GetUserByExternalID(customerNo);
            if (user == null)
            {
                _odataDestinationWriter.Logger?.Error($"The customer ({customerNo}) does not exists in the database and therefore the ship-to addresses of the customer is skipped.");
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
                userAddress.Company = row["Name"].ToString();
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
                                case "AccessUserAddress.AccessUserAddressName":
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
            SaveCompanyShipToAddress(user, userAddress);
        }
        private void SaveCompanyShipToAddress(User companyUser, UserAddress companyUserAddress)
        {
            foreach (User user in _odataDestinationWriter.GetUsersByCustomerNumber(companyUser.CustomerNumber))
            {
                List<UserAddress> userAddresses = UserAddress.GetUserAddresses(user.ID).Where(obj => obj.CountryCode == companyUserAddress.CountryCode &&
                      obj.Address == companyUserAddress.Address && obj.Address2 == companyUserAddress.Address2 && obj.City == companyUserAddress.City &&
                      obj.Zip == companyUserAddress.Zip).ToList();
                if (userAddresses.Count() == 0)
                {
                    UserAddress userAddress = new UserAddress
                    {
                        Name = companyUserAddress.Name,
                        Company = companyUserAddress.Company,
                        Address = companyUserAddress.Address,
                        Address2 = companyUserAddress.Address2,
                        City = companyUserAddress.City,
                        Zip = companyUserAddress.Zip,
                        CountryCode = companyUserAddress.CountryCode,
                        Phone = companyUserAddress.Phone,
                        Cell = companyUserAddress.Cell,
                        Fax = companyUserAddress.Fax,
                        Email = companyUserAddress.Email,
                        Country = companyUserAddress.Country,
                        CustomerNumber = companyUserAddress.CustomerNumber,
                        AddressType = companyUserAddress.AddressType,
                        UserID = user.ID,
                        CallName = companyUserAddress.CallName
                    };
                    userAddress.Save();
                }
                if (_odataDestinationWriter.DeleteMissingRows)
                {
                    foreach (var item in userAddresses)
                    {
                        _odataDestinationWriter.ItemsToBeDeleted.Remove(item.ID.ToString());
                    }
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
                    UserAddress userAddress = UserAddress.GetUserAddressById(Convert.ToInt32(item));
                    UserAddress.Delete(Convert.ToInt32(item));
                    _odataDestinationWriter.Logger?.Info($"Detected that UserCustomerShiptoAddress {userAddress.Address} {userAddress.Address2}, {userAddress.Zip} {userAddress.City}({userAddress.CallName})" +
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
