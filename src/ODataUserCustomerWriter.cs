using Dynamicweb.DataIntegration.Integration;
using Dynamicweb.DataIntegration.Integration.Interfaces;
using Dynamicweb.DataIntegration.Providers.ODataProvider.Interfaces;
using Dynamicweb.Security.UserManagement;
using Dynamicweb.Security.Utilities;
using System;
using System.Collections.Generic;
using System.Linq;

namespace Dynamicweb.DataIntegration.Providers.ODataProvider
{
    public class ODataUserCustomerWriter : IDestinationWriter, IDisposable, IODataDestinationWriter
    {
        private readonly Group _group;
        private readonly bool _customerHasContacts;
        private readonly int _subGroupId;
        private List<string> UserNames;
        private ODataDestinationWriter _odataDestinationWriter;
        public Mapping Mapping { get; }
        internal ODataUserCustomerWriter(ODataDestinationWriter odataDestinationWriter, string customerHasContacts, string userGroupID)
        {
            _odataDestinationWriter = odataDestinationWriter;
            Mapping = _odataDestinationWriter.Mapping;
            _customerHasContacts = customerHasContacts == "Yes";
            _group = Group.GetGroupByID(Convert.ToInt32(userGroupID));
            List<Group> groups = _group.Subgroups.Where(obj => obj.Name == "Customer").ToList();
            if (groups.Count() == 0)
            {
                Group group = new Group("Customer")
                {
                    ParentID = _group.ID
                };
                group.Save();
                _group = Group.GetGroupByID(Convert.ToInt32(userGroupID));
                _subGroupId = _group.Subgroups.Where(obj => obj.Name == "Customer").Select(obj => obj.ID).FirstOrDefault();
                _odataDestinationWriter.SetPermissionOnGroup(_subGroupId, "Customer");
            }
            else
            {
                _subGroupId = _group.Subgroups.Where(obj => obj.Name == "Customer").Select(obj => obj.ID).FirstOrDefault();
            }
            UserNames = _odataDestinationWriter.GetAllUserNamesFromUserGroup(_subGroupId, 360);
            if (_odataDestinationWriter.DeleteMissingRows)
            {
                _odataDestinationWriter.ItemsToBeDeleted = _odataDestinationWriter.GetAllUserIDSFromUserGroup(_subGroupId, 360);
                _odataDestinationWriter.RemoveUsersFromDiscountAndPriceGroups();
            }
        }
        public void Write(Dictionary<string, object> row)
        {
            if (!Mapping.Conditionals.CheckConditionals(row))
            {
                return;
            }
            var columnMappings = Mapping.GetColumnMappings();
            if (row["Name"].ToString() == "")
            {
                _odataDestinationWriter.Logger?.Error($"User ({row["No"]}) does not have a name and therefor is skipped.");
                return;
            }
            if (row["E_Mail"].ToString() == "")
            {
                _odataDestinationWriter.Logger?.Error($"User {row["Name"]} ({row["No"]}) does not have an Email and therefor is skipped.");
                return;
            }
            MapValuesToObject(row, columnMappings);
        }
        private void MapValuesToObject(Dictionary<string, object> row, ColumnMappingCollection columnMappings)
        {
            User user;
            if (UserNames.Contains(row["E_Mail"].ToString()))
            {
                user = User.GetUserByUserName(row["E_Mail"].ToString());
            }
            else
            {
                user = new User
                {
                    Password = PasswordGenerator.GeneratePassword(8)
                };
            }
            user.Active = true;
            user.AddToGroup(_subGroupId);
            string customerNumber = row["No"].ToString();
            user.UserType = UserType.Default;
            user.EmailAllowed = true;
            if (_odataDestinationWriter.ImportAll)
            {
                user.UserName = user.Email = row["E_Mail"].ToString();
                user.CustomerNumber = user.ExternalID = customerNumber;
                user.Name = row["Name"].ToString();
                user.Address = row["Address"].ToString();
                user.Address2 = row["Address_2"].ToString();
                user.CountryCode = row["Country_Region_Code"].ToString();
                user.Zip = row["Post_Code"].ToString();
                user.City = row["City"].ToString();
                user.Phone = row["Phone_No"].ToString();
                user.PhoneMobile = row["MobilePhoneNo"].ToString();
                user.Fax = row["Fax_No"].ToString();
                user.Currency = row["Currency_Code"].ToString();
                user.Country = _odataDestinationWriter.GetCountryDisplayName(user.CountryCode);
                foreach (var item in user.CustomFieldValues)
                {
                    if (row.TryGetValue("AccessUser." + item.CustomField.SystemName, out var userCustomFieldValue))
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
                                case "ObjectTypeUserCustomerEMail":
                                    user.UserName = user.Email = columnValue;
                                    break;
                                case "ObjectTypeUserCustomerNo":
                                    user.CustomerNumber = user.ExternalID = columnValue;
                                    break;
                                case "AccessUser.AccessUserName":
                                    user.Name = columnValue;
                                    break;
                                case "AccessUser.AccessUserAddress":
                                    user.Address = columnValue;
                                    break;
                                case "AccessUser.AccessUserAddress2":
                                    user.Address2 = columnValue;
                                    break;
                                case "AccessUser.AccessUserCountryCode":
                                    user.CountryCode = columnValue;
                                    user.Country = _odataDestinationWriter.GetCountryDisplayName(columnValue);
                                    break;
                                case "AccessUser.AccessUserZip":
                                    user.Zip = columnValue;
                                    break;
                                case "AccessUser.AccessUserCity":
                                    user.City = columnValue;
                                    break;
                                case "AccessUser.AccessUserPhone":
                                    user.Phone = columnValue;
                                    break;
                                case "AccessUser.AccessUserMobile":
                                    user.PhoneMobile = columnValue;
                                    break;
                                case "AccessUser.AccessUserFax":
                                    user.Fax = columnValue;
                                    break;
                                case "AccessUser.AccessUserCurrencyCharacter":
                                    user.Currency = columnValue;
                                    break;
                                default:
                                    foreach (var item in user.CustomFieldValues)
                                    {
                                        if ("AccessUser." + item.CustomField.SystemName == column.DestinationColumn.Name)
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
            bool salesPersonExists = false;
            foreach (var item in user.SecondaryUsers)
            {
                if (item.ExternalID == row["Salesperson_Code"].ToString())
                {
                    salesPersonExists = true;
                }
            }
            if (!salesPersonExists)
            {
                User salesPersonUser = _odataDestinationWriter.GetUserByExternalID(row["Salesperson_Code"].ToString());
                if (salesPersonUser != null)
                {
                    ICollection<int> secondaryUsersIDs = new List<int>
                    {
                        salesPersonUser.ID
                    };
                    User.UpdateSecondaryUsers(user.ID, secondaryUsersIDs, false);
                }
                else
                {
                    _odataDestinationWriter.Logger?.Info($"Salesperson ({row["Salesperson_Code"].ToString()}) does not exists, so the relation to imported user ({user.Name}) is skipped.");
                }
            }
            user.Save();
            user = User.GetUserByEmailAddress(row["E_Mail"].ToString());
            string customerPriceGroup = row["Customer_Price_Group"].ToString();
            if (!string.IsNullOrWhiteSpace(customerPriceGroup))
            {
                string groupName = _odataDestinationWriter.PriceGroupPrefix + customerPriceGroup;
                CreateCustomerGroup(user, groupName, customerNumber);
            }
            string customerDiscGroup = row["Customer_Disc_Group"].ToString();
            if (!string.IsNullOrWhiteSpace(customerDiscGroup))
            {
                string groupName = _odataDestinationWriter.DiscountGroupPrefix + customerDiscGroup;
                CreateCustomerGroup(user, groupName, customerNumber);
            }
            if (!UserNames.Contains(user.UserName))
            {
                UserNames.Add(user.UserName);
            }
            if (_customerHasContacts)
            {
                SaveCompanyAddressOnContacts(user);
            }
            if (_odataDestinationWriter.DeleteMissingRows)
            {
                _odataDestinationWriter.ItemsToBeDeleted.Remove(user.ID.ToString());
                _odataDestinationWriter.RowsToBeDeleted = _odataDestinationWriter.ItemsToBeDeleted.Count > 0;
            }
        }
        private void CreateCustomerGroup(User user, string groupName, string customerNumber)
        {
            Group group = GetOrCreateGroup(groupName, _subGroupId);
            group.AddUser(user.ID);
            group.Save();
            List<Group> groups = Group.GetGroups().Where(obj => obj.Name == "User").ToList();
            if (groups.Any())
            {
                CreateCustomerGroupForUsers(groupName, customerNumber, groups.First().ID);
            }
        }
        private void CreateCustomerGroupForUsers(string groupName, string customerNumber, int subGroupId)
        {
            Group group = GetOrCreateGroup(groupName, subGroupId);
            var users = _odataDestinationWriter.GetUsersByCustomerNumber(customerNumber).Where(obj => obj.ExternalID != customerNumber).ToList();
            foreach (User user in users)
            {
                group.AddUser(user.ID);
            }
            group.Save();
        }
        private Group GetOrCreateGroup(string groupName, int subGroupId)
        {
            Group subGroup = _odataDestinationWriter.GetGroupFromNameAndParentGroupId(groupName, subGroupId);
            if (subGroup is null)
            {
                Group userGroup = new Group(groupName)
                {
                    ParentID = subGroupId
                };
                userGroup.Save();
                subGroup = _odataDestinationWriter.GetGroupFromNameAndParentGroupId(groupName, subGroupId);
            }
            return subGroup;
        }
        public void RemoveRowsNotInEndpoint()
        {
            if (_odataDestinationWriter.DeleteMissingRows)
            {
                foreach (string item in _odataDestinationWriter.ItemsToBeDeleted)
                {
                    User user = User.GetUserByID(Convert.ToInt32(item));
                    User.Delete(Convert.ToInt32(item));
                    _odataDestinationWriter.Logger?.Info($"Detected that UserCustomer {user.Name} ('{user.Email}') is not part of the endpoint, and therefore is deleted.");
                }
            }
        }
        private void SaveCompanyAddressOnContacts(User companyUser)
        {
            foreach (User user in _odataDestinationWriter.GetUsersByExternalIDAndCompany(companyUser.ExternalID, companyUser.Name, false, 360))
            {
                user.CustomerNumber = companyUser.ExternalID;
                user.Save();
                UserAddress userAddress = new UserAddress
                {
                    CountryCode = companyUser.CountryCode,
                    AddressType = false,
                    UserID = user.ID,
                    CustomerNumber = companyUser.CustomerNumber,
                    Company = companyUser.Name,
                    Address = companyUser.Address,
                    Address2 = companyUser.Address2,
                    City = companyUser.City,
                    Zip = companyUser.Zip,
                    Phone = companyUser.Phone,
                    Cell = companyUser.PhoneMobile,
                    Fax = companyUser.Fax,
                    Email = companyUser.Email,
                    Name = companyUser.Name,
                    CallName = companyUser.Name,
                    Country = companyUser.Country,
                    IsDefault = true
                };
                bool companyAddressExist = false;
                List<UserAddress> companyAddresses = UserAddress.GetUserAddresses(user.ID).Where(obj => obj.Name == "Company address").ToList();
                foreach (UserAddress companyAddress in companyAddresses)
                {
                    if (companyAddress.CountryCode != userAddress.CountryCode || companyAddress.Address != userAddress.Address || companyAddress.Address2 != userAddress.Address2 ||
                        companyAddress.City != userAddress.City || companyAddress.Zip != userAddress.Zip || companyAddress.Company != userAddress.Company)
                    {
                        UserAddress.Delete(companyAddress.ID);
                        _odataDestinationWriter.Logger?.Info($"Detected new CompanyAddress, so old is deleted {companyAddress.Address} {companyAddress.Address2}, {companyAddress.Zip} {companyAddress.City}({companyAddress.CallName})" +
                     $"on contact {user.Name}.");
                    }
                    else
                    {
                        companyAddressExist = true;
                    }
                }
                if (!companyAddressExist)
                {
                    userAddress.Save();
                }
                UserAddress companyUserAddress = UserAddress.GetUserAddresses(user.ID).Where(obj => obj.CountryCode == userAddress.CountryCode &&
                 obj.Address == userAddress.Address && obj.Address2 == userAddress.Address2 && obj.City == userAddress.City &&
                 obj.Zip == userAddress.Zip && obj.Company == userAddress.Company).FirstOrDefault();
                if (companyUserAddress != null)
                {
                    companyUserAddress.IsDefault = true;
                    companyUserAddress.CallName = "Company address";
                    companyUserAddress.Name = "Company address";
                    UserAddress.Delete(companyUserAddress.ID);
                    companyUserAddress.Save();
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
