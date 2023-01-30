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
    public class ODataUserWriter : IDestinationWriter, IDisposable, IODataDestinationWriter
    {
        private readonly Group _group;
        private readonly int _subGroupId;
        private List<string> UserNames;
        private ODataDestinationWriter _odataDestinationWriter;
        public Mapping Mapping { get; }
        internal ODataUserWriter(ODataDestinationWriter odataDestinationWriter, string userGroupID)
        {
            _odataDestinationWriter = odataDestinationWriter;
            Mapping = _odataDestinationWriter.Mapping;
            _group = Group.GetGroupByID(Convert.ToInt32(userGroupID));
            List<Group> groups = _group.Subgroups.Where(obj => obj.Name == "User").ToList();
            if (groups.Count() == 0)
            {
                Group group = new Group("User")
                {
                    ParentID = _group.ID,
                };
                group.Save();
                _group = Group.GetGroupByID(Convert.ToInt32(userGroupID));
                _subGroupId = _group.Subgroups.Where(obj => obj.Name == "User").Select(obj => obj.ID).FirstOrDefault();
                _odataDestinationWriter.SetPermissionOnGroup(_subGroupId, "User");
            }
            else
            {
                _subGroupId = _group.Subgroups.Where(obj => obj.Name == "User").Select(obj => obj.ID).FirstOrDefault();
            }
            UserNames = _odataDestinationWriter.GetAllUserNamesFromUserGroup(_subGroupId, 360);
            if (_odataDestinationWriter.DeleteMissingRows)
            {
                _odataDestinationWriter.ItemsToBeDeleted = _odataDestinationWriter.GetAllUserIDSFromUserGroup(_subGroupId, 360);
            }
        }
        public void Write(Dictionary<string, object> row)
        {
            if (row == null || !Mapping.Conditionals.CheckConditionals(row))
            {
                return;
            }
            var columnMappings = Mapping.GetColumnMappings();
            if (!row.TryGetValue("Type", out var userType))
            {
                _odataDestinationWriter.Logger?.Error("Provided data does not contain a Contact type ('Type')");
                return;
            }
            if (userType.ToString() != "Person")
            {
                return;
            }
            if (row["Company_Name"].ToString() == "")
            {
                _odataDestinationWriter.Logger?.Error($"User {row["Name"]} ({row["No"]}) does not belong to any company and therefor is skipped.");
                return;
            }
            if (row["IntegrationCustomerNo"].ToString() == "")
            {
                _odataDestinationWriter.Logger?.Error($"User {row["Name"]} ({row["No"]}) does not link to any customers and therefor is skipped.");
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
            user.UserType = UserType.Default;
            user.EmailAllowed = true;
            if (_odataDestinationWriter.ImportAll)
            {
                user.UserName = user.Email = row["E_Mail"].ToString();
                user.ExternalID = row["No"].ToString();
                user.CustomerNumber = row["IntegrationCustomerNo"].ToString();
                user.Company = row["Company_Name"].ToString();
                user.Name = row["Name"].ToString();
                user.Address = row["Address"].ToString();
                user.Address2 = row["Address_2"].ToString();
                user.CountryCode = row["Country_Region_Code"].ToString();
                user.Zip = row["Post_Code"].ToString();
                user.City = row["City"].ToString();
                user.Phone = row["Phone_No"].ToString();
                user.PhoneMobile = row["Mobile_Phone_No"].ToString();
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
                                case "ObjectTypeUserEMail":
                                    user.UserName = user.Email = columnValue;
                                    break;
                                case "AccessUser.AccessUserExternalId":
                                    user.ExternalID = columnValue;
                                    break;
                                case "AccessUser.AccessUserCustomerNumber":
                                    user.CustomerNumber = columnValue;
                                    break;
                                case "AccessUser.AccessUserCompany":
                                    user.Company = columnValue;
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
            if (!UserNames.Contains(user.UserName))
            {
                UserNames.Add(user.UserName);
            }
            if (_odataDestinationWriter.DeleteMissingRows)
            {
                _odataDestinationWriter.ItemsToBeDeleted.Remove(User.GetUserByUserName(user.UserName).ID.ToString());
                _odataDestinationWriter.RowsToBeDeleted = _odataDestinationWriter.ItemsToBeDeleted.Count > 0;
            }
        }
        public void RemoveRowsNotInEndpoint()
        {
            if (_odataDestinationWriter.DeleteMissingRows)
            {
                foreach (string item in _odataDestinationWriter.ItemsToBeDeleted)
                {
                    User user = User.GetUserByID(Convert.ToInt32(item));
                    User.Delete(Convert.ToInt32(item));
                    _odataDestinationWriter.Logger?.Info($"Detected that User {user.Name} ('{user.Email}') is not part of the endpoint, and therefore is deleted.");
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
