using Dynamicweb.Core;
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
    public class ODataUserSalespersonWriter : IDestinationWriter, IDisposable, IODataDestinationWriter
    {
        private readonly Group _group;
        private readonly int _subGroupId;
        private List<string> UserNames;
        private ODataDestinationWriter _odataDestinationWriter;
        public Mapping Mapping { get; }
        internal ODataUserSalespersonWriter(ODataDestinationWriter odataDestinationWriter, string userGroupID)
        {
            _odataDestinationWriter = odataDestinationWriter;
            Mapping = _odataDestinationWriter.Mapping;
            _group = Group.GetGroupByID(Convert.ToInt32(userGroupID));
            List<Group> groups = _group.Subgroups.Where(obj => obj.Name == "Salesperson").ToList();
            if (groups.Count() == 0)
            {
                Group group = new Group("Salesperson")
                {
                    ParentID = _group.ID
                };
                group.Save();
                _group = Group.GetGroupByID(Convert.ToInt32(userGroupID));
            }
            _subGroupId = _group.Subgroups.Where(obj => obj.Name == "Salesperson").Select(obj => obj.ID).FirstOrDefault();
            UserNames = _odataDestinationWriter.GetAllUserNamesFromUserGroup(_subGroupId, 360);
            if (_odataDestinationWriter.DeleteMissingRows)
            {
                _odataDestinationWriter.ItemsToBeDeleted = _odataDestinationWriter.GetAllUserIDSFromUserGroup(_subGroupId, 360);
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
                _odataDestinationWriter.Logger?.Error($"User ({row["Code"]}) does not have a name and therefor is skipped.");
                return;
            }
            if (row["E_Mail"].ToString() == "")
            {
                _odataDestinationWriter.Logger?.Error($"User {row["Name"]} ({row["Code"]}) does not have an Email and therefor is skipped.");
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
                _odataDestinationWriter.ItemsToBeDeleted.Remove(user.ID.ToString());
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
                user.ExternalID = row["Code"].ToString();
                user.Name = row["Name"].ToString();
                user.Phone = row["Phone_No"].ToString();
                user.JobTitle = row["Job_Title"].ToString();
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
                                case "ObjectTypeUserSalespersonEMail":
                                    user.UserName = user.Email = columnValue;
                                    break;
                                case "AccessUser.AccessUserExternalId":
                                    user.ExternalID = columnValue;
                                    break;
                                case "AccessUser.AccessUserName":
                                    user.Name = columnValue;
                                    break;
                                case "AccessUser.AccessUserJobTitle":
                                    user.JobTitle = columnValue;
                                    break;
                                case "AccessUser.AccessUserPhone":
                                    user.Phone = columnValue;
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
                    _odataDestinationWriter.Logger?.Info($"Detected that UserSalesperson {user.Name} ('{user.Email}') is not part of the endpoint, and therefore is deleted.");
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
