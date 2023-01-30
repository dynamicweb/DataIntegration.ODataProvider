using Dynamicweb.Core;
using Dynamicweb.DataIntegration.Integration;
using Dynamicweb.DataIntegration.Integration.Interfaces;
using Dynamicweb.DataIntegration.Providers.ODataProvider.Interfaces;
using Dynamicweb.DataIntegration.Providers.ODataProvider.Model;
using Dynamicweb.Ecommerce.Products;
using System;
using System.Collections.Generic;
using System.Linq;

namespace Dynamicweb.DataIntegration.Providers.ODataProvider
{
    public class ODataProductGroupWriter : IDestinationWriter, IDisposable, IODataDestinationWriter
    {
        private readonly string _defaultShopId;
        private readonly Group _group;
        private readonly IProductGroupReader _productGroupReader;
        private readonly IProductGroupWriter _productGroupWriter;
        private ODataDestinationWriter _odataDestinationWriter;
        public Mapping Mapping { get; }
        public List<ProductGroup> NotImplementedGroups { get; set; }
        private int ItterationCounter { get; set; }

        internal ODataProductGroupWriter(ODataDestinationWriter odataDestinationWriter, string defaultShopId, Group group, IProductGroupReader productGroupReader, IProductGroupWriter productGroupWriter)
        {
            _odataDestinationWriter = odataDestinationWriter;
            Mapping = _odataDestinationWriter.Mapping;
            _defaultShopId = defaultShopId;
            _group = group;
            _productGroupReader = productGroupReader;
            _productGroupWriter = productGroupWriter;
            NotImplementedGroups = new List<ProductGroup>();
            ItterationCounter = 0;
            if (_odataDestinationWriter.DeleteMissingRows)
            {
                _odataDestinationWriter.ItemsToBeDeleted = _productGroupReader.GetGroups(_odataDestinationWriter.LanguageId).Select(obj => obj.Id).ToList();
            }
        }
        public void TryAddingSkippedObjecs()
        {
            List<ProductGroup> result = new List<ProductGroup>();
            foreach (var itemCategoryCard in NotImplementedGroups)
            {
                var group = new Group { Id = itemCategoryCard.Code, LanguageId = _odataDestinationWriter.LanguageId, Name = itemCategoryCard.Description, NavigationShowInMenu = true, NavigationShowInSiteMap = true, NavigationClickable = true, InheritOrderLineFields = true, InheritCategoryFieldsFromParent = true };
                Group parentGroup;
                if (itemCategoryCard.Parent_Category != "")
                {
                    parentGroup = _productGroupReader.GetGroup(itemCategoryCard.Parent_Category, _odataDestinationWriter.LanguageId);
                    if (parentGroup == null)
                    {
                        result.Add(new ProductGroup { Code = itemCategoryCard.Code, Description = itemCategoryCard.Description, Parent_Category = itemCategoryCard.Parent_Category });
                    }
                }
                var _newGroup = _productGroupReader.GetGroup(itemCategoryCard.Code, _odataDestinationWriter.LanguageId);
                if (_newGroup == null)
                {
                    _productGroupWriter.SaveGroup(group);
                    _productGroupWriter.SaveShopGroupRelation(_defaultShopId, itemCategoryCard.Code);
                }
                if (itemCategoryCard.Parent_Category != "")
                {
                    _productGroupWriter.SaveGroupRelation(itemCategoryCard.Code, itemCategoryCard.Parent_Category);
                }
                if (_odataDestinationWriter.DeleteMissingRows)
                {
                    _odataDestinationWriter.ItemsToBeDeleted.Remove(group.Id);
                    _odataDestinationWriter.RowsToBeDeleted = _odataDestinationWriter.ItemsToBeDeleted.Count > 0;
                }
            }
            if (result.Count > 0 && ItterationCounter < 100)
            {
                ++ItterationCounter;
                NotImplementedGroups = result;
                TryAddingSkippedObjecs();
            }
        }

        public void Write(Dictionary<string, object> row)
        {
            if (row == null || !Mapping.Conditionals.CheckConditionals(row))
            {
                return;
            }
            var columnMappings = Mapping.GetColumnMappings();
            GetItemCategoryCard(row, columnMappings, out var code, out var description, out var parent_Category);
            if (_defaultShopId == "0")
            {
                _odataDestinationWriter.Logger?.Error("A shop is not selected, all records is skipped.");
                return;
            }
            var group = new Group { Id = code, LanguageId = _odataDestinationWriter.LanguageId, Name = description, NavigationShowInMenu = true, NavigationShowInSiteMap = true, NavigationClickable = true, InheritOrderLineFields = true, InheritCategoryFieldsFromParent = true };
            Group parentGroup;
            if (parent_Category != "")
            {
                parentGroup = _productGroupReader.GetGroup(parent_Category, _odataDestinationWriter.LanguageId);
                if (parentGroup == null)
                {
                    NotImplementedGroups.Add(new ProductGroup { Code = code, Description = description, Parent_Category = parent_Category });
                    return;
                }
            }
            var _newGroup = _productGroupReader.GetGroup(code, _odataDestinationWriter.LanguageId);
            if (_newGroup == null)
            {
                _productGroupWriter.SaveGroup(group);
                _productGroupWriter.SaveShopGroupRelation(_defaultShopId, code);
            }
            if (parent_Category != "")
            {
                _productGroupWriter.SaveGroupRelation(code, parent_Category);
            }
            else if (_group != null)
            {
                _productGroupWriter.SaveGroupRelation(code, _group.Id);
            }
            if (_odataDestinationWriter.DeleteMissingRows)
            {
                _odataDestinationWriter.ItemsToBeDeleted.Remove(group.Id);
                _odataDestinationWriter.RowsToBeDeleted = _odataDestinationWriter.ItemsToBeDeleted.Count > 0;
            }
        }
        public void RemoveRowsNotInEndpoint()
        {
            if (_odataDestinationWriter.DeleteMissingRows)
            {
                foreach (string item in _odataDestinationWriter.ItemsToBeDeleted)
                {
                    Group group = _productGroupReader.GetGroup(item, _odataDestinationWriter.LanguageId);
                    _productGroupWriter.DeleteGroup(group, _odataDestinationWriter.LanguageId);
                    _odataDestinationWriter.Logger?.Info($"Detected that ProductGroup {group.Description} ('{group.Id}') is not part of the endpoint, and therefore is deleted.");
                }
            }
        }
        private void GetItemCategoryCard(Dictionary<string, object> row, ColumnMappingCollection columnMappings, out string code, out string description, out string parent_Category)
        {
            code = "";
            description = "";
            parent_Category = "";
            if (_odataDestinationWriter.ImportAll)
            {
                code = row["Code"].ToString();
                description = row["Description"].ToString();
                parent_Category = row["Parent_Category"].ToString();
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
                                case "EcomGroups.GroupId":
                                    code = columnValue;
                                    break;
                                case "EcomGroups.GroupName":
                                    description = columnValue;
                                    break;
                                case "ObjectTypeProductGroupRelations":
                                    parent_Category = columnValue;
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