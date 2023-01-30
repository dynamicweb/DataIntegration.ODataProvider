using Dynamicweb.DataIntegration.Providers.ODataProvider.Interfaces;
using Dynamicweb.Ecommerce.Products;
using Dynamicweb.Ecommerce.Shops;

namespace Dynamicweb.DataIntegration.Providers.ODataProvider
{
    internal class ProductGroupWriter : IProductGroupWriter
    {
        private readonly GroupService _groupService;
        public ProductGroupWriter(GroupService ghroupService)
        {
            _groupService = ghroupService;
        }
        public void SaveGroup(Group group)
        {
            _groupService.Save(group);
        }
        public void DeleteGroup(Group group, string languageId)
        {
            _groupService.Delete(group, languageId);
        }
        public void SaveShopGroupRelation(string shopId, string groupId)
        {
            ShopGroupRelation _shopGroupRelation = new ShopGroupRelation
            {
                ShopId = shopId,
                GroupId = groupId,
                Sorting = 0
            };
            _shopGroupRelation.Save(shopId, groupId);
        }
        public void SaveGroupRelation(string theGroupId, string theParentId)
        {
            GroupRelation _groupRelation = new GroupRelation
            {
                Id = theGroupId,
                ParentId = theParentId,
                Sorting = 1,
                IsPrimary = true,
                InheritCategories = false
            };
            _groupRelation.Save(theGroupId, theParentId);
        }
    }
}
