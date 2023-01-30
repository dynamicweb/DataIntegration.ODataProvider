using Dynamicweb.Ecommerce.Products;

namespace Dynamicweb.DataIntegration.Providers.ODataProvider.Interfaces
{
    internal interface IProductGroupWriter
    {
        void SaveGroup(Group group);
        void SaveShopGroupRelation(string shopId, string groupId);
        void SaveGroupRelation(string theGroupId, string theParentId);
        void DeleteGroup(Group group, string languageId);
    }
}
