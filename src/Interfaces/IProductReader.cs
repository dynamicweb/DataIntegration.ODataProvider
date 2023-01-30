using Dynamicweb.Ecommerce.Products;
using System.Collections.Generic;

namespace Dynamicweb.DataIntegration.Providers.ODataProvider.Interfaces
{
    internal interface IProductReader
    {
        Product GetNewProduct();
        Product GetProduct(string productId, string productLanguageId = null);
        Product GetProductByNumber(string productNumber, string productLanguageId = null);
        Dictionary<string, ProductField> GetCustomProductFields();
        ProductGroupRelation GetProductGroup(Product product, Group group);
        Group GetGroup(string groupId, string languageId);
    }
}