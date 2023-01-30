using Dynamicweb.Ecommerce.Products;

namespace Dynamicweb.DataIntegration.Providers.ODataProvider.Interfaces
{
    internal interface IProductWriter
    {
        void SaveOrUpdate(Product product);
        void SaveProductGroup(Product product, Group group);
        void SaveProductFieldValue(Product product, ProductField productField, object value);
        void DeleteProduct(string productId, string variantId, string languageId);
    }
}