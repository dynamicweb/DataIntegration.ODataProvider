using Dynamicweb.DataIntegration.Providers.ODataProvider.Interfaces;
using Dynamicweb.Ecommerce.Products;

namespace Dynamicweb.DataIntegration.Providers.ODataProvider
{
    internal class ProductWriter : IProductWriter
    {
        private readonly ProductService _productService;

        public ProductWriter(ProductService productService)
        {
            _productService = productService;
        }
        public void SaveProductFieldValue(Product product, ProductField productField, object value)
        {
            _productService.SetFieldValue(product, productField, value);
        }

        public void SaveOrUpdate(Product product)
        {
            _productService.Save(product);
        }

        public void SaveProductGroup(Product product, Group group)
        {
            _productService.AddGroup(product, group);
        }
        public void DeleteProduct(string productId, string variantId, string languageId)
        {
            _productService.Delete(productId, variantId, languageId);
        }
    }
}