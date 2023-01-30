using Dynamicweb.Content;
using Dynamicweb.DataIntegration.Providers.ODataProvider.Interfaces;
using Dynamicweb.Ecommerce.Common;
using Dynamicweb.Ecommerce.Products;
using System.Collections.Generic;
using System.Linq;

namespace Dynamicweb.DataIntegration.Providers.ODataProvider
{
    internal class ProductReader : IProductReader
    {
        private readonly ProductService _productService;
        private readonly GroupService _groupService;

        public ProductReader(ProductService productService, GroupService groupService)
        {
            _productService = productService;
            _groupService = groupService;
        }

        public Product GetNewProduct()
        {
            return Product.CreateProductWithoutDefaultProductFields();
        }

        public Product GetProduct(string productId, string productLanguageId = null)
        {
            if (productLanguageId == null)
            {
                return _productService.GetProductById(productId, "", true);
            }

            return _productService.GetProductById(productId, "", productLanguageId);
        }
        public Product GetProductByNumber(string productNumber, string productLanguageId = null)
        {
            if (productLanguageId == null)
            {
                return _productService.GetProductByNumber(productNumber, Ecommerce.Services.Languages.GetDefaultLanguageId());
            }
            return _productService.GetProductByNumber(productNumber, productLanguageId);
        }

        public Dictionary<string, ProductField> GetCustomProductFields()
        {
            return Application.ProductFields.ToDictionary(field => field.Id);
        }

        public ProductGroupRelation GetProductGroup(Product product, Group group)
        {
            return _groupService.GetProductGroupRelation(product.Id, group.Id);
        }
        public Group GetGroup(string groupId, string languageId)
        {
            return _groupService.GetGroup(groupId, languageId);
        }
    }
}