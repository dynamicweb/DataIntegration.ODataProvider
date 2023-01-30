using Dynamicweb.DataIntegration.Providers.ODataProvider.Interfaces;
using Dynamicweb.Ecommerce;
using Dynamicweb.Ecommerce.Products;
using Dynamicweb.Ecommerce.Variants;
using System.Collections.Generic;

namespace Dynamicweb.DataIntegration.Providers.ODataProvider
{
    internal class ProductVariantReader : IProductVariantReader
    {
        private readonly VariantOptionService _variantOptionService;
        private readonly VariantGroupService _variantGroupService;
        private readonly ProductService _productService;

        public ProductVariantReader(VariantOptionService variantOptionService, VariantGroupService variantGroupService, ProductService productService)
        {
            _variantOptionService = variantOptionService;
            _variantGroupService = variantGroupService;
            _productService = productService;
        }
        public IEnumerable<VariantOption> GetVariantOptionForProduct(string productId)
        {
            Product product = _productService.GetProductById(productId, "", true);
            if (product != null)
            {
                return _variantOptionService.GetStockUnitsWithVariants(product);
            }
            return null;
        }

        public VariantOption GetVariantOption(string variantOptionId)
        {
            return _variantOptionService.GetVariantOption(variantOptionId);
        }

        public IEnumerable<VariantGroupProductRelation> GetVariantGroupProductRelation(string productId)
        {
            return _variantGroupService.GetProductRelations(productId);
        }

        public Product GetVariant(string productId, string variantId, string productLanguageId = null)
        {
            if (productLanguageId == null)
            {
                return _productService.GetProductById(productId, variantId, true);
            }

            return _productService.GetProductById(productId, variantId, productLanguageId);
        }

        public VariantGroup GetVariantGroup(string variantGroupId)
        {
            return _variantGroupService.GetVariantGroup(variantGroupId);
        }
    }
}