using Dynamicweb.DataIntegration.Providers.ODataProvider.Interfaces;
using Dynamicweb.Ecommerce;
using Dynamicweb.Ecommerce.Products;
using Dynamicweb.Ecommerce.Variants;
using System.Collections.Generic;

namespace Dynamicweb.DataIntegration.Providers.ODataProvider
{
    internal class ProductVariantWriter : IProductVariantWriter
    {
        private readonly VariantService _variantService;
        private readonly VariantOptionService _variantOptionService;
        private readonly VariantGroupService _variantGroupService;
        private readonly ProductService _productService;

        public ProductVariantWriter(VariantService variantService, VariantOptionService variantOptionService, VariantGroupService variantGroupService, ProductService productService)
        {
            _variantService = variantService;
            _variantOptionService = variantOptionService;
            _variantGroupService = variantGroupService;
            _productService = productService;
        }

        public void CreateSimpleVariant(Product product, IEnumerable<string> optionIds)
        {
            _variantService.CreateSimpleVariant(product.Id, optionIds);
        }

        public void CreateExtendedVariant(Product product, IEnumerable<string> optionIds)
        {
            _variantService.CreateExtendedVariant(product, optionIds);
        }

        public void SaveVariantGroup(VariantGroup variantGroup)
        {
            _variantGroupService.Save(variantGroup);
        }

        public void CreateProductToVariantGroupRelation(string productId, string variantGroupId)
        {
            _variantGroupService.CreateProductRelation(productId, variantGroupId);
        }

        public void SaveVariantOption(VariantOption variantOption)
        {
            _variantOptionService.Save(variantOption);
        }

        public void SaveOrUpdate(Product product)
        {
            _productService.Save(product);
        }
    }
}