using Dynamicweb.Ecommerce.Products;
using Dynamicweb.Ecommerce.Variants;
using System.Collections.Generic;

namespace Dynamicweb.DataIntegration.Providers.ODataProvider.Interfaces
{
    internal interface IProductVariantWriter
    {
        void SaveOrUpdate(Product product);
        void CreateSimpleVariant(Product product, IEnumerable<string> optionIds);
        void CreateExtendedVariant(Product product, IEnumerable<string> optionIds);
        void SaveVariantGroup(VariantGroup variantGroup);
        void CreateProductToVariantGroupRelation(string productId, string variantGroupId);
        void SaveVariantOption(VariantOption variantOption);
    }
}