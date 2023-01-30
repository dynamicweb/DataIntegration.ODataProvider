using Dynamicweb.Ecommerce.Products;
using Dynamicweb.Ecommerce.Variants;
using System.Collections.Generic;

namespace Dynamicweb.DataIntegration.Providers.ODataProvider.Interfaces
{
    internal interface IProductVariantReader
    {
        IEnumerable<VariantOption> GetVariantOptionForProduct(string productId);
        VariantOption GetVariantOption(string variantOptionId);
        Product GetVariant(string productId, string variantId, string productLanguageId = null);
        VariantGroup GetVariantGroup(string variantGroupId);
        IEnumerable<VariantGroupProductRelation> GetVariantGroupProductRelation(string productId);
    }
}