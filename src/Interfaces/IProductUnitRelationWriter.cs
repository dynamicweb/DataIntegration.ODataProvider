using Dynamicweb.Ecommerce.Stocks;
using Dynamicweb.Ecommerce.Variants;

namespace Dynamicweb.DataIntegration.Providers.ODataProvider.Interfaces
{
    internal interface IProductUnitRelationWriter
    {
        void SaveOrUpdate(VariantOption variantOption);
        void SaveUnitGroup(VariantGroup variantGroup);
        void SaveOrUpdate(StockUnit stockUnit);
        void SaveUnitOfMeasure(UnitOfMeasure unitOfMeasure);
        void DeleteStockUnit(StockUnit stockUnit);
        void DeleteVariantOption(string id);
    }
}