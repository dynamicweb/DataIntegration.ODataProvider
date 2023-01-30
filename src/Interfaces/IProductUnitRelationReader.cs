using Dynamicweb.Ecommerce.Stocks;
using Dynamicweb.Ecommerce.Variants;
using System.Collections.Generic;

namespace Dynamicweb.DataIntegration.Providers.ODataProvider.Interfaces
{
    internal interface IProductUnitRelationReader
    {
        VariantOption GetNewVariant();
        StockUnit GetNewStockUnit();
        IEnumerable<StockLocation> GetStockLocations();
        VariantOption GetVariant(string variantId);
        IEnumerable<StockUnit> GetStockUnit(string productId, string variantId);
        IEnumerable<StockUnit> GetStockUnit(string productId);
        IList<VariantGroup> GetUnitGroups();
        VariantGroup GetUnitGroup(string unitGroupId);
        IEnumerable<VariantOption> GetVariantOptions();
        IEnumerable<UnitOfMeasure> GetUnitOfMeasures(string productId);
    }
}