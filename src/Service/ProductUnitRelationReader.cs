using Dynamicweb.DataIntegration.Providers.ODataProvider.Interfaces;
using Dynamicweb.Ecommerce;
using Dynamicweb.Ecommerce.Stocks;
using Dynamicweb.Ecommerce.Variants;
using System.Collections.Generic;

namespace Dynamicweb.DataIntegration.Providers.ODataProvider
{
    internal class ProductUnitRelationReader : IProductUnitRelationReader
    {
        private readonly VariantOptionService _variantOptionService;
        private readonly VariantGroupService _variantGroupService;
        private readonly StockService _stockService;
        private readonly UnitOfMeasureService _unitOfMeasureService;

        public ProductUnitRelationReader(VariantOptionService variantOptionService, VariantGroupService variantGroupService, StockService stockService, UnitOfMeasureService unitOfMeasureService)
        {
            _variantOptionService = variantOptionService;
            _variantGroupService = variantGroupService;
            _stockService = stockService;
            _unitOfMeasureService = unitOfMeasureService;
        }

        public VariantOption GetNewVariant()
        {
            return new VariantOption();
        }
        public StockUnit GetNewStockUnit()
        {
            return new StockUnit();
        }
        public IEnumerable<StockLocation> GetStockLocations()
        {
            return _stockService.GetStockLocations();
        }
        public VariantOption GetVariant(string variantId)
        {
            return _variantOptionService.GetVariantOption(variantId);
        }
        public IEnumerable<VariantOption> GetVariantOptions()
        {
            return _variantOptionService.GetByGroup("NavUnits");
        }
        public IEnumerable<StockUnit> GetStockUnit(string productId, string variantId)
        {
            return _stockService.GetStockUnits(productId, variantId);
        }
        public IEnumerable<StockUnit> GetStockUnit(string productId)
        {
            return _stockService.GetStockUnitsWithVariants(productId);
        }

        public IList<VariantGroup> GetUnitGroups()
        {
            return _variantGroupService.GetUnits();
        }

        public VariantGroup GetUnitGroup(string unitGroupId)
        {
            return _variantGroupService.GetVariantGroup(unitGroupId);
        }
        public IEnumerable<UnitOfMeasure> GetUnitOfMeasures(string productId)
        {
            return _unitOfMeasureService.GetUnitOfMeasures(productId);
        }
    }
}