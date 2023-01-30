using Dynamicweb.DataIntegration.Providers.ODataProvider.Interfaces;
using Dynamicweb.Ecommerce;
using Dynamicweb.Ecommerce.Stocks;
using Dynamicweb.Ecommerce.Variants;

namespace Dynamicweb.DataIntegration.Providers.ODataProvider
{
    internal class ProductUnitRelationWriter : IProductUnitRelationWriter
    {
        private readonly VariantOptionService _variantOptionService;
        private readonly VariantService _variantService;
        private readonly StockService _stockService;
        private readonly UnitOfMeasureService _unitOfMeasureService;

        public ProductUnitRelationWriter(VariantOptionService variantOptionService, VariantService variantService, StockService stockService, UnitOfMeasureService unitOfMeasureService)
        {
            _variantOptionService = variantOptionService;
            _variantService = variantService;
            _stockService = stockService;
            _unitOfMeasureService = unitOfMeasureService;
        }

        public void SaveOrUpdate(VariantOption variantOption)
        {
            _variantOptionService.Save(variantOption);
        }

        public void SaveOrUpdate(StockUnit stockUnit)
        {
            _stockService.SaveStockUnit(stockUnit);
        }
        public void SaveUnitOfMeasure(UnitOfMeasure unitOfMeasure)
        {
            _unitOfMeasureService.Save(unitOfMeasure);
        }

        public void SaveUnitGroup(VariantGroup variantGroup)
        {
            _variantService.Save(variantGroup);
        }
        public void DeleteVariantOption(string id)
        {
            _variantOptionService.DeleteVariantOption(id);
        }
        public void DeleteStockUnit(StockUnit stockUnit)
        {
            StockUnitIdentifier stockUnitIdentifier = new StockUnitIdentifier
            {
                ProductId = stockUnit.ProductId,
                ProductNumber = stockUnit.ProductNumber,
                StockLocationId = stockUnit.StockLocationId,
                UnitId = stockUnit.UnitId,
                VariantId = stockUnit.VariantId
            };
            _stockService.DeleteStockUnit(stockUnitIdentifier);
        }
    }
}