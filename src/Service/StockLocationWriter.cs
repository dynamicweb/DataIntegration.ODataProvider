using Dynamicweb.DataIntegration.Providers.ODataProvider.Interfaces;
using Dynamicweb.Ecommerce.Stocks;

namespace Dynamicweb.DataIntegration.Providers.ODataProvider
{
    internal class StockLocationWriter : IStockLocationWriter
    {
        private readonly StockService _stockService;

        public StockLocationWriter(StockService stockService)
        {
            _stockService = stockService;
        }

        public void SaveStockLocation(StockLocation theStockLocation)
        {
            _stockService.SaveStockLocation(theStockLocation);
        }
        public void DeleteStockLocation(StockLocation stockLocation)
        {
            _stockService.DeleteStockLocation(stockLocation);
        }
    }
}
