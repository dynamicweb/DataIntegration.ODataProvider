using Dynamicweb.DataIntegration.Providers.ODataProvider.Interfaces;
using Dynamicweb.Ecommerce.Stocks;
using System.Collections.Generic;

namespace Dynamicweb.DataIntegration.Providers.ODataProvider
{
    internal class StockLocationReader : IStockLocationReader
    {
        private readonly StockService _stockService;
        public StockLocationReader(StockService stockService)
        {
            _stockService = stockService;
        }

        public StockLocation GetStockLocation(string code, string lagnguageId)
        {
            StockLocation result = new StockLocation { GroupID = 0 };
            foreach (var item in _stockService.GetStockLocations(true))
            {
                if (item.GetName(lagnguageId) == code)
                {
                    result = item;
                    break;
                }
                else if (item.GroupID > result.GroupID)
                {
                    result.GroupID = item.GroupID;
                }
            }
            return result;
        }
        public IEnumerable<StockLocation> GetStockLocations()
        {
            return _stockService.GetStockLocations();
        }
    }
}
