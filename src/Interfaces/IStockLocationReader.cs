using Dynamicweb.Ecommerce.Stocks;
using System.Collections.Generic;

namespace Dynamicweb.DataIntegration.Providers.ODataProvider.Interfaces
{
    internal interface IStockLocationReader
    {
        StockLocation GetStockLocation(string code, string lagnguageId);
        IEnumerable<StockLocation> GetStockLocations();
    }
}
