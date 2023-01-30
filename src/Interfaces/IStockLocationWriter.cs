using Dynamicweb.Ecommerce.Stocks;

namespace Dynamicweb.DataIntegration.Providers.ODataProvider.Interfaces
{
    internal interface IStockLocationWriter
    {
        void SaveStockLocation(StockLocation theStockLocation);
        void DeleteStockLocation(StockLocation stockLocation);
    }
}
