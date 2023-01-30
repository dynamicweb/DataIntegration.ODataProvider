using Dynamicweb.Ecommerce.International;

namespace Dynamicweb.DataIntegration.Providers.ODataProvider.Interfaces
{
    internal interface ICurrencyWriter
    {
        void Save(Currency currency);
        void Delete(string currencyCode);
    }
}
