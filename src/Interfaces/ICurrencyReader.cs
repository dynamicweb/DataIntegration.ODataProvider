using Dynamicweb.Ecommerce.International;
using System.Collections.Generic;

namespace Dynamicweb.DataIntegration.Providers.ODataProvider.Interfaces
{
    internal interface ICurrencyReader
    {
        Currency GetDefaultCurrency();
        Currency GetCurrencyFromCurrencyCode(string currencyCode);
        IEnumerable<Currency> GetAllCurrencies();
    }
}
