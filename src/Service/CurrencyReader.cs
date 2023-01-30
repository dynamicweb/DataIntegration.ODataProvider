using Dynamicweb.DataIntegration.Providers.ODataProvider.Interfaces;
using Dynamicweb.Ecommerce.International;
using System.Collections.Generic;

namespace Dynamicweb.DataIntegration.Providers.ODataProvider.Service
{
    internal class CurrencyReader : ICurrencyReader
    {
        private readonly CurrencyService _currencyService;
        public CurrencyReader(CurrencyService currencyService)
        {
            _currencyService = currencyService;
        }
        public Currency GetDefaultCurrency()
        {
            return _currencyService.GetDefaultCurrency();
        }
        public Currency GetCurrencyFromCurrencyCode(string currencyCode)
        {
            return _currencyService.GetCurrency(currencyCode);
        }
        public IEnumerable<Currency> GetAllCurrencies()
        {
            return _currencyService.GetAllCurrencies();
        }
    }
}
