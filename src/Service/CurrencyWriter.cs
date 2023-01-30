using Dynamicweb.DataIntegration.Providers.ODataProvider.Interfaces;
using Dynamicweb.Ecommerce.International;
using System.Globalization;
using System.Linq;

namespace Dynamicweb.DataIntegration.Providers.ODataProvider.Service
{
    internal class CurrencyWriter : ICurrencyWriter
    {
        private readonly CurrencyService _currencyService;
        public CurrencyWriter(CurrencyService currencyService)
        {
            _currencyService = currencyService;
        }
        public void Delete(string currencyCode)
        {
            _currencyService.Delete(currencyCode);
        }
        public void Save(Currency currency)
        {
            if (TryGetCurrencySymbol(currency.Code, out var currencySymbol))
            {
                currency.Symbol = currencySymbol.ToString();
            }
            if (TryGetCurrencyCulture(currency.Code, out var currencyCulture))
            {
                currency.CultureInfo = currencyCulture.ToString();
            }
            _currencyService.Save(currency);
        }
        private bool TryGetCurrencySymbol(string currencyCode, out string symbol)
        {
            symbol = CultureInfo
                .GetCultures(CultureTypes.AllCultures)
                .Where(c => !c.IsNeutralCulture)
                .Select(culture =>
                {
                    try
                    {
                        return new RegionInfo(culture.Name);
                    }
                    catch
                    {
                        return null;
                    }
                })
                .Where(ri => ri != null && ri.ISOCurrencySymbol == currencyCode)
                .Select(ri => ri.CurrencySymbol)
                .FirstOrDefault();
            return symbol != null;
        }
        private bool TryGetCurrencyCulture(string currencyCode, out string cultureInfo)
        {
            try
            {
                cultureInfo = CultureInfo.GetCultures(CultureTypes.SpecificCultures)
                    .Where(x => new RegionInfo(x.LCID).ISOCurrencySymbol == currencyCode).FirstOrDefault().Name;
            }
            catch (System.Exception)
            {
                cultureInfo = null;
            }
            return cultureInfo != null;
        }
    }
}
