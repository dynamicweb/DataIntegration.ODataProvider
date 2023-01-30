using Dynamicweb.Ecommerce.Products;
using System.Collections.Generic;

namespace Dynamicweb.DataIntegration.Providers.ODataProvider.Interfaces
{
    internal interface IProductGroupReader
    {
        Group GetGroup(string groupId, string languageId);
        IEnumerable<Group> GetGroups(string languageId);
    }
}
