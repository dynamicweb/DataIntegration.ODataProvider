using Dynamicweb.DataIntegration.Providers.ODataProvider.Interfaces;
using Dynamicweb.Ecommerce.Products;
using System.Collections.Generic;

namespace Dynamicweb.DataIntegration.Providers.ODataProvider
{
    internal class ProductGroupReader : IProductGroupReader
    {
        private readonly GroupService _groupService;

        public ProductGroupReader(GroupService ghroupService)
        {
            _groupService = ghroupService;
        }
        public Group GetGroup(string groupId, string languageId)
        {
            return _groupService.GetGroup(groupId, languageId);
        }
        public IEnumerable<Group> GetGroups(string languageId)
        {
            return _groupService.GetGroups(languageId);
        }
    }
}
