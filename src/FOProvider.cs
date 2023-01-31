using Dynamicweb.DataIntegration.Integration;
using Dynamicweb.DataIntegration.Integration.Interfaces;
using Dynamicweb.Extensibility.AddIns;
using Dynamicweb.Extensibility.Editors;
using System;
using System.Collections;
using System.Xml;

namespace Dynamicweb.DataIntegration.Providers.ODataProvider
{
    [AddInName("Dynamicweb.DataIntegration.Providers.Provider")]
    [AddInLabel("FO Provider")]
    [AddInDescription("FO provider")]
    [AddInIgnore(false)]
    public class FOProvider : BaseEndpointProvider, IDropDownOptionActions
    {
        public FOProvider() : base() { }

        public FOProvider(XmlNode xmlNode) : base(xmlNode) { }

        public new Hashtable GetOptions(string name)
        {
            var options = base.GetOptions(name);

            if (name == "Predefined endpoint" || name == "Destination endpoint")
            {
                foreach (var endpoint in GetEndpoints(typeof(FOProvider)))
                {
                    options.Add(endpoint.Id, endpoint.Name);
                }
            }
            return options;
        }

        public override bool RunJob(Job job)
        {
            ReplaceMappingConditionalsWithValuesFromRequest(job);

            _endpointService.GetEndpoints(); //needed for reset cached Endpoints as the used one is getting updated values, so it will accumulate it's parameters on each run.
            _endpoint = _endpointService.GetEndpointById(Convert.ToInt32(EndpointId));
            Logger?.Log($"Starting OData export.");
            foreach (var mapping in job.Mappings)
            {
                if (mapping.SourceTable == null)
                {
                    Logger?.Log($"Source table is null.");
                    continue;
                }

                if (mapping.DestinationTable == null)
                {
                    Logger?.Log($"Destination table is null.");
                    continue;
                }
                if (!mapping.Active && mapping.GetColumnMappings().Count == 0)
                {
                    Logger?.Log($"There are no active mappings between '{mapping.SourceTable.Name}' and '{mapping.DestinationTable.Name}'.");
                    continue;
                }

                BaseEndpointWriter baseEndpointWriter = new BaseEndpointWriter(Logger, mapping, _endpoint, _credentials, _endpointAuthenticationService);

                using (var writer = new FOWriter(baseEndpointWriter))
                {
                    using (ISourceReader sourceReader = mapping.Source.GetReader(mapping))
                    {
                        baseEndpointWriter.WriteData(sourceReader, writer);
                    }
                }
            }
            Logger?.Log($"Finished OData export.");

            return true;
        }
    }
}
