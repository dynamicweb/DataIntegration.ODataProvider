using Dynamicweb.DataIntegration.Integration;
using Dynamicweb.DataIntegration.Integration.Interfaces;
using System;
using System.Collections.Generic;

namespace Dynamicweb.DataIntegration.Providers.ODataProvider
{
    public class CRMWriter : IDisposable, IDestinationWriter
    {
        private readonly BaseEndpointWriter _baseEndpointWriter;
        public Mapping Mapping { get; }

        internal CRMWriter(BaseEndpointWriter baseEndpointWriter)
        {
            _baseEndpointWriter = baseEndpointWriter;
        }
        public void Write(Dictionary<string, object> Row)
        {
            _baseEndpointWriter.WriteToERP(Row, "CRMWriter");
        }

        public void Close() { }

        public void Dispose() { }
    }
}
