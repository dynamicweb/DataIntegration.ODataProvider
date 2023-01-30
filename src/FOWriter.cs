using Dynamicweb.DataIntegration.Integration;
using Dynamicweb.DataIntegration.Integration.Interfaces;
using System;
using System.Collections.Generic;

namespace Dynamicweb.DataIntegration.Providers.ODataProvider
{
    public class FOWriter : IDisposable, IDestinationWriter
    {
        private readonly BaseEndpointWriter _baseEndpointWriter;
        public Mapping Mapping { get; }

        internal FOWriter(BaseEndpointWriter baseEndpointWriter)
        {
            _baseEndpointWriter = baseEndpointWriter;
        }

        public void Write(Dictionary<string, object> Row)
        {
            _baseEndpointWriter.WriteToERP(Row, "FOWriter");
        }

        public void Close() { }

        public void Dispose() { }
    }
}
