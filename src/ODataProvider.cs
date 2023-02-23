﻿using Dynamicweb.Core;
using Dynamicweb.DataIntegration.EndpointManagement;
using Dynamicweb.DataIntegration.Integration;
using Dynamicweb.DataIntegration.Integration.Interfaces;
using Dynamicweb.DataIntegration.Providers.ODataProvider.Interfaces;
using Dynamicweb.DataIntegration.Providers.ODataProvider.Model;
using Dynamicweb.Extensibility.AddIns;
using Dynamicweb.Extensibility.Editors;
using System;
using System.Collections;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Net;
using System.Threading.Tasks;
using System.Xml;
using System.Xml.Linq;

namespace Dynamicweb.DataIntegration.Providers.ODataProvider
{
    [AddInName("Dynamicweb.DataIntegration.Providers.Provider")]
    [AddInLabel("OData Provider")]
    [AddInDescription("OData provider")]
    [AddInIgnore(false)]
    [AddInUseParameterSectioning(true)]
    public class ODataProvider : BaseProvider, ISource, IDestination, IDropDownOptions
    {
        internal string _workingDirectory;
        internal readonly EndpointService _endpointService = new EndpointService();
        internal Schema _schema;
        internal Endpoint _endpoint;
        internal ICredentials _credentials;
        internal string _autodetectedMetadataURL;
        internal string _metadataUrl;
        internal bool _loadAPIFunction = false;
        internal ODataSourceReader _endpointSourceReader;
        internal IHttpRestClient Client => new HttpRestClient(_credentials, 20); // Unfortunately we need to provide for encapsulated instantiation of the HttpClient due to how ConfigurableAddIns work :(


        #region AddInManager/ConfigurableAddIn Source

        [AddInParameter("Predefined endpoint")]
        [AddInParameterEditor(typeof(DropDownParameterEditor), "none=true;refreshParameters=true;required=true")]
        [AddInParameterGroup("Source")]
        [AddInParameterSection("Source")]
        public string EndpointId
        {
            get => _endpoint?.Id.ToString();
            set => _endpoint = _endpointService.GetEndpointById(Convert.ToInt32(value));
        }

        [AddInParameter("Metadata url")]
        [AddInParameterEditor(typeof(TextParameterEditor), "infoText=Manually override autodetected metadata URL")]
        [AddInParameterGroup("Source")]
        [AddInParameterSection("Metadata")]
        public string MetadataUrl { get => _metadataUrl; set => _metadataUrl = value; }

        [AddInParameter("Autodetected metadata url")]
        [AddInParameterEditor(typeof(LabelParameterEditor), "")]
        [AddInParameterGroup("Source")]
        [AddInParameterSection("Metadata")]
        public string AutodetectedMetadataURL { get => _autodetectedMetadataURL; set { SetCredentials(); } }

        [AddInParameter("Mode")]
        [AddInParameterEditor(typeof(DropDownParameterEditor), "Info=Required;none=true;nonetext=Full Replication;noneHint=This mode gets all records and deletes nothing. This option should only run once.;columns=Mode|Comment;SortBy=Key;HideParameters=Run request in intervals (pages),Do not store last response in log file")]
        [AddInParameterGroup("Source")]
        [AddInParameterSection("Advanced activity settings")]
        public string Mode { get; set; }

        [AddInParameter("Maximum page size")]
        [AddInParameterEditor(typeof(IntegerNumberParameterEditor), "allowNegativeValues=false")]
        [AddInParameterGroup("Source")]
        [AddInParameterSection("Advanced activity settings")]
        public int MaximumPageSize { get; set; }

        [AddInParameter("Request timeout (minutes)")]
        [AddInParameterEditor(typeof(IntegerNumberParameterEditor), "allowNegativeValues=false")]
        [AddInParameterGroup("Source")]
        [AddInParameterSection("Advanced activity settings")]
        public int RequestTimeout { get; set; } = 20;

        [AddInParameter("Run last request")]
        [AddInParameterEditor(typeof(YesNoParameterEditor), "")]
        [AddInParameterGroup("Source")]
        [AddInParameterSection("Advanced activity settings")]
        public bool RunLastRequest { get; set; }

        [AddInParameter("Run request in intervals (pages)")]
        [AddInParameterEditor(typeof(IntegerNumberParameterEditor), "allowNegativeValues=false")]
        [AddInParameterGroup("Source")]
        [AddInParameterSection("Advanced activity settings")]
        public int RequestIntervals { get; set; } = 0;

        [AddInParameter("Do not store last response in log file")]
        [AddInParameterEditor(typeof(YesNoParameterEditor), "Tooltip=Usefull when working with large amount of data.")]
        [AddInParameterGroup("Source")]
        [AddInParameterSection("Advanced activity settings")]
        public bool DoNotStoreLastResponseInLogFile { get; set; }

        #endregion

        #region AddInManager/ConfigurableAddIn Destination

        [AddInParameter("Destination endpoint")]
        [AddInParameterEditor(typeof(DropDownParameterEditor), "none=true;refreshParameters=true;required=true")]
        [AddInParameterGroup("Destination")]
        public string DestinationEndpointId
        {
            get => _endpoint?.Id.ToString();
            set => _endpoint = _endpointService.GetEndpointById(Convert.ToInt32(value));
        }

        [AddInParameter("Destination metadata url")]
        [AddInParameterEditor(typeof(TextParameterEditor), "infoText=Manually override autodetected metadata URL")]
        [AddInParameterGroup("Destination")]
        public string DestinationMetadataURL { get => _metadataUrl; set => _metadataUrl = value; }

        [AddInParameter("Autodetected destination metadata url")]
        [AddInParameterEditor(typeof(LabelParameterEditor), "")]
        [AddInParameterGroup("Destination")]
        public string AutodetectedDestinationMetadataURL { get => _autodetectedMetadataURL; set { SetCredentials(); } }

        #endregion

        private string GetMetadataURL()
        {
            if (_endpoint.Url.Contains("companies(", StringComparison.OrdinalIgnoreCase))
            {
                _autodetectedMetadataURL = _endpoint.Url.Substring(0, _endpoint.Url.IndexOf("companies(", StringComparison.OrdinalIgnoreCase)) + "$metadata";
            }
            else if (_endpoint.Url.Contains("company(", StringComparison.OrdinalIgnoreCase))
            {
                _autodetectedMetadataURL = _endpoint.Url.Substring(0, _endpoint.Url.IndexOf("company(", StringComparison.OrdinalIgnoreCase)) + "$metadata";
            }
            else
            {
                _autodetectedMetadataURL = new Uri(new Uri(_endpoint.Url), "$metadata").AbsoluteUri;
            }
            if (string.IsNullOrEmpty(_metadataUrl))
            {
                return _autodetectedMetadataURL;
            }
            else
            {
                return _metadataUrl;
            }
        }

        private string GetEntityName()
        {
            return new Uri(_endpoint.Url).Segments.LastOrDefault() ?? _endpoint.Name;
        }

        internal void SetCredentials()
        {
            if (_endpoint != null)
            {
                var endpointAuthentication = _endpoint.Authentication;
                if (endpointAuthentication != null)
                {
                    var metadataUri = new Uri(GetMetadataURL());
                    var credentialCache = new CredentialCache
                    {
                        { new Uri(metadataUri.GetLeftPart(UriPartial.Authority)), endpointAuthentication.Type.ToString(), endpointAuthentication.GetNetworkCredential() }
                    };
                    _credentials = credentialCache;
                }
            }
        }

        /// <inheritdoc />
        public Hashtable GetOptions(string name)
        {
            Hashtable options = new Hashtable();
            if (name == "Mode")
            {
                options.Add("Delta Replication", "Delta replication|This mode filters records on date and time, whenever possible, and it only acts on new or updated records. It never deletes.");
                options.Add("First page", "First page|If maximum page size is 100 then this setting only handles the 100 records of the first page.");
            }
            if (name == "Predefined endpoint" || name == "Destination endpoint")
            {
                foreach (var endpoint in _endpointService.GetEndpoints())
                {
                    options.Add(endpoint.Id, endpoint.Name);
                }
            }
            return options;
        }

        /// <inheritdoc />
        public override string WorkingDirectory
        {
            get => _workingDirectory;
            set => _workingDirectory = value.Replace("\\", "/");
        }

        /// <inheritdoc />
        public override bool SchemaIsEditable => false;

        /// <inheritdoc />
        public override Schema GetSchema()
        {
            return _schema ?? (_schema = GetOriginalSourceSchema());
        }

        /// <inheritdoc />
        public override void OverwriteSourceSchemaToOriginal()
        {
            _schema = GetOriginalSourceSchema();
        }

        public override Schema GetOriginalDestinationSchema()
        {
            return GetOriginalSourceSchema();
        }

        public override void OverwriteDestinationSchemaToOriginal()
        {
            _schema = GetOriginalSourceSchema();
        }

        /// <inheritdoc />
        public override Schema GetOriginalSourceSchema()
        {
            var name = GetEntityName();
            var entityTypeTables = new Schema();
            var entitySetsTables = new Schema();

            var header = new Dictionary<string, string>
            {
                { "accept", "text/html,application/xhtml+xml,application/xml" },
                { "Content-Type", "text/html" }
            };
            if (_endpoint != null)
            {
                var endpointAuthentication = _endpoint.Authentication;
                Task metadataResponse;
                if (endpointAuthentication.IsTokenBased())
                {
                    string token = OAuthHelper.GetToken(_endpoint, endpointAuthentication);
                    metadataResponse = Client.GetAsync(GetMetadataURL(), HandleStream, token);
                }
                else
                {
                    metadataResponse = Client.GetAsync(GetMetadataURL(), HandleStream, endpointAuthentication, header);
                }
                metadataResponse.Wait();
            }
            return entitySetsTables;

            void HandleStream(Stream responseStream, HttpStatusCode responseStatusCode, Dictionary<string, string> responseHeaders)
            {
                try
                {
                    var xmlReader = XmlReader.Create(responseStream);
                    using (xmlReader)
                    {
                        if (EndpointIsLoadAllEntities(_endpoint.Url))
                        {
                            while (xmlReader.Read())
                            {
                                if (xmlReader.NodeType == XmlNodeType.Element &&
                                    xmlReader.Name.Equals("EntityType", StringComparison.OrdinalIgnoreCase))
                                {
                                    var table = entityTypeTables.AddTable(xmlReader.GetAttribute("Name"));
                                    AddPropertiesFromXMLReaderToTable(xmlReader, table, entityTypeTables);
                                }
                                else if (xmlReader.NodeType == XmlNodeType.Element &&
                                    xmlReader.Name.Equals("EntitySet", StringComparison.OrdinalIgnoreCase))
                                {
                                    GetColumnsFromEntityTypeTableToEntitySetTable(entitySetsTables.AddTable(xmlReader.GetAttribute("Name")), entityTypeTables, xmlReader.GetAttribute("EntityType"));
                                }
                            }
                        }
                        else
                        {
                            var table = entityTypeTables.AddTable(name);
                            while (xmlReader.Read())
                            {
                                if (xmlReader.NodeType == XmlNodeType.Element &&
                                    xmlReader.Name.Equals("EntityType", StringComparison.OrdinalIgnoreCase))
                                {
                                    if (xmlReader.GetAttribute("Name")?.Equals(name, StringComparison.OrdinalIgnoreCase) ?? false)
                                    {
                                        AddPropertiesFromXMLReaderToTable(xmlReader, table, entityTypeTables);
                                    }
                                    else if (xmlReader.GetAttribute("Name")?.Equals(name + "SalesLines", StringComparison.OrdinalIgnoreCase) ?? false)
                                    {
                                        table = entityTypeTables.AddTable(name + "SalesLines");
                                        AddPropertiesFromXMLReaderToTable(xmlReader, table, entityTypeTables);
                                    }
                                    else if (xmlReader.GetAttribute("Name")?.Equals(name + "SalesInvLines", StringComparison.OrdinalIgnoreCase) ?? false)
                                    {
                                        table = entityTypeTables.AddTable(name + "SalesInvLines");
                                        AddPropertiesFromXMLReaderToTable(xmlReader, table, entityTypeTables);
                                    }
                                }
                                else if (xmlReader.NodeType == XmlNodeType.Element &&
                                    xmlReader.Name.Equals("EntitySet", StringComparison.OrdinalIgnoreCase))
                                {
                                    GetColumnsFromEntityTypeTableToEntitySetTable(entitySetsTables.AddTable(xmlReader.GetAttribute("Name")), entityTypeTables, xmlReader.GetAttribute("EntityType"));
                                }
                            }
                        }
                    }
                }
                catch (Exception e)
                {
                    Logger?.Error("Error getting original source schema.", e);
                }
            }
        }

        private void GetColumnsFromEntityTypeTableToEntitySetTable(Table table, Schema entityTypeSchema, string entityTypeName)
        {
            var entityTypeNameClean = entityTypeName.Substring(entityTypeName.LastIndexOf(".") + 1);
            Table result = entityTypeSchema.GetTables().Where(obj => obj.Name.Equals(entityTypeNameClean, StringComparison.OrdinalIgnoreCase)).FirstOrDefault();
            if (result != null)
            {
                foreach (var item in result.Columns)
                {
                    if (table.Columns.Where(obj => obj.Name == item.Name).Count() == 0)
                    {
                        table.AddColumn(new Column(item.Name, item.Type, table, item.IsPrimaryKey, item.IsNew));
                    }
                }
            }
        }

        private void AddPropertiesFromXMLReaderToTable(XmlReader xmlReader, Table table, Schema result)
        {
            string baseType = xmlReader.GetAttribute("BaseType");
            string entityName = xmlReader.GetAttribute("Name");
            List<string> primaryKeys = new List<string>();
            while (xmlReader.Read() && !(xmlReader.NodeType == XmlNodeType.EndElement && xmlReader.Name.Equals("EntityType", StringComparison.OrdinalIgnoreCase)))
            {
                if (xmlReader.NodeType == XmlNodeType.Element && xmlReader.Name.Equals("PropertyRef", StringComparison.OrdinalIgnoreCase))
                {
                    primaryKeys.Add(xmlReader.GetAttribute("Name"));
                }
                else if (xmlReader.NodeType == XmlNodeType.Element && xmlReader.Name.Equals("Property", StringComparison.OrdinalIgnoreCase))
                {
                    var columnName = xmlReader.GetAttribute("Name");
                    var columnTypeString = xmlReader.GetAttribute("Type");
                    var columnType = GetColumnType(columnTypeString);
                    var isPrimaryKey = primaryKeys.Contains(columnName);
                    table.AddColumn(new Column(columnName, columnType, table, isPrimaryKey, false));
                }
                else if (xmlReader.Name.Equals("EntityType", StringComparison.OrdinalIgnoreCase) && xmlReader.GetAttribute("Name") != entityName)
                {
                    break;
                }
            }
            //for CRM as they can extend other tables
            if (!string.IsNullOrWhiteSpace(baseType))
            {
                Table baseTypeTable = result.GetTables().Where(obj => ("mscrm." + obj.Name).Equals(baseType, StringComparison.OrdinalIgnoreCase)).FirstOrDefault();
                if (baseTypeTable != null)
                {
                    foreach (var item in baseTypeTable.Columns)
                    {
                        if (table.Columns.Where(obj => obj.Name == item.Name).Count() == 0)
                        {
                            table.AddColumn(new Column(item.Name, item.Type, table, item.IsPrimaryKey, item.IsNew));
                        }
                    }
                }
            }
        }

        /// <summary>
        /// Gets the type of the column based on OData standard (https://docs.oasis-open.org/odata/odata-csdl-json/v4.01/csprd05/odata-csdl-json-v4.01-csprd05.html#_Toc21527141)
        /// </summary>
        /// <param name="columnTypeString">The column type string.</param>
        /// <returns></returns>
        private static Type GetColumnType(string columnTypeString)
        {
            switch (columnTypeString)
            {
                case "Edm.String":
                    return typeof(string);
                case "Edm.Guid":
                    return typeof(Guid);
                case "Edm.Int32":
                    return typeof(int);
                case "Edm.Decimal":
                    return typeof(decimal);
                case "Edm.Stream":
                    return typeof(Stream);
                case "Edm.Date":
                case "Edm.DateTimeOffset":
                    return typeof(DateTime);
                case "Edm.Boolean":
                    return typeof(bool);
            }
            return typeof(object);
        }

        /// <inheritdoc />
        public override ISourceReader GetReader(Mapping mapping)
        {
            SetCredentials();
            if (!string.IsNullOrEmpty(Mode))
            {
                RequestIntervals = 0;
                DoNotStoreLastResponseInLogFile = false;
            }
            _endpointService.GetEndpoints(); //needed for reset cached Endpoints as the used one is getting updated values, so it will accumulate it's parameters on each run.
            _endpoint = _endpointService.GetEndpointById(Convert.ToInt32(EndpointId));
            _endpointSourceReader = new ODataSourceReader(new HttpRestClient(_credentials, RequestTimeout), Logger, mapping, _endpoint, Mode, MaximumPageSize, RunLastRequest, RequestIntervals, DoNotStoreLastResponseInLogFile);
            return _endpointSourceReader;
        }

        /// <inheritdoc />
        public override void Close()
        {
            // Not used
        }

        public override string ValidateSourceSettings()
        {
            return null;
        }

        public override string ValidateDestinationSettings()
        {
            return null;
        }

        public override void UpdateDestinationSettings(IDestination destination)
        {
            ODataProvider newProvider = (ODataProvider)destination;
            DestinationEndpointId = newProvider.DestinationEndpointId;
            DestinationMetadataURL = newProvider.DestinationMetadataURL;
            AutodetectedDestinationMetadataURL = newProvider.AutodetectedDestinationMetadataURL;
            SetCredentials();
        }

        /// <inheritdoc />
        public override void UpdateSourceSettings(ISource source)
        {
            ODataProvider newProvider = (ODataProvider)source;
            Mode = newProvider.Mode;
            MaximumPageSize = newProvider.MaximumPageSize;
            RequestTimeout = newProvider.RequestTimeout;
            RunLastRequest = newProvider.RunLastRequest;
            RequestIntervals = newProvider.RequestIntervals;
            DoNotStoreLastResponseInLogFile = newProvider.DoNotStoreLastResponseInLogFile;
            EndpointId = newProvider.EndpointId;
            MetadataUrl = newProvider.MetadataUrl;
            AutodetectedMetadataURL = newProvider.AutodetectedMetadataURL;
            SetCredentials();
            GetEntityName();
        }

        public ODataProvider()
        {
            _workingDirectory = SystemInformation.MapPath("/Files/");
        }

        public ODataProvider(XmlNode xmlNode) : this()
        {
            foreach (XmlNode node in xmlNode.ChildNodes)
            {
                switch (node.Name)
                {
                    case "Schema":
                        _schema = new Schema(node);
                        break;
                    case "Mode":
                        if (node.HasChildNodes)
                        {
                            Mode = node.FirstChild.Value;
                        }
                        break;
                    case "Maximumpagesize":
                        if (node.HasChildNodes)
                        {
                            MaximumPageSize = Converter.ToInt32(node.FirstChild.Value);
                        }
                        break;
                    case "Requesttimeout":
                        if (node.HasChildNodes)
                        {
                            RequestTimeout = Converter.ToInt32(node.FirstChild.Value);
                        }
                        break;
                    case "Runlastrequest":
                        if (node.HasChildNodes)
                        {
                            RunLastRequest = node.FirstChild.Value == "True";
                        }
                        break;
                    case "Requestintervals":
                        if (node.HasChildNodes)
                        {
                            RequestIntervals = Converter.ToInt32(node.FirstChild.Value);
                        }
                        break;
                    case "Donotstorelastresponseinlogfile":
                        if (node.HasChildNodes)
                        {
                            DoNotStoreLastResponseInLogFile = node.FirstChild.Value == "True";
                        }
                        break;
                    case "Predefinedendpoint":
                        if (node.HasChildNodes)
                        {
                            EndpointId = node.FirstChild.Value;
                        }
                        break;
                    case "Destinationendpoint":
                        if (node.HasChildNodes)
                        {
                            DestinationEndpointId = node.FirstChild.Value;
                        }
                        break;
                    case "Metadataurl":
                        if (node.HasChildNodes)
                        {
                            MetadataUrl = node.FirstChild.Value;
                        }
                        break;
                    case "Destinationmetadataurl":
                        if (node.HasChildNodes)
                        {
                            DestinationMetadataURL = node.FirstChild.Value;
                        }
                        break;
                    case "Autodetectedmetadataurl":
                        if (node.HasChildNodes)
                        {
                            AutodetectedMetadataURL = node.FirstChild.Value;
                        }
                        break;
                    case "Autodetecteddestinationmetadataurl":
                        if (node.HasChildNodes)
                        {
                            AutodetectedDestinationMetadataURL = node.FirstChild.Value;
                        }
                        break;
                }
            }
        }

        public override string Serialize()
        {
            var document = new XDocument(new XDeclaration("1.0", "utf-8", string.Empty));
            var root = new XElement("Parameters");
            document.Add(root);
            root.Add(CreateParameterNode(GetType(), "Mode", Mode));
            root.Add(CreateParameterNode(GetType(), "Maximum page size", MaximumPageSize.ToString()));
            root.Add(CreateParameterNode(GetType(), "Request timeout (minutes)", RequestTimeout.ToString()));
            root.Add(CreateParameterNode(GetType(), "Run last request", RunLastRequest.ToString()));
            root.Add(CreateParameterNode(GetType(), "Run request in intervals (pages)", RequestIntervals.ToString()));
            root.Add(CreateParameterNode(GetType(), "Do not store last response in log file", DoNotStoreLastResponseInLogFile.ToString()));
            root.Add(CreateParameterNode(GetType(), "Predefined endpoint", EndpointId));
            root.Add(CreateParameterNode(GetType(), "Destination endpoint", DestinationEndpointId));
            root.Add(CreateParameterNode(GetType(), "Metadata url", MetadataUrl));
            root.Add(CreateParameterNode(GetType(), "Destination metadata url", DestinationMetadataURL));
            root.Add(CreateParameterNode(GetType(), "Autodetected metadata url", AutodetectedMetadataURL));
            root.Add(CreateParameterNode(GetType(), "Autodetected destination metadata url", AutodetectedDestinationMetadataURL));
            return document.ToString();
        }

        /// <inheritdoc />
        public override void SaveAsXml(XmlTextWriter textWriter)
        {
            textWriter.WriteElementString("Mode", Mode);
            textWriter.WriteElementString("Maximumpagesize", MaximumPageSize.ToString());
            textWriter.WriteElementString("Requesttimeout", RequestTimeout.ToString());
            textWriter.WriteElementString("Runlastrequest", RunLastRequest.ToString());
            textWriter.WriteElementString("Requestintervals", RequestIntervals.ToString());
            textWriter.WriteElementString("Donotstorelastresponseinlogfile", DoNotStoreLastResponseInLogFile.ToString());
            textWriter.WriteElementString("Predefinedendpoint", EndpointId);
            textWriter.WriteElementString("Destinationendpoint", DestinationEndpointId);
            textWriter.WriteElementString("Metadataurl", MetadataUrl);
            textWriter.WriteElementString("Destinationmetadataurl", DestinationMetadataURL);
            textWriter.WriteElementString("Autodetectedmetadataurl", AutodetectedMetadataURL);
            textWriter.WriteElementString("Autodetecteddestinationmetadataurl", AutodetectedDestinationMetadataURL);
            GetSchema().SaveAsXml(textWriter);
        }

        /// <inheritdoc />
        public override void Initialize()
        {
            // Not used
        }

        /// <inheritdoc />
        public override void LoadSettings(Job job)
        {
            // Not used
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

                Logger?.Log($"Begin synchronizing '{mapping.SourceTable.Name}' to '{mapping.DestinationTable.Name}'.");
                using (var writer = new ODataWriter(Logger, mapping, _endpoint, _credentials))
                {
                    using (ISourceReader sourceReader = mapping.Source.GetReader(mapping))
                    {
                        try
                        {
                            while (!sourceReader.IsDone())
                            {
                                var sourceRow = sourceReader.GetNext();
                                ProcessInputRow(mapping, sourceRow);
                                writer.Write(sourceRow);
                            }
                        }
                        catch (Exception e)
                        {
                            Logger?.Log(e.ToString());
                            throw;
                        }
                    }
                }
                Logger?.Log($"End synchronizing '{mapping.SourceTable.Name}' to '{mapping.DestinationTable.Name}'.");
            }
            Logger?.Log($"Finished OData export.");

            return true;
        }

        public static bool EndpointIsLoadAllEntities(string url)
        {
            return url.EndsWith("/") || url.EndsWith("$metadata", StringComparison.OrdinalIgnoreCase);
        }
    }
}