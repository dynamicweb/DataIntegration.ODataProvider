using Dynamicweb.Core;
using Dynamicweb.DataIntegration.EndpointManagement;
using Dynamicweb.DataIntegration.Integration;
using Dynamicweb.DataIntegration.Integration.ERPIntegration;
using Dynamicweb.DataIntegration.Integration.Interfaces;
using Dynamicweb.DataIntegration.Providers.ODataProvider.Interfaces;
using Dynamicweb.DataIntegration.Providers.ODataProvider.Model;
using Dynamicweb.Extensibility.AddIns;
using Dynamicweb.Extensibility.Editors;
using Dynamicweb.Logging;
using Dynamicweb.Security.Licensing;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Net;
using System.Net.Http;
using System.Text.RegularExpressions;
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
    [ResponseMapping(true)]
    public class ODataProvider : BaseProvider, ISource, IDestination, IParameterOptions, IODataBaseProvider
    {
        internal readonly EndpointService _endpointService = new EndpointService();
        internal Schema _schema;
        internal Endpoint _endpoint;
        internal ICredentials _credentials;
        private const string OldBCBatch = "eCom_DataIntegrationERPBatch";
        private const string BCBatch = "eCom_DataIntegrationERPBatch_BC";
        private const string CRMBatch = "eCom_DataIntegrationERPBatch_CRM";
        private const string FOBatch = "eCom_DataIntegrationERPBatch_FO";
        private const string GenericOData = "eCom_DataIntegrationODataGeneric";
        private const string CRMTrilingVersionPattern = "\\/v[0-9]*.[0-9]*";
        private const string CRMCloudRegexPattern = "https:\\/\\/[0-9a-z]+.crm[0-9]*.dynamics.com\\/[0-9a-z]*";
        private const string FOCloudRegexPattern = "https://.+.(((ax)?cloud(ax)?)|(operations)).dynamics.com/(data[0-9a-z])*";
        internal IHttpRestClient Client => new HttpRestClient(_credentials, 20);

        #region AddInManager/ConfigurableAddIn Source

        [AddInParameter("Predefined endpoint")]
        [AddInParameterEditor(typeof(GroupedDropDownParameterEditor), "none=true;refreshParameters=true;required=true")]
        [AddInParameterGroup("Source")]
        [AddInParameterSection("Source")]
        public string EndpointId
        {
            get => _endpoint?.Id.ToString();
            set
            {
                _endpoint = _endpointService.GetEndpointById(Convert.ToInt32(value));
                SetCredentials();
            }
        }

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

        [AddInParameter("Run last response")]
        [AddInParameterEditor(typeof(YesNoParameterEditor), "Tooltip=Runs the job from the last saved response instead of calling the endpoint.")]
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

        [AddInParameter("Delta modifier")]
        [AddInParameterEditor(typeof(TextParameterEditor), "infoText=Add your own delta properties. Default looking at these properties: Last_Date_Modified, Order_Date, LastDateTimeModified, lastModifiedDateTime and modifiedon.;inputClass=NewUIinput;")]
        [AddInParameterGroup("Source")]
        [AddInParameterSection("Advanced activity settings")]
        public string DeltaModifier { get; set; }

        #endregion

        #region AddInManager/ConfigurableAddIn Destination

        [AddInParameter("Destination endpoint")]
        [AddInParameterEditor(typeof(GroupedDropDownParameterEditor), "none=true;refreshParameters=true;required=true")]
        [AddInParameterGroup("Destination")]
        public string DestinationEndpointId
        {
            get => _endpoint?.Id.ToString();
            set => _endpoint = _endpointService.GetEndpointById(Convert.ToInt32(value));
        }

        [AddInParameter("Continue on error")]
        [AddInParameterEditor(typeof(YesNoParameterEditor), "Tooltip=Use this when the job should continue when endpoint returns an error.")]
        [AddInParameterGroup("Destination")]
        public bool ContinueOnError { get; set; }

        #endregion

        private string GetMetadataURL()
        {
            if (_endpoint.Url.Contains("companies(", StringComparison.OrdinalIgnoreCase))
            {
                return _endpoint.Url.Substring(0, _endpoint.Url.IndexOf("companies(", StringComparison.OrdinalIgnoreCase)) + "$metadata";
            }
            else if (_endpoint.Url.Contains("company(", StringComparison.OrdinalIgnoreCase))
            {
                return _endpoint.Url.Substring(0, _endpoint.Url.IndexOf("company(", StringComparison.OrdinalIgnoreCase)) + "$metadata";
            }
            else
            {
                string url = _endpoint.Url;
                if (EndpointIsLoadAllEntities(url))
                {
                    if (!url.EndsWith("/") && !url.EndsWith("metadata", StringComparison.OrdinalIgnoreCase))
                    {
                        url += "/";
                    }
                }
                return new Uri(new Uri(url), "$metadata").AbsoluteUri;
            }
        }

        private string GetEntityName()
        {
            return new Uri(_endpoint.Url).Segments.LastOrDefault().TrimEnd('/') ?? "";
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

        IEnumerable<ParameterOption> IParameterOptions.GetParameterOptions(string parameterName)
        {
            switch (parameterName ?? "")
            {
                case "Mode":
                    {
                        return new List<ParameterOption>()
                        {
                            { new("Delta replication", "Delta Replication") { Hint = "This mode filters records on date and time, whenever possible, and it only acts on new or updated records. It never deletes." } },
                            { new("First page", "First page") { Hint = "If maximum page size is 100 then this setting only handles the 100 records of the first page." } }
                        };
                    }

                case "Destination endpoint":
                case "Predefined endpoint":
                    {
                        var result = new List<ParameterOption>();
                        foreach (var endpoint in _endpointService.GetEndpoints())
                        {
                            var value = new GroupedDropDownParameterEditor.DropDownItem(endpoint.Name, endpoint.Collection != null ? endpoint.Collection.Name : "Dynamicweb 9 Endpoints", endpoint.Id.ToString());
                            result.Add(new(endpoint.Name, value) { Group = endpoint.Collection != null ? endpoint.Collection.Name : "Dynamicweb 9 Endpoints" });
                        }
                        return result;
                    }

                default:
                    {
                        return null;
                    }
            }
        }

        /// <inheritdoc />
        public override bool SchemaIsEditable => false;

        private ErpType? _erpType = null;

        public ErpType ErpType
        {
            get
            {
                if (_erpType is null)
                    _erpType = GetErpType();
                return !_erpType.HasValue ? ErpType.Undefined : _erpType.Value;
            }
        }


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
                if (endpointAuthentication != null)
                {
                    SetCredentials();
                }
                Task metadataResponse;
                if (endpointAuthentication.IsTokenBased())
                {
                    string token = OAuthHelper.GetToken(_endpoint, endpointAuthentication);
                    metadataResponse = new HttpRestClient(_credentials, 20).GetAsync(GetMetadataURL(), HandleStream, token);
                }
                else
                {
                    metadataResponse = new HttpRestClient(_credentials, 20).GetAsync(GetMetadataURL(), HandleStream, endpointAuthentication, header);
                }
                metadataResponse.Wait();
            }

            var emptySchema = new Schema();
            if (entitySetsTables == emptySchema)
            {
                Logger?.Error("Error getting a new schema.");
            }
            return entitySetsTables != emptySchema ? entitySetsTables : _schema ?? emptySchema;

            void HandleStream(Stream responseStream, HttpStatusCode responseStatusCode, Dictionary<string, string> responseHeaders)
            {
                try
                {
                    var xmlReader = XmlReader.Create(responseStream);
                    using (xmlReader)
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
                        if (!EndpointIsLoadAllEntities(_endpoint.Url))
                        {
                            var singleEntitySetSelected = entitySetsTables.GetTables().FirstOrDefault(obj => obj.Name.Equals(name, StringComparison.OrdinalIgnoreCase));
                            entitySetsTables = new Schema();
                            if (singleEntitySetSelected != null)
                            {
                                entitySetsTables.AddTable(singleEntitySetSelected);
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
                        table.AddColumn(new Column(item.Name, item.Type, table, item.IsPrimaryKey, item.IsNew, item.ReadOnly));
                    }
                }
            }
        }

        private void AddPropertiesFromXMLReaderToTable(XmlReader xmlReader, Table table, Schema result)
        {
            string baseType = xmlReader.GetAttribute("BaseType");
            string entityName = xmlReader.GetAttribute("Name");
            List<string> primaryKeys = new List<string>();
            Column column = null;
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
                    column = new Column(columnName, columnType, table, isPrimaryKey, false);
                    table.AddColumn(column);
                }
                else if (xmlReader.NodeType == XmlNodeType.Element && xmlReader.Name.Equals("Annotation", StringComparison.OrdinalIgnoreCase) && column is not null)
                {
                    // BC & FO
                    var term = xmlReader.GetAttribute("Term");
                    var value = xmlReader.GetAttribute("Bool");
                    if (!string.IsNullOrEmpty(term) && term.Contains("AllowEdit") && !string.IsNullOrEmpty(value) && !Converter.ToBoolean(value))
                        column.ReadOnly = true;
                }
                else if (xmlReader.NodeType == XmlNodeType.Element && xmlReader.Name.Equals("EnumMember", StringComparison.OrdinalIgnoreCase) && column is not null)
                {
                    // CRM
                    string permission = xmlReader.Value;
                    if (!string.IsNullOrEmpty(permission) && permission.ToLower().EndsWith("permissiontype/read"))
                        column.ReadOnly = true;
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
                            table.AddColumn(new Column(item.Name, item.Type, table, item.IsPrimaryKey, item.IsNew, item.ReadOnly));
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
            if (!CheckLicense())
            {
                throw new Exception("License check for OData failed.");
            }

            if (!string.IsNullOrEmpty(Mode))
            {
                RequestIntervals = 0;
                DoNotStoreLastResponseInLogFile = false;
            }

            return new ODataSourceReader(new HttpRestClient(_credentials, RequestTimeout), Logger, mapping, _endpoint, Mode, DeltaModifier, MaximumPageSize, RunLastRequest, RequestIntervals, DoNotStoreLastResponseInLogFile);
        }

        /// <inheritdoc />
        public override void Close()
        {
            // Not used
        }

        public override string ValidateSourceSettings()
        {
            SetCredentials();
            if (string.IsNullOrEmpty(EndpointId))
            {
                return "Predefined endpoint can not be empty. Please select any predefined endpoint.";
            }
            if (_endpoint?.Authentication == null)
            {
                return "Credentials not set for endpoint, please add credentials before continue.";
            }
            var endpointStatusCode = GetEndpointResponse(_endpoint.Url, out string _endpointResponse, out Exception exception);
            if (exception != null)
            {
                return $"{exception.Message}";
            }
            if (!new HttpResponseMessage(endpointStatusCode).IsSuccessStatusCode)
            {
                return $"Endpoint returned statuscode: {endpointStatusCode} with response: {_endpointResponse}";
            }
            if (!CheckLicense())
            {
                return "License error: no Batch Integration module is installed for this Endpoint.";
            }
            return null;
        }

        public override string ValidateDestinationSettings()
        {
            SetCredentials();
            if (string.IsNullOrEmpty(DestinationEndpointId))
            {
                return "Destination endpoint can not be empty. Please select any destination endpoint";
            }
            if (_endpoint?.Authentication == null)
            {
                return "Credentials not set for endpoint, please add credentials before continue.";
            }
            var endpointStatusCode = GetEndpointResponse(_endpoint.Url, out string _endpointResponse, out Exception exception);
            if (exception != null)
            {
                return $"{exception.Message}";
            }
            if (!new HttpResponseMessage(endpointStatusCode).IsSuccessStatusCode)
            {
                return $"Endpoint returned statuscode: {endpointStatusCode} with response: {_endpointResponse}";
            }
            if (!CheckLicense())
            {
                return "License error: no Batch Integration module is installed for this Endpoint.";
            }
            return null;
        }

        public override void UpdateDestinationSettings(IDestination destination)
        {
            ODataProvider newProvider = (ODataProvider)destination;
            DestinationEndpointId = newProvider.DestinationEndpointId;
            ContinueOnError = newProvider.ContinueOnError;
        }

        /// <inheritdoc />
        public override void UpdateSourceSettings(ISource source)
        {
            ODataProvider newProvider = (ODataProvider)source;
            Mode = newProvider.Mode;
            DeltaModifier = newProvider.DeltaModifier;
            MaximumPageSize = newProvider.MaximumPageSize;
            RequestTimeout = newProvider.RequestTimeout;
            RunLastRequest = newProvider.RunLastRequest;
            RequestIntervals = newProvider.RequestIntervals;
            DoNotStoreLastResponseInLogFile = newProvider.DoNotStoreLastResponseInLogFile;
            EndpointId = newProvider.EndpointId;
        }

        public ODataProvider() { }

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
                    case "Deltamodifier":
                        if (node.HasChildNodes)
                        {
                            DeltaModifier = node.FirstChild.Value;
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
                    case "Continueonerror":
                        if (node.HasChildNodes)
                        {
                            ContinueOnError = node.FirstChild.Value == "True";
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
            root.Add(CreateParameterNode(GetType(), "Delta modifier", DeltaModifier));
            root.Add(CreateParameterNode(GetType(), "Maximum page size", MaximumPageSize.ToString()));
            root.Add(CreateParameterNode(GetType(), "Request timeout (minutes)", RequestTimeout.ToString()));
            root.Add(CreateParameterNode(GetType(), "Run last response", RunLastRequest.ToString()));
            root.Add(CreateParameterNode(GetType(), "Run request in intervals (pages)", RequestIntervals.ToString()));
            root.Add(CreateParameterNode(GetType(), "Do not store last response in log file", DoNotStoreLastResponseInLogFile.ToString()));
            root.Add(CreateParameterNode(GetType(), "Predefined endpoint", EndpointId));
            root.Add(CreateParameterNode(GetType(), "Destination endpoint", DestinationEndpointId));
            root.Add(CreateParameterNode(GetType(), "Continue on error", ContinueOnError.ToString()));
            return document.ToString();
        }

        /// <inheritdoc />
        public override void SaveAsXml(XmlTextWriter textWriter)
        {
            textWriter.WriteElementString("Mode", Mode);
            textWriter.WriteElementString("Deltamodifier", DeltaModifier);
            textWriter.WriteElementString("Maximumpagesize", MaximumPageSize.ToString());
            textWriter.WriteElementString("Requesttimeout", RequestTimeout.ToString());
            textWriter.WriteElementString("Runlastrequest", RunLastRequest.ToString());
            textWriter.WriteElementString("Requestintervals", RequestIntervals.ToString());
            textWriter.WriteElementString("Donotstorelastresponseinlogfile", DoNotStoreLastResponseInLogFile.ToString());
            textWriter.WriteElementString("Predefinedendpoint", EndpointId);
            textWriter.WriteElementString("Destinationendpoint", DestinationEndpointId);
            textWriter.WriteElementString("Continueonerror", ContinueOnError.ToString());
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
            SetCredentials();
            if (!CheckLicense())
            {
                return false;
            }
            ReplaceMappingConditionalsWithValuesFromRequest(job);

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

                if (!mapping.Active || mapping.GetColumnMappings().Count == 0)
                {
                    Logger?.Log($"There are no active mappings between '{mapping.SourceTable.Name}' and '{mapping.DestinationTable.Name}'.");
                    continue;
                }

                Logger?.Log($"Begin synchronizing '{mapping.SourceTable.Name}' to '{mapping.DestinationTable.Name}'.");

                using (var writer = new ODataWriter(Logger, mapping, _endpoint, _credentials, ContinueOnError))
                {
                    using (ISourceReader sourceReader = mapping.Source.GetReader(mapping))
                    {
                        try
                        {
                            bool sourceReaderIsResponseWriter = sourceReader is IResponseWriter;
                            IResponseWriter responseMappingWriter = null;
                            if (sourceReaderIsResponseWriter)
                            {
                                responseMappingWriter = (IResponseWriter)sourceReader;
                            }
                            var responseMappingCollection = mapping.GetResponseColumnMappings();
                            while (!sourceReader.IsDone())
                            {
                                var sourceRow = sourceReader.GetNext();
                                ProcessInputRow(mapping, sourceRow);
                                writer.Write(sourceRow);
                                if (sourceReaderIsResponseWriter)
                                {
                                    if (writer.PostBackObject != null && writer.PostBackObject.Count > 0)
                                    {
                                        Dictionary<string, object> responseToWrite = new Dictionary<string, object>();
                                        foreach (var item in responseMappingCollection)
                                        {
                                            if (item.HasScriptWithValue)
                                            {
                                                responseToWrite.Add(item.DestinationColumn.Name, item.GetScriptValue());
                                            }
                                            else
                                            {
                                                var postBackValue = writer.GetPostBackValue(item);
                                                if (postBackValue != null)
                                                {
                                                    responseToWrite.Add(item.DestinationColumn.Name, postBackValue);
                                                }
                                            }
                                        }
                                        responseMappingWriter.Write(responseToWrite);
                                    }
                                }
                            }
                            if (responseMappingWriter != null)
                            {
                                responseMappingWriter.Close();
                            }
                        }
                        catch (Exception e)
                        {
                            string msg = e.Message;
                            string stackTrace = e.StackTrace;

                            Logger?.Log(e.ToString());
                            Logger?.Error($"Error: {msg.Replace(System.Environment.NewLine, " ")} Stack: {stackTrace.Replace(System.Environment.NewLine, " ")}", e);
                            LogManager.System.GetLogger(LogCategory.Application, "Dataintegration").Error($"{GetType().Name} error: {msg} Stack: {stackTrace}", e);
                            return false;
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
            bool result = url.EndsWith("/data", StringComparison.OrdinalIgnoreCase) ||
                url.EndsWith("$metadata", StringComparison.OrdinalIgnoreCase) ||
                Regex.IsMatch(url.Substring(url.LastIndexOf("/")), CRMTrilingVersionPattern);
            if (!result && !string.IsNullOrEmpty(url))
            {
                var uri = new Uri(url.ToLower());
                // Checking this kind of url https://api.businesscentral.dynamics.com/v2.0/7dd45d63-24fc-4edd-8ad8-5fd66b6f9733/BC17/ODataV4/Company('CRONUS%20UK%20Ltd.')
                // Or this kind of url https://api.businesscentral.dynamics.com/v2.0/7dd45d63-24fc-4edd-8ad8-5fd66b6f9733/bc17/api/v2.0/companies(0f8058e0-affd-ea11-bb48-000d3a2fece2)
                if (string.IsNullOrEmpty(uri.Query))
                {
                    var lastSegment = uri.Segments.LastOrDefault();
                    if (!string.IsNullOrEmpty(lastSegment))
                    {
                        var companyRegex = new Regex(@"company\(\'(.)+\'\)");
                        var companiesRegex = new Regex(@"companies\((.)+\)");
                        if (companyRegex.IsMatch(lastSegment) || companiesRegex.IsMatch(lastSegment))
                        {
                            result = true;
                        }
                    }
                }
            }
            return result;
        }

        private bool CheckLicense()
        {
            return LicenseManager.LicenseHasFeature(BCBatch) ||
               LicenseManager.LicenseHasFeature(OldBCBatch) ||
               LicenseManager.LicenseHasFeature(FOBatch) ||
               LicenseManager.LicenseHasFeature(CRMBatch) ||
               LicenseManager.LicenseHasFeature(GenericOData);
        }

        private ErpType GetErpType()
        {
            string url = _endpoint?.Url;
            if (!string.IsNullOrEmpty(url))
            {
                ErpType? type = null;
                if (Regex.IsMatch(url, FOCloudRegexPattern))
                {
                    type = ErpType.FinanceAndOperations;
                }
                else if (Regex.IsMatch(url, CRMCloudRegexPattern))
                {
                    type = ErpType.CRM;
                }
                else if (url.Contains(".businesscentral.dynamics.com", StringComparison.OrdinalIgnoreCase))
                {
                    type = ErpType.BusinessCentral;
                }
                if (type is null)
                {
                    if (IsBCEndpoint())
                    {
                        type = ErpType.BusinessCentral;
                    }
                    else if (IsFOEnpoint(url))
                    {
                        type = ErpType.FinanceAndOperations;
                    }
                    else if (IsCRMEndpoint(url))
                    {
                        type = ErpType.CRM;
                    }
                }
                return type is null ? ErpType.Undefined : type.Value;
            }
            return ErpType.Undefined;
        }

        private bool IsFOEnpoint(string url)
        {
            bool result = false;
            HttpStatusCode response = GetEndpointResponse($"{new Uri(url).GetLeftPart(UriPartial.Authority)}/data.svc", out string _endpointResponse, out _);
            if (new HttpResponseMessage(response).IsSuccessStatusCode && !string.IsNullOrEmpty(_endpointResponse))
            {
                if (_endpointResponse.Contains("Microsoft Dynamics 365 Finance and Operations", StringComparison.OrdinalIgnoreCase))
                {
                    result = true;
                }
            }
            return result;
        }

        private bool IsCRMEndpoint(string url)
        {
            bool result = false;
            HttpStatusCode response = GetEndpointResponse(url, out string _endpointResponse, out _);
            if (new HttpResponseMessage(response).IsSuccessStatusCode && !string.IsNullOrEmpty(_endpointResponse))
            {
                if (_endpointResponse.Contains("<title>Microsoft Dynamics 365</title>", StringComparison.OrdinalIgnoreCase))
                {
                    result = true;
                }
            }
            return result;
        }

        private bool IsBCEndpoint()
        {
            bool result = false;
            HttpStatusCode response = GetEndpointResponse(GetMetadataURL(), out string _endpointResponse, out _);
            if (new HttpResponseMessage(response).IsSuccessStatusCode && !string.IsNullOrEmpty(_endpointResponse))
            {
                if (_endpointResponse.Contains("<Schema Namespace=\"Microsoft.NAV\"", StringComparison.OrdinalIgnoreCase)
                    || _endpointResponse.Contains("<Schema Namespace=\"NAV\"", StringComparison.OrdinalIgnoreCase))
                {
                    result = true;
                }
            }
            return result;
        }

        private HttpStatusCode GetEndpointResponse(string url, out string endpointResponse, out Exception exception)
        {
            string _endpointResponse = "";
            exception = null;
            try
            {
                HttpStatusCode result = HttpStatusCode.NotFound;
                Task task;
                var endpointAuthentication = _endpoint.Authentication;
                if (endpointAuthentication.IsTokenBased())
                {
                    string token = OAuthHelper.GetToken(_endpoint, endpointAuthentication, out Exception error);
                    exception = error;
                    task = Client.GetAsync(url, HandleResponse, token);
                }
                else
                {
                    task = Client.GetAsync(url, HandleResponse, endpointAuthentication);
                }
                task.Wait();
                void HandleResponse(Stream responseStream, HttpStatusCode responseStatusCode, Dictionary<string, string> responseHeaders)
                {
                    using (var stream = new StreamReader(responseStream))
                    {
                        _endpointResponse = stream.ReadToEnd();
                        result = responseStatusCode;
                    }
                }
                endpointResponse = _endpointResponse;
                return result;
            }
            catch (Exception ex)
            {
                Logger?.Error($"Error GetEndpointResponse url: {url}", ex);
            }
            endpointResponse = _endpointResponse;
            return HttpStatusCode.NotFound;
        }
    }
}
