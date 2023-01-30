using Dynamicweb.Data;
using Dynamicweb.DataIntegration.EndpointManagement;
using Dynamicweb.DataIntegration.Integration;
using Dynamicweb.DataIntegration.Providers.ODataProvider.Model;
using Dynamicweb.Ecommerce.Orders;
using Dynamicweb.Ecommerce.Prices;
using Dynamicweb.Logging;
using Dynamicweb.Security.Permissions;
using Dynamicweb.Security.UserManagement;
using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.Globalization;
using System.Linq;
using System.Net;
using System.Threading.Tasks;

namespace Dynamicweb.DataIntegration.Providers.ODataProvider
{
    public class ODataDestinationWriter
    {
        private SqlConnection _connection;
        public readonly ILogger Logger;
        public readonly bool ImportAll;
        public readonly string LanguageId;
        public List<string> ItemsToBeDeleted;
        public bool RowsToBeDeleted;
        public readonly OrderService OrderService;
        public readonly bool DeleteMissingRows;
        public readonly string DatabaseDiscountExternalId = "ERP Import";
        public readonly string DiscountGroupPrefix = "Disc_";
        public readonly string PriceGroupPrefix = "Price_";
        private bool? databasePriceHasExternalIdColumn;
        private EndpointAuthentication _endpointAuthentication;
        private readonly EndpointAuthenticationService _endpointAuthenticationService = new EndpointAuthenticationService();
        private readonly EndpointService _endpointService = new EndpointService();
        public readonly Endpoint Endpoint;
        private AuthenticationHelper AuthenticationHelper = new AuthenticationHelper();
        public Mapping Mapping { get; }
        internal ODataDestinationWriter(ILogger logger, Mapping mapping, bool importAll, string languageId, OrderService orderService, bool deleteMissingRows, string endpointId)
        {
            Logger = logger;
            ImportAll = importAll;
            LanguageId = languageId;
            RowsToBeDeleted = false;
            ItemsToBeDeleted = new List<string>();
            Mapping = mapping;
            OrderService = orderService;
            DeleteMissingRows = deleteMissingRows;
            Endpoint = _endpointService.GetEndpointById(Convert.ToInt32(endpointId));
        }
        public void SetPermissionOnGroup(int userGroupID, string groupName)
        {
            UnifiedPermissionService unifiedPermissionService = new UnifiedPermissionService();
            UnifiedPermissionIdentifier unifiedPermissionIdentifier = new UnifiedPermissionIdentifier(Group.GetGroupByID(userGroupID).Subgroups.Where(obj => obj.Name == groupName).FirstOrDefault().ID.ToString(), "UserGroup");
            unifiedPermissionService.SetPermission("AuthenticatedBackend", unifiedPermissionIdentifier, PermissionLevel.Read);
        }
        private string GetPlainUrl(string url)
        {
            string result = url;
            if (url.Contains("?$"))
            {
                result = url.Substring(0, url.IndexOf("?"));
            }
            return result;
        }
        internal List<SalesPriceListLine> GetSalesPriceListLinesFromBC()
        {
            List<SalesPriceListLine> result = new List<SalesPriceListLine>();
            try
            {
                string url = GetPlainUrl(Endpoint.Url);
                var header = new Dictionary<string, string>
                {
                    {"Accept", "application/xml"},
                };
                result = GetFromBC<SalesPriceListLine>(url, header, Endpoint);
            }
            catch (Exception ex)
            {
                Logger?.Error("GetSalesPriceListLinesFromBC went wrong! " + ex.ToString());
            }
            return result;
        }
        internal List<OrderLines> GetOrderLinesFromBC(string documentNo, bool orderIsAPostedOrderInBC)
        {
            List<OrderLines> result = new List<OrderLines>();
            try
            {
                string tempUrl = GetPlainUrl(Endpoint.Url);
                string url;
                if (orderIsAPostedOrderInBC)
                {
                    url = tempUrl + "SalesInvLines?$filter=Document_No eq '" + documentNo + "'";
                }
                else
                {
                    url = tempUrl + "SalesLines?$filter=Document_No eq '" + documentNo + "'";
                }
                var header = new Dictionary<string, string>
                {
                    {"Accept", "application/xml"},
                };
                result = GetFromBC<OrderLines>(url, header, Endpoint);
            }
            catch (Exception ex)
            {
                Logger?.Error("GetOrderLinesFromBC went wrong! " + ex.ToString());
            }
            return result;
        }
        internal List<SalesLineDiscount> GetSalesLineDiscountsFromBC(string salesType)
        {
            List<SalesLineDiscount> result = new List<SalesLineDiscount>();
            try
            {
                string tempUrl = GetPlainUrl(Endpoint.Url);
                string url = tempUrl + $"?$filter=SalesType eq '{salesType}'";
                var header = new Dictionary<string, string>
                {
                    {"Accept", "application/xml"},
                };
                result = GetFromBC<SalesLineDiscount>(url, header, Endpoint);
            }
            catch (Exception ex)
            {
                Logger?.Error("GetSalesLineDiscountsFromBC went wrong! " + ex.ToString());
            }
            return result;
        }
        public bool CheckIfDiscountExternalIdExistsInDatabase()
        {
            using (var reader = Database.CreateDataReader("SELECT TOP(1) * FROM [EcomDiscount]"))
            {
                return Database.ColumnExists(reader, "DiscountExternalId");
            }
        }
        public List<T> GetFromBC<T>(string URL, Dictionary<string, string> header, Endpoint endpoint)
        {
            _endpointAuthentication = _endpointAuthenticationService.GetEndpointAuthenticationById(endpoint.AuthenticationId);
            var _client = new HttpRestClient(GetCredentials(endpoint), 20, Logger);
            Task<RestResponse<ResponseFromERP<T>>> awaitResponseFromBC;
            if (AuthenticationHelper.IsTokenBased(_endpointAuthentication))
            {
                string token = AuthenticationHelper.GetToken(endpoint, _endpointAuthentication);
                awaitResponseFromBC = _client.GetAsync<ResponseFromERP<T>>(URL, token);
            }
            else
            {
                awaitResponseFromBC = _client.GetAsync<ResponseFromERP<T>>(URL, _endpointAuthentication, header);
            }
            awaitResponseFromBC.Wait();
            List<T> result = awaitResponseFromBC.Result.Content.Value;
            if (!string.IsNullOrEmpty(awaitResponseFromBC.Result.Content.OdataNextLink))
            {
                result.AddRange(GetFromBC<T>(awaitResponseFromBC.Result.Content.OdataNextLink, header, endpoint));
            }
            return result;
        }
        private ICredentials GetCredentials(Endpoint endpoint)
        {
            var credentials = new CredentialCache();
            credentials.Add(new Uri(new Uri(endpoint.Url).GetLeftPart(UriPartial.Authority)), _endpointAuthentication.Type.ToString(), _endpointAuthentication.GetNetworkCredential());
            return credentials;
        }
        public List<string> GetAllProductIDs(string languageID, bool onlyVariants, int commandTimeout)
        {
            List<string> result = new List<string>();
            string sql = "SELECT [ProductId], [ProductVariantId] FROM [EcomProducts] WHERE [ProductLanguageId] = @ProductLanguageId";
            if (onlyVariants)
            {
                sql += " AND [ProductVariantId] <> ''";
            }
            else
            {
                sql += " AND [ProductVariantId] = ''";
            }
            using (var command = new SqlCommand(sql, Connection))
            {
                command.CommandTimeout = commandTimeout;
                command.Parameters.AddWithValue("@ProductLanguageId", languageID);
                OpenConnection();
                using (var reader = command.ExecuteReader())
                {
                    while (reader.Read())
                    {
                        if (onlyVariants)
                        {
                            result.Add(reader["ProductId"].ToString() + ";" + reader["ProductVariantId"].ToString());
                        }
                        else
                        {
                            result.Add(reader["ProductId"].ToString());
                        }
                    }
                }
                Connection.Close();
            }
            return result;
        }
        public string GetCountryDisplayName(string value)
        {
            string result = "";
            if (!string.IsNullOrWhiteSpace(value))
            {
                result = new RegionInfo(value).DisplayName;
            }
            return result;
        }
        public List<string> GetAllOrderIDsFormShopAndLedgerEntry(string shopID, bool isLedgerEntry, int commandTimeout)
        {
            List<string> result = new List<string>();
            string sql = "SELECT [OrderId] FROM [EcomOrders] WHERE [OrderShopId] = @OrderShopId AND [OrderIntegrationOrderId] <> '' AND [OrderIsLedgerEntry] = @OrderIsLedgerEntry";
            using (var command = new SqlCommand(sql, Connection))
            {
                command.CommandTimeout = commandTimeout;
                command.Parameters.AddWithValue("@OrderShopId", shopID);
                command.Parameters.AddWithValue("@OrderIsLedgerEntry", isLedgerEntry);
                OpenConnection();
                using (var reader = command.ExecuteReader())
                {
                    while (reader.Read())
                    {
                        result.Add(reader["OrderId"].ToString());
                    }
                }
                Connection.Close();
            }
            return result;
        }
        public void RemoveUsersFromDiscountAndPriceGroups()
        {
            List<Group> result = new List<Group>();
            string sql = $"SELECT [AccessUserId] FROM [AccessUser] WHERE [AccessUserUserName] like '{DiscountGroupPrefix}%' or [AccessUserUserName] like '{PriceGroupPrefix}%'";
            using (var command = new SqlCommand(sql, Connection))
            {
                OpenConnection();
                using (var reader = command.ExecuteReader())
                {
                    while (reader.Read())
                    {
                        result.Add(Group.GetGroupByID(Convert.ToInt32(reader["AccessUserId"])));
                    }
                }
                Connection.Close();
            }
            foreach (var userGroup in result)
            {
                foreach (var user in userGroup.Users)
                {
                    userGroup.RemoveUser(user.ID);
                    user.RemoveFromGroup(userGroup.ID);
                    user.Save();
                }
                userGroup.Save();
            }
        }
        public Group GetGroupFromNameAndParentGroupId(string name, int parentGroupId)
        {
            Group result = null;
            string sql = $"SELECT [AccessUserId] FROM [AccessUser] WHERE [AccessUserParentId] = {parentGroupId} AND [AccessUserUserName] = '{name}'";
            using (var command = new SqlCommand(sql, Connection))
            {
                OpenConnection();
                using (var reader = command.ExecuteReader())
                {
                    while (reader.Read())
                    {
                        result = Group.GetGroupByID(Convert.ToInt32(reader["AccessUserId"]));
                    }
                }
                Connection.Close();
            }
            return result;
        }
        public List<string> GetAllUserIDSFromUserGroup(int userGroupID, int commandTimeout)
        {
            List<string> result = new List<string>();
            string sql = "SELECT [AccessUserId] FROM [AccessUser] WHERE [AccessUserGroups] LIKE @AccessUserGroups";
            using (var command = new SqlCommand(sql, Connection))
            {
                command.CommandTimeout = commandTimeout;
                command.Parameters.AddWithValue("@AccessUserGroups", "%@" + userGroupID.ToString() + "@%");
                OpenConnection();
                using (var reader = command.ExecuteReader())
                {
                    while (reader.Read())
                    {
                        result.Add(reader["AccessUserId"].ToString());
                    }
                }
                Connection.Close();
            }
            return result;
        }
        public List<string> GetAllUserNamesFromUserGroup(int userGroupID, int commandTimeout)
        {
            List<string> result = new List<string>();
            string sql = "SELECT [AccessUserUserName] FROM [AccessUser] WHERE [AccessUserGroups] LIKE @AccessUserGroups";
            using (var command = new SqlCommand(sql, Connection))
            {
                command.CommandTimeout = commandTimeout;
                command.Parameters.AddWithValue("@AccessUserGroups", "%@" + userGroupID.ToString() + "@%");
                OpenConnection();
                using (var reader = command.ExecuteReader())
                {
                    while (reader.Read())
                    {
                        result.Add(reader["AccessUserUserName"].ToString());
                    }
                }
                Connection.Close();
            }
            return result;
        }
        public User GetUserByExternalID(string externalID)
        {
            User result = null;
            string sql = "SELECT [AccessUserId] FROM [AccessUser] WHERE [AccessUserExternalId] = @AccessUserExternalId";
            using (var command = new SqlCommand(sql, Connection))
            {
                command.Parameters.AddWithValue("@AccessUserExternalId", externalID);
                OpenConnection();
                using (var reader = command.ExecuteReader())
                {
                    if (reader.Read())
                    {
                        result = User.GetUserByID(Convert.ToInt32(reader["AccessUserId"]));
                    }
                }
                Connection.Close();
            }
            return result;
        }
        public List<User> GetUsersByCustomerNumber(string customerNumber)
        {
            List<User> result = new List<User>();
            string sql = "SELECT [AccessUserId] FROM [AccessUser] WHERE [AccessUserCustomerNumber] = @AccessUserCustomerNumber";
            using (var command = new SqlCommand(sql, Connection))
            {
                command.Parameters.AddWithValue("@AccessUserCustomerNumber", customerNumber);
                OpenConnection();
                using (var reader = command.ExecuteReader())
                {
                    while (reader.Read())
                    {
                        result.Add(User.GetUserByID(Convert.ToInt32(reader["AccessUserId"])));
                    }
                }
                Connection.Close();
            }
            return result;
        }
        public List<User> GetUsersByExternalIDAndCompany(string externalID, string company, bool equalToExternalID, int commandTimeout)
        {
            List<User> result = new List<User>();
            string sql = "SELECT [AccessUserId] FROM [AccessUser] WHERE [AccessUserCompany] = @AccessUserCompany";
            if (equalToExternalID)
            {
                sql += " AND [AccessUserExternalId] = @AccessUserExternalId";
            }
            else
            {
                sql += " AND [AccessUserExternalId] <> @AccessUserExternalId";
            }
            using (var command = new SqlCommand(sql, Connection))
            {
                command.CommandTimeout = commandTimeout;
                command.Parameters.AddWithValue("@AccessUserExternalId", externalID);
                command.Parameters.AddWithValue("@AccessUserCompany", company);
                OpenConnection();
                using (var reader = command.ExecuteReader())
                {
                    while (reader.Read())
                    {
                        result.Add(User.GetUserByID(Convert.ToInt32(reader["AccessUserId"])));
                    }
                }
                Connection.Close();
            }
            return result;
        }
        public List<UserAddress> GetAllUserAddressesByUserGroup(int userGroupID)
        {
            List<UserAddress> result = new List<UserAddress>();
            List<string> userIDs = GetAllUserIDSFromUserGroup(userGroupID, 360);
            foreach (string userID in userIDs)
            {
                result.AddRange(UserAddress.GetUserAddresses(Convert.ToInt32(userID)));
            }
            return result;
        }
        public Order GetOrderByExternalDocumentNo(string external_Document_No)
        {
            Order result = null;
            string sql = "SELECT OrderId FROM EcomOrders WITH(NOLOCK) WHERE [OrderId] = @ExternalDocumentNo";
            using (var command = new SqlCommand(sql, Connection))
            {
                command.Parameters.Clear();
                command.Parameters.AddWithValue("@ExternalDocumentNo", external_Document_No);
                OpenConnection();
                using (var reader = command.ExecuteReader())
                {
                    if (reader.Read())
                    {
                        result = OrderService.GetById(reader["OrderId"].ToString());
                    }
                }
                Connection.Close();
            }
            return result;
        }
        public Order GetOrderByOrderIntegrationOrderId(string OrderIntegrationOrderId)
        {
            Order result = null;
            string sql = "SELECT OrderId FROM EcomOrders WITH(NOLOCK) WHERE [OrderIntegrationOrderId] = @OrderIntegrationOrderId";
            using (var command = new SqlCommand(sql, Connection))
            {
                command.Parameters.Clear();
                command.Parameters.AddWithValue("@OrderIntegrationOrderId", OrderIntegrationOrderId);
                OpenConnection();
                using (var reader = command.ExecuteReader())
                {
                    if (reader.Read())
                    {
                        result = OrderService.GetById(reader["OrderId"].ToString());
                    }
                }
                Connection.Close();
            }
            return result;
        }
        public List<Price> GetPriceByExternalId(string externalId)
        {
            List<Price> result = new List<Price>();
            if (databasePriceHasExternalIdColumn == null)
            {
                string sql = "SELECT TOP(1) * FROM [EcomPrices]";
                using (var command = new SqlCommand(sql, Connection))
                {
                    OpenConnection();
                    using (var reader = command.ExecuteReader())
                    {
                        databasePriceHasExternalIdColumn = Database.ColumnExists(reader, "PriceExternalId");
                    }
                    Connection.Close();
                }
            }
            if (databasePriceHasExternalIdColumn.Value)
            {
                string sql = "SELECT * FROM [EcomPrices] WHERE [PriceExternalId] = @PriceExternalId";
                using (var command = new SqlCommand(sql, Connection))
                {
                    command.Parameters.Clear();
                    command.Parameters.AddWithValue("@PriceExternalId", externalId);
                    OpenConnection();
                    using (var reader = command.ExecuteReader())
                    {
                        if (reader.Read())
                        {
                            result.Add(new Price(reader));
                        }
                    }
                    Connection.Close();
                }
            }
            return result;
        }
        public List<SimplePrice> GetAllSimplePriceByShopIdAndLanguageId(string shopId, string languageId, int commandTimeout)
        {
            List<SimplePrice> result = new List<SimplePrice>();
            string sql = "SELECT * FROM [EcomPrices] WHERE [PriceProductLanguageId] = @PriceProductLanguageId AND [PriceShopId] = @PriceShopId";
            using (var command = new SqlCommand(sql, Connection))
            {
                command.CommandTimeout = commandTimeout;
                command.Parameters.AddWithValue("@PriceProductLanguageId", languageId);
                command.Parameters.AddWithValue("@PriceShopId", shopId);
                OpenConnection();
                using (var reader = command.ExecuteReader())
                {
                    bool databasePriceHasExternalIdColumn = Database.ColumnExists(reader, "PriceExternalId");
                    while (reader.Read())
                    {
                        if (databasePriceHasExternalIdColumn)
                        {
                            if (!string.IsNullOrWhiteSpace(reader["PriceExternalId"].ToString()))
                            {
                                result.Add(new SimplePrice { Id = reader["PriceId"].ToString(), ExternalId = reader["PriceExternalId"].ToString() });
                            }
                        }
                        else
                        {
                            result.Add(new SimplePrice { Id = reader["PriceId"].ToString(), ExternalId = "" });
                        }
                    }
                }
                Connection.Close();
            }
            return result;
        }
        public Price GetPriceById(string priceId)
        {
            Price result = new Price();
            string sql = "SELECT * FROM [EcomPrices] WHERE [PriceId] = @PriceId";
            using (var command = new SqlCommand(sql, Connection))
            {
                command.Parameters.AddWithValue("@PriceId", priceId);
                OpenConnection();
                using (var reader = command.ExecuteReader())
                {
                    while (reader.Read())
                    {
                        result = new Price(reader);
                    }
                }
                Connection.Close();
            }
            return result;
        }
        private SqlConnection Connection
        {
            get { return _connection ?? (_connection = (SqlConnection)Database.CreateConnection()); }
            set { _connection = value; }
        }
        private void OpenConnection()
        {
            if (Connection.State != ConnectionState.Open)
            {
                Connection.Open();
            }
        }
    }
}
