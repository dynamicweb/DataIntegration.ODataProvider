using Dynamicweb.Core;
using Dynamicweb.Data;
using Dynamicweb.DataIntegration.Integration;
using Dynamicweb.DataIntegration.Integration.Interfaces;
using Dynamicweb.DataIntegration.ProviderHelpers;
using Dynamicweb.DataIntegration.Providers.ODataProvider.Interfaces;
using Dynamicweb.DataIntegration.Providers.ODataProvider.Service;
using Dynamicweb.Ecommerce.Common;
using Dynamicweb.Ecommerce.International;
using Dynamicweb.Ecommerce.Products;
using Dynamicweb.Ecommerce.Shops;
using Dynamicweb.Ecommerce.Variants;
using Dynamicweb.Extensibility.AddIns;
using Dynamicweb.Extensibility.Editors;
using Dynamicweb.Logging;
using Dynamicweb.Security.UserManagement;
using System;
using System.Collections;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.Linq;
using System.Xml;
using System.Xml.Linq;

namespace Dynamicweb.DataIntegration.Providers.ODataProvider
{
    /// <summary>
    /// OData Provider. Only a destination provider for now
    /// </summary>
    /// <seealso cref="BaseProvider" />
    [AddInName("Dynamicweb.DataIntegration.Providers.Provider")]
    [AddInLabel("OData Provider")]
    [AddInDescription("OData provider")]
    [AddInUseParameterSectioning(true)]
    [AddInIgnore(false)]
    [AddInDeprecated(true)]
    public class ODataProvider : BaseProvider, IODataBaseProvider, ISource, IDestination, IDropDownOptionActions
    {
        private string _workingDirectory;
        private Schema _schema;
        private Language _destinationLanguage;
        private readonly LanguageService _languageService = Ecommerce.Services.Languages;
        private readonly ShopService _shopService = Ecommerce.Services.Shops;
        private readonly GroupService _groupService = Ecommerce.Services.ProductGroups;
        private readonly CurrencyService _currenciesService = Ecommerce.Services.Currencies;
        private SqlConnection _connection;
        private string _sourceLanguageCode;
        private Dictionary<string, object> _currentSourceRow;
        private List<Dictionary<string, object>> _restOfSourceRow;

        #region AddInManager/ConfigurableAddIn Crap


        /// <summary>
        /// Gets or sets the type of the object being imported.
        /// </summary>
        /// <value>
        /// The type of the object.
        /// </value>
        [AddInParameter("Object type")]
        [AddInParameterEditor(typeof(DropDownParameterEditor), "Info=Required;NewGUI=true;none=true;nonetext=Please select an Object Type;columns=Object type|Import order|Comment;SortBy=Key;")]
        [AddInParameterGroup("Destination")]
        [AddInParameterOrder(0)]
        [AddInParameterSection("Destination")]
        public string ObjectType { get; set; }
        [AddInParameter("Source object type")]
        [AddInParameterEditor(typeof(DropDownParameterEditor), "Info=Required;NewGUI=true;none=true;nonetext=Please select an Object Type;")]
        [AddInParameterGroup("Source")]
        [AddInParameterSection("Source")]
        public string SourceObjectType { get; set; }

        /// <summary>
        /// Gets or sets the language since we cannot get this from NAV/BC.
        /// </summary>
        /// <value>
        /// The default language.
        /// </value>
        [AddInParameter("Destination language")]
        [AddInParameterEditor(typeof(DropDownParameterEditor), "Info=Required;NewGUI=true;")]
        [AddInParameterGroup("Destination")]
        [AddInParameterOrder(10)]
        [AddInParameterSection("Object Type Settings")]
        public string DestinationLanguage
        {
            get => _destinationLanguage != null ? _destinationLanguage.LanguageId : Ecommerce.Services.Languages.GetDefaultLanguage()?.LanguageId;
            set => _destinationLanguage = _languageService.GetLanguage(value);
        }

        /// <summary>
        /// Gets or sets the shop to import to.
        /// </summary>
        /// <value>
        /// The shop.
        /// </value>
        [AddInParameter("Shop")]
        [AddInParameterEditor(typeof(DropDownParameterEditor), "Info=Required;NewGUI=true;refreshParameters=true;SortBy=group,value;")]
        [AddInParameterGroup("Destination")]
        [AddInParameterOrder(20)]
        [AddInParameterSection("Object Type Settings")]
        public string Shop
        {
            get;
            set;
        }

        /// <summary>
        /// Gets or sets the sourceLanguageCode so we know what language the items is added to.
        /// </summary>
        /// <value>
        /// The BC name for languagecode.
        /// </value>
        [AddInParameter("Source language code")]
        [AddInParameterEditor(typeof(TextParameterEditor), "infoText=Required. Make sure this language code exists in ERP")]
        [AddInParameterGroup("Destination")]
        [AddInParameterOrder(25)]
        [AddInParameterSection("Object Type Settings")]
        public string SourceLanguageCode
        {
            get => _sourceLanguageCode;
            set => _sourceLanguageCode = value;
        }
        /// <summary>
        /// Gets or sets the productgroup to import to.
        /// </summary>
        /// <value>
        /// The Productgroup
        /// </value>
        [AddInParameter("Product group")]
        [AddInParameterEditor(typeof(DropDownParameterEditor), "NewGUI=true;")]
        [AddInParameterGroup("Destination")]
        [AddInParameterOrder(30)]
        [AddInParameterSection("Object Type Settings")]
        public string Group
        {
            get;
            set;
        }
        [AddInParameter("User group")]
        [AddInParameterEditor(typeof(DropDownParameterEditor), "Info=Required;NewGUI=true;")]
        [AddInParameterGroup("Destination")]
        [AddInParameterOrder(35)]
        [AddInParameterSection("Object Type Settings")]
        public string UserGroup
        {
            get;
            set;
        }
        [AddInParameter("Source user group")]
        [AddInParameterEditor(typeof(DropDownParameterEditor), "Info=Required;NewGUI=true;")]
        [AddInParameterGroup("Source")]
        [AddInParameterSection("Source Object Type Settings")]
        public string SourceUserGroup
        {
            get;
            set;
        }
        [AddInParameter("Default currency")]
        [AddInLabel("Detected default currency")]
        [AddInParameterEditor(typeof(DropDownParameterEditor), "Info=Required;InfoBar=true;none=false;Tooltip=Make sure the base currency of the ERP system is equal to the default currency in Dynamicweb. If the currencies do not match then all exchange rates will be wrong.")]
        [AddInParameterGroup("Destination")]
        [AddInParameterOrder(40)]
        [AddInParameterSection("Object Type Settings")]
        public string Defaultcurrency
        {
            get;
            set;
        }
        [AddInParameter("Customer has contacts")]
        [AddInLabel("Customer has contacts")]
        [AddInParameterEditor(typeof(DropDownParameterEditor), "Info=Required;none=false;NewGUI=true;")]
        [AddInParameterGroup("Destination")]
        [AddInParameterOrder(45)]
        [AddInParameterSection("Object Type Settings")]
        public string Customerhascontacts
        {
            get;
            set;
        }
        [AddInParameter("Anonymous user")]
        [AddInParameterEditor(typeof(TextParameterEditor), "infoText=Make sure there is a customer with this customer number in the ERP.")]
        [AddInParameterGroup("Destination")]
        [AddInParameterOrder(50)]
        [AddInParameterSection("Object Type Settings")]
        public string AnonymousUser { get; set; }
        [AddInParameter("Discount name prefix")]
        [AddInParameterEditor(typeof(TextParameterEditor), "infoText=Required. Prefix of the name to imported discounts.")]
        [AddInParameterGroup("Destination")]
        [AddInParameterOrder(55)]
        [AddInParameterSection("Object Type Settings")]
        public string DiscountNamePrefix { get; set; }
        [AddInParameter("Merge compared discounts together")]
        [AddInLabel("Merge compared discounts together")]
        [AddInParameterEditor(typeof(DropDownParameterEditor), "Info=Required;none=false;NewGUI=true;")]
        [AddInParameterGroup("Destination")]
        [AddInParameterOrder(60)]
        [AddInParameterSection("Object Type Settings")]
        public string MergeDiscounts { get; set; }
        [AddInParameter("Sales type customer")]
        [AddInParameterEditor(typeof(YesNoParameterEditor), "")]
        [AddInParameterGroup("Destination")]
        [AddInParameterOrder(61)]
        [AddInParameterSection("Object Type Settings")]
        public bool SalesTypeCustomer { get; set; }
        [AddInParameter("Sales type all customer")]
        [AddInParameterEditor(typeof(YesNoParameterEditor), "")]
        [AddInParameterGroup("Destination")]
        [AddInParameterOrder(62)]
        [AddInParameterSection("Object Type Settings")]
        public bool SalesTypeAllCustomer { get; set; }
        [AddInParameter("Sales type customer price group")]
        [AddInParameterEditor(typeof(YesNoParameterEditor), "")]
        [AddInParameterGroup("Destination")]
        [AddInParameterOrder(63)]
        [AddInParameterSection("Object Type Settings")]
        public bool SalesTypeCustomerPriceGroup { get; set; }
        [AddInParameter("Sales type customer discount group")]
        [AddInParameterEditor(typeof(YesNoParameterEditor), "")]
        [AddInParameterGroup("Destination")]
        [AddInParameterOrder(64)]
        [AddInParameterSection("Object Type Settings")]
        public bool SalesTypeCustomerDiscountGroup { get; set; }
        public Hashtable GetOptions(string name)
        {
            var options = new Hashtable();
            if (name == "Shop")
            {
                foreach (var shop in _shopService.GetShops())
                {
                    options.Add(shop.Id, shop.Name);
                }
            }
            else if (name == "Customer has contacts")
            {
                options.Add("Yes", "Yes");
                options.Add("No", "No");
            }
            else if (name == "Merge compared discounts together")
            {
                options.Add("Yes", "Yes");
                options.Add("No", "No");
            }
            else if (name == "Default currency")
            {
                options = GetDefaultCurrency();
            }
            else if (name == "Product group")
            {
                options = GetShopGroups(Shop);
            }
            else if (name == "User group")
            {
                options = GetUserGroups(true);
            }
            else if (name == "Source user group")
            {
                options = GetUserGroups(false);
            }
            else if (name == "Destination language")
            {
                var languages = _languageService.GetLanguages();

                foreach (var language in languages)
                {
                    options.Add(language.LanguageId, language.Name);
                }
            }
            else if (name == "Object type")
            {
                IODataBaseProvider endpointProvider = GetSourceProviderSession();
                if (endpointProvider == null || endpointProvider?.GetEndpointType() == "OData V4 - Business Central")
                {
                    options.Add("Custom", "Custom|22");
                    options.Add("Product", "Product|11|Use with source: Page Object ID 30");
                    options.Add("ProductVariant", "Product variant|12|Use with source: Page Object ID 5401");
                    options.Add("ProductUnitRelation", "Product-unit relation|14|Use with source: Page Object ID 5404");
                    options.Add("ProductUnit", "Product unit|13|Use with source: Page Object ID 209");
                    options.Add("ProductTranslation", "Product translation|15|Use with source: Page Object ID 35");
                    options.Add("ProductUnitTranslation", "Product unit translation|16|Use with source: Page Object ID 5402");
                    options.Add("ProductGroup", "Product group|10|Use with source: Page Object ID 5733");
                    options.Add("StockLocation", "Stock location|9|Use with source: Page Object ID 15");
                    options.Add("StockAmount", "Stock amount|17|Use with source: Page Object ID 5700");
                    options.Add("Order", "Order|18|Use with source: Page Object ID 42");
                    options.Add("Currency", "Currency|2|Use with source: Page Object ID 5");
                    options.Add("Manufacturer", "Manufacturer|8|Use with source: Page Object ID 26");
                    options.Add("Country", "Country|1|Use with source: Page Object ID 10");
                    options.Add("User", "User|4|Use with source: Page Object ID 5050");
                    options.Add("UserAddress", "User address|6|Use with source: Page Object ID 5056");
                    options.Add("UserCustomer", "User customer|5|Use with source: Page Object ID 21");
                    options.Add("UserCustomerShipToAddress", "User customer ship-to address|7|Use with source: Page Object ID 300");
                    options.Add("UserSalesperson", "User salesperson|3|Use with source: Page Object ID 5116");
                    options.Add("Discount", "Discount|19|Use with source: Page Object ID 7004");
                    options.Add("Price", "Price|20|Use with source: Page Object ID 7002");
                    options.Add("PriceAndDiscount", "Price & Discount|21|Use with source: Page Object ID 7016");
                }
                else
                {
                    options.Add("Custom", "Custom|1");
                }
            }
            else if (name == "Source object type")
            {
                options.Add("Order", "Order");
                options.Add("User", "User");
                options.Add("UserAddress", "User address");
                options.Add("UserCustomer", "User customer");
                options.Add("UserCustomerShipToAddress", "User customer ship-to address");
            }
            return options;
        }

        private IODataBaseProvider GetSourceProviderSession()
        {
            IODataBaseProvider result = null;
            if (Context.Current?.Session?["newImportSource"] != null)
            {
                ISource source = Context.Current.Session["newImportSource"] as ISource;
                if (source is IODataBaseProvider)
                {
                    result = source as IODataBaseProvider;
                }
            }
            return result;
        }

        /// <summary>
        /// Do not ever use in code. For AddInManager use only.
        /// </summary>
        public ODataProvider()
        {
            try
            {
                _workingDirectory = SystemInformation.MapPath("/Files/");
            }
            catch (NullReferenceException)
            {
                _workingDirectory = "";
            }
            _destinationLanguage = _languageService.GetDefaultLanguage();
        }

        public Column DoODataMapping(Column sourceColumn, ColumnCollection destinationColumns)
        {
            string dColumnName = ConvertColumnNameFromBCToDW(sourceColumn, destinationColumns[0].Table.Name);
            if (!string.IsNullOrEmpty(dColumnName))
            {
                return destinationColumns.Find(obj => obj.Name == dColumnName);
            }
            switch (destinationColumns[0].Table.Name)
            {
                case "ObjectTypeProducts":
                    if (destinationColumns.Find(obj => obj.Name == "EcomProducts." + sourceColumn.Name) != null)
                    {
                        return destinationColumns.Find(obj => obj.Name == "EcomProducts." + sourceColumn.Name);
                    }
                    break;
                case "ObjectTypeOrder":
                    if (destinationColumns.Find(obj => obj.Name == "EcomOrders." + sourceColumn.Name) != null)
                    {
                        return destinationColumns.Find(obj => obj.Name == "EcomOrders." + sourceColumn.Name);
                    }
                    break;
                case "ObjectTypeUser":
                    if (destinationColumns.Find(obj => obj.Name == "AccessUser." + sourceColumn.Name) != null)
                    {
                        return destinationColumns.Find(obj => obj.Name == "AccessUser." + sourceColumn.Name);
                    }
                    break;
                case "ObjectTypeUserAddress":
                    if (destinationColumns.Find(obj => obj.Name == "AccessUserAddress." + sourceColumn.Name) != null)
                    {
                        return destinationColumns.Find(obj => obj.Name == "AccessUserAddress." + sourceColumn.Name);
                    }
                    break;
                case "ObjectTypeUserCustomer":
                    if (destinationColumns.Find(obj => obj.Name == "AccessUser." + sourceColumn.Name) != null)
                    {
                        return destinationColumns.Find(obj => obj.Name == "AccessUser." + sourceColumn.Name);
                    }
                    break;
                case "ObjectTypeUserCustomerShipToAddress":
                    if (destinationColumns.Find(obj => obj.Name == "AccessUserAddress." + sourceColumn.Name) != null)
                    {
                        return destinationColumns.Find(obj => obj.Name == "AccessUserAddress." + sourceColumn.Name);
                    }
                    break;
                case "ObjectTypeUserSalesperson":
                    if (destinationColumns.Find(obj => obj.Name == "AccessUser." + sourceColumn.Name) != null)
                    {
                        return destinationColumns.Find(obj => obj.Name == "AccessUser." + sourceColumn.Name);
                    }
                    break;
            }
            return null;
        }
        private string ConvertColumnNameFromBCToDW(Column sourceColumn, string objectType)
        {
            string result = "";
            switch (objectType)
            {
                case "ObjectTypeProduct":
                    {
                        switch (sourceColumn.Name)
                        {
                            case "@odata.etag": { result = "@odata.etag"; break; }
                            case "No": { result = "EcomProducts.ProductNumber"; break; }
                            case "Description": { result = "EcomProducts.ProductName"; break; }
                            case "Base_Unit_of_Measure": { result = "EcomProducts.ProductDefaultUnitId"; break; }
                            case "GTIN": { result = "EcomProducts.ProductEAN"; break; }
                            case "Unit_Price": { result = "EcomProducts.ProductPrice"; break; }
                            case "Gross_Weight": { result = "EcomProducts.ProductWeight"; break; }
                            case "Unit_Volume": { result = "EcomProducts.ProductVolume"; break; }
                            case "Unit_Cost": { result = "EcomProducts.ProductCost"; break; }
                            case "Minimum_Order_Quantity": { result = "EcomProducts.ProductPurchaseMinimumQuantity"; break; }
                            case "Vendor_No": { result = "EcomProducts.ProductManufacturerId"; break; }
                            case "Blocked": { result = "EcomProducts.ProductActive"; break; }
                        }
                        break;
                    }
                case "ObjectTypeProductVariant":
                    {
                        switch (sourceColumn.Name)
                        {
                            case "@odata.etag": { result = "@odata.etag"; break; }
                            case "Item_No": { result = "ObjectTypeProductVariantItemNo"; break; }
                            case "Code": { result = "ObjectTypeProductVariantCode"; break; }
                            case "Description": { result = "ObjectTypeProductVariantDescription"; break; }
                        }
                        break;
                    }
                case "ObjectTypeProductUnitRelation":
                    {
                        switch (sourceColumn.Name)
                        {
                            case "@odata.etag": { result = "@odata.etag"; break; }
                            case "Item_No": { result = "EcomStockUnit.StockUnitProductId"; break; }
                            case "Code": { result = "EcomStockUnit.StockUnitDescription"; break; }
                            case "Qty_per_Unit_of_Measure": { result = "EcomStockUnit.StockUnitQuantity"; break; }
                        }
                        break;
                    }
                case "ObjectTypeProductUnit":
                    {
                        switch (sourceColumn.Name)
                        {
                            case "@odata.etag": { result = "@odata.etag"; break; }
                            case "Code": { result = "EcomVariantsOptions.VariantOptionId"; break; }
                            case "Description": { result = "EcomVariantsOptions.VariantOptionName"; break; }
                        }
                        break;
                    }
                case "ObjectTypeProductTranslation":
                    {
                        switch (sourceColumn.Name)
                        {
                            case "@odata.etag": { result = "@odata.etag"; break; }
                            case "Item_No": { result = "ObjectTypeProductTranslationItemNo"; break; }
                            case "Variant_Code": { result = "ObjectTypeProductTranslationCode"; break; }
                            case "Description": { result = "ObjectTypeProductTranslationDescription"; break; }
                            case "Language_Code": { result = "ObjectTypeProductTranslationLanguageCode"; break; }
                        }
                        break;
                    }
                case "ObjectTypeProductUnitTranslation":
                    {
                        switch (sourceColumn.Name)
                        {
                            case "@odata.etag": { result = "@odata.etag"; break; }
                            case "Code": { result = "EcomVariantsOptions.VariantOptionId"; break; }
                            case "Language_Code": { result = "EcomVariantsOptions.VariantOptionLanguageId"; break; }
                            case "Description": { result = "EcomVariantsOptions.VariantOptionName"; break; }
                        }
                        break;
                    }
                case "ObjectTypeProductGroup":
                    {
                        switch (sourceColumn.Name)
                        {
                            case "@odata.etag": { result = "@odata.etag"; break; }
                            case "Code": { result = "EcomGroups.GroupId"; break; }
                            case "Description": { result = "EcomGroups.GroupName"; break; }
                            case "Parent_Category": { result = "ObjectTypeProductGroupRelations"; break; }
                        }
                        break;
                    }
                case "ObjectTypeStockLocation":
                    {
                        switch (sourceColumn.Name)
                        {
                            case "@odata.etag": { result = "@odata.etag"; break; }
                            case "Code": { result = "EcomStockLocation.StockLocationName"; break; }
                            case "Name": { result = "EcomStockLocation.StockLocationDescription"; break; }
                        }
                        break;
                    }
                case "ObjectTypeStockAmount":
                    {
                        switch (sourceColumn.Name)
                        {
                            case "@odata.etag": { result = "@odata.etag"; break; }
                            case "Location_Code": { result = "ObjectTypeStockAmountLocationCode"; break; }
                            case "Item_No": { result = "ObjectTypeStockAmountItemNo"; break; }
                            case "Variant_Code": { result = "ObjectTypeStockAmountVariantCode"; break; }
                            case "Inventory": { result = "ObjectTypeStockAmountInventory"; break; }
                        }
                        break;
                    }
                case "ObjectTypeOrder":
                    switch (sourceColumn.Name)
                    {
                        case "@odata.etag": { result = "@odata.etag"; break; }
                        case "Sell_to_Customer_Name": { result = "EcomOrders.OrderCustomerName"; break; }
                        case "Bill_to_Address": { result = "EcomOrders.OrderCustomerAddress"; break; }
                        case "Bill_to_Address_2": { result = "EcomOrders.OrderCustomerAddress2"; break; }
                        case "Bill_to_Post_Code": { result = "EcomOrders.OrderCustomerZip"; break; }
                        case "Bill_to_City": { result = "EcomOrders.OrderCustomerCity"; break; }
                        case "Sell_to_Phone_No": { result = "ObjectTypeOrderSelltoPhoneNo"; break; }
                        case "Sell_to_E_Mail": { result = "ObjectTypeOrderSelltoEMail"; break; }
                        case "Sell_to_Address": { result = "EcomOrders.OrderDeliveryAddress"; break; }
                        case "Sell_to_Address_2": { result = "EcomOrders.OrderDeliveryAddress2"; break; }
                        case "Sell_to_Post_Code": { result = "EcomOrders.OrderDeliveryZip"; break; }
                        case "Sell_to_City": { result = "EcomOrders.OrderDeliveryCity"; break; }
                        case "Sell_to_Country_Region_Code": { result = "EcomOrders.OrderCustomerCountryCode"; break; }
                        case "Payment_Discount_Percent": { result = "EcomOrders.OrderDiscountPercentage"; break; }
                        case "No": { result = "ObjectTypeOrderNo"; break; }
                        case "Order_Date": { result = "ObjectTypeOrderOrderDate"; break; }
                        case "Posting_Date": { result = "ObjectTypeOrderPostingDate"; break; }
                    }
                    break;
                case "ObjectTypeOrderLines":
                    switch (sourceColumn.Name)
                    {
                        case "@odata.etag": { result = "@odata.etag"; break; }
                        case "No": { result = "ObjectTypeOrderLinesNo"; break; }
                        case "Description": { result = "ObjectTypeOrderLinesDescription"; break; }
                        case "Total_VAT_Amount": { result = "ObjectTypeOrderLinesTotalVATAmount"; break; }
                        case "Unit_of_Measure_Code": { result = "EcomOrderLines.OrderLineUnitId"; break; }
                        case "Line_Amount": { result = "ObjectTypeOrderLinesLineAmount"; break; }
                        case "Line_Discount_Amount": { result = "ObjectTypeOrderLinesLineDiscountAmount"; break; }
                        case "Line_Discount_Percent": { result = "ObjectTypeOrderLinesLineDiscountPercent"; break; }
                        case "Unit_Price": { result = "ObjectTypeOrderLinesUnitPrice"; break; }
                        case "Location_Code": { result = "EcomOrderLines.OrderLineStockLocationId"; break; }
                        case "Variant_Code": { result = "ObjectTypeOrderLinesVariantCode"; break; }
                        case "Quantity": { result = "ObjectTypeOrderLinesQuantity"; break; }
                    }
                    break;
                case "ObjectTypeCurrency":
                    switch (sourceColumn.Name)
                    {
                        case "@odata.etag": { result = "@odata.etag"; break; }
                        case "Code": { result = "EcomCurrencies.CurrencyCode"; break; }
                        case "Description": { result = "EcomCurrencies.CurrencyName"; break; }
                        case "ExchangeRateAmt": { result = "EcomCurrencies.CurrencyRate"; break; }
                        case "ISO_Numeric_Code": { result = "EcomCurrencies.CurrencyPayGatewayCode"; break; }
                    }
                    break;
                case "ObjectTypeManufacturer":
                    switch (sourceColumn.Name)
                    {
                        case "@odata.etag": { result = "@odata.etag"; break; }
                        case "No": { result = "EcomManufacturers.ManufacturerId"; break; }
                        case "Name": { result = "EcomManufacturers.ManufacturerName"; break; }
                        case "Address": { result = "ObjectTypeManufacturerAddress"; break; }
                        case "Address_2": { result = "ObjectTypeManufacturerAddress2"; break; }
                        case "City": { result = "EcomManufacturers.ManufacturerCity"; break; }
                        case "Post_Code": { result = "EcomManufacturers.ManufacturerZipCode"; break; }
                        case "Country_Region_Code": { result = "EcomManufacturers.ManufacturerCountry"; break; }
                        case "Phone_No": { result = "EcomManufacturers.ManufacturerPhone"; break; }
                        case "E_Mail": { result = "EcomManufacturers.ManufacturerEmail"; break; }
                        case "Fax_No": { result = "EcomManufacturers.ManufacturerFax"; break; }
                        case "Home_Page": { result = "EcomManufacturers.ManufacturerWeb"; break; }
                    }
                    break;
                case "ObjectTypeCountry":
                    switch (sourceColumn.Name)
                    {
                        case "@odata.etag": { result = "@odata.etag"; break; }
                        case "Code": { result = "EcomCountries.CountryCode2"; break; }
                        case "Name": { result = "EcomCountryText.CountryTextName"; break; }
                        case "ISO_Numeric_Code": { result = "EcomCountries.CountryNumber"; break; }
                        case "Address_Format": { result = "ObjectTypeCountryAddressFormat"; break; }
                    }
                    break;
                case "ObjectTypeUser":
                    switch (sourceColumn.Name)
                    {
                        case "@odata.etag": { result = "@odata.etag"; break; }
                        case "E_Mail": { result = "ObjectTypeUserEMail"; break; }
                        case "No": { result = "AccessUser.AccessUserExternalId"; break; }
                        case "IntegrationCustomerNo": { result = "AccessUser.AccessUserCustomerNumber"; break; }
                        case "Company_Name": { result = "AccessUser.AccessUserCompany"; break; }
                        case "Name": { result = "AccessUser.AccessUserName"; break; }
                        case "Address": { result = "AccessUser.AccessUserAddress"; break; }
                        case "Address_2": { result = "AccessUser.AccessUserAddress2"; break; }
                        case "Country_Region_Code": { result = "AccessUser.AccessUserCountryCode"; break; }
                        case "Post_Code": { result = "AccessUser.AccessUserZip"; break; }
                        case "City": { result = "AccessUser.AccessUserCity"; break; }
                        case "Phone_No": { result = "AccessUser.AccessUserPhone"; break; }
                        case "Mobile_Phone_No": { result = "AccessUser.AccessUserMobile"; break; }
                        case "Fax_No": { result = "AccessUser.AccessUserFax"; break; }
                        case "Currency_Code": { result = "AccessUser.AccessUserCurrencyCharacter"; break; }
                    }
                    break;
                case "ObjectTypeUserAddress":
                    switch (sourceColumn.Name)
                    {
                        case "@odata.etag": { result = "@odata.etag"; break; }
                        case "Code": { result = "ObjectTypeUserAddressCode"; break; }
                        case "Company_Name": { result = "AccessUserAddress.AccessUserAddressCompany"; break; }
                        case "Address": { result = "AccessUserAddress.AccessUserAddressAddress"; break; }
                        case "Address_2": { result = "AccessUserAddress.AccessUserAddressAddress2"; break; }
                        case "City": { result = "AccessUserAddress.AccessUserAddressCity"; break; }
                        case "Post_Code": { result = "AccessUserAddress.AccessUserAddressZip"; break; }
                        case "Country_Region_Code": { result = "AccessUserAddress.AccessUserAddressCountryCode"; break; }
                        case "Phone_No": { result = "AccessUserAddress.AccessUserAddressPhone"; break; }
                        case "Mobile_Phone_No": { result = "AccessUserAddress.AccessUserAddressCell"; break; }
                        case "Fax_No": { result = "AccessUserAddress.AccessUserAddressFax"; break; }
                        case "E_Mail": { result = "AccessUserAddress.AccessUserAddressEmail"; break; }
                    }
                    break;
                case "ObjectTypeUserCustomer":
                    switch (sourceColumn.Name)
                    {
                        case "@odata.etag": { result = "@odata.etag"; break; }
                        case "E_Mail": { result = "ObjectTypeUserCustomerEMail"; break; }
                        case "No": { result = "ObjectTypeUserCustomerNo"; break; }
                        case "Name": { result = "AccessUser.AccessUserName"; break; }
                        case "Address": { result = "AccessUser.AccessUserAddress"; break; }
                        case "Address_2": { result = "AccessUser.AccessUserAddress2"; break; }
                        case "Country_Region_Code": { result = "AccessUser.AccessUserCountryCode"; break; }
                        case "Post_Code": { result = "AccessUser.AccessUserZip"; break; }
                        case "City": { result = "AccessUser.AccessUserCity"; break; }
                        case "Phone_No": { result = "AccessUser.AccessUserPhone"; break; }
                        case "MobilePhoneNo": { result = "AccessUser.AccessUserMobile"; break; }
                        case "Fax_No": { result = "AccessUser.AccessUserFax"; break; }
                        case "Currency_Code": { result = "AccessUser.AccessUserCurrencyCharacter"; break; }
                    }
                    break;

                case "ObjectTypeUserCustomerShipToAddress":
                    switch (sourceColumn.Name)
                    {
                        case "@odata.etag": { result = "@odata.etag"; break; }
                        case "Code": { result = "AccessUserAddress.AccessUserAddressName"; break; }
                        case "Name": { result = "AccessUserAddress.AccessUserAddressCompany"; break; }
                        case "Address": { result = "AccessUserAddress.AccessUserAddressAddress"; break; }
                        case "Address_2": { result = "AccessUserAddress.AccessUserAddressAddress2"; break; }
                        case "City": { result = "AccessUserAddress.AccessUserAddressCity"; break; }
                        case "Post_Code": { result = "AccessUserAddress.AccessUserAddressZip"; break; }
                        case "Country_Region_Code": { result = "AccessUserAddress.AccessUserAddressCountryCode"; break; }
                        case "Phone_No": { result = "AccessUserAddress.AccessUserAddressPhone"; break; }
                        case "Fax_No": { result = "AccessUserAddress.AccessUserAddressFax"; break; }
                        case "E_Mail": { result = "AccessUserAddress.AccessUserAddressEmail"; break; }
                    }
                    break;
                case "ObjectTypeUserSalesperson":
                    switch (sourceColumn.Name)
                    {
                        case "@odata.etag": { result = "@odata.etag"; break; }
                        case "Code": { result = "AccessUser.AccessUserExternalId"; break; }
                        case "Name": { result = "AccessUser.AccessUserName"; break; }
                        case "Job_Title": { result = "AccessUser.AccessUserJobTitle"; break; }
                        case "Phone_No": { result = "AccessUser.AccessUserPhone"; break; }
                        case "E_Mail": { result = "ObjectTypeUserSalespersonEMail"; break; }
                    }
                    break;
                case "ObjectTypeDiscount":
                    switch (sourceColumn.Name)
                    {
                        case "@odata.etag": { result = "@odata.etag"; break; }
                        case "Starting_Date": { result = "EcomDiscount.DiscountValidFrom"; break; }
                        case "Line_Discount_Percent": { result = "EcomDiscount.DiscountPercentage"; break; }
                        case "Ending_Date": { result = "EcomDiscount.DiscountValidTo"; break; }
                    }
                    break;
                case "ObjectTypePrice":
                    switch (sourceColumn.Name)
                    {
                        case "@odata.etag": { result = "@odata.etag"; break; }
                        case "Item_No": { result = "EcomPrices.PriceProductId"; break; }
                        case "Starting_Date": { result = "EcomPrices.PriceValidFrom"; break; }
                        case "Currency_Code": { result = "EcomPrices.PriceCurrency"; break; }
                        case "Unit_of_Measure_Code": { result = "EcomPrices.PriceUnitId"; break; }
                        case "Minimum_Quantity": { result = "EcomPrices.PriceQuantity"; break; }
                        case "Unit_Price": { result = "EcomPrices.PriceAmount"; break; }
                        case "Ending_Date": { result = "EcomPrices.PriceValidTo"; break; }
                        case "Price_Includes_VAT": { result = "EcomPrices.PriceIsWithVat"; break; }
                    }
                    break;
                case "ObjectTypePriceAndDiscount":
                    switch (sourceColumn.Name)
                    {
                        case "@odata.etag": { result = "@odata.etag"; break; }
                        case "CurrencyCode": { result = "EcomPrices.PriceCurrency"; break; }
                        case "StartingDate": { result = "ObjectTypePriceAndDiscountStartingDate"; break; }
                        case "EndingDate": { result = "ObjectTypePriceAndDiscountEndingDate"; break; }
                        case "Product_No": { result = "ObjectTypePriceAndDiscountProductNo"; break; }
                        case "Unit_of_Measure_Code": { result = "EcomPrices.PriceUnitId"; break; }
                        case "Minimum_Quantity": { result = "EcomPrices.PriceQuantity"; break; }
                        case "Unit_Price": { result = "EcomPrices.PriceAmount"; break; }
                        case "Line_Discount_Percent": { result = "EcomDiscount.DiscountPercentage"; break; }
                        case "PriceIncludesVAT": { result = "EcomPrices.PriceIsWithVat"; break; }
                    }
                    break;
            }
            return result;
        }

        /// <summary>
        /// Do not ever use in code. For AddInManager use only.
        /// </summary>
        public ODataProvider(XmlNode xmlNode)
        {
            foreach (XmlNode node in xmlNode.ChildNodes)
            {
                switch (node.Name)
                {
                    case "Schema":
                        _schema = new Schema(node);
                        break;
                    case "Defaultlanguage":
                        if (node.HasChildNodes)
                        {
                            DestinationLanguage = node.FirstChild.Value;
                        }
                        break;
                    case "Sourcelanguagecode":
                        if (node.HasChildNodes)
                        {
                            SourceLanguageCode = node.FirstChild.Value;
                        }
                        break;
                    case "Customerhascontacts":
                        if (node.HasChildNodes)
                        {
                            Customerhascontacts = node.FirstChild.Value;
                        }
                        break;
                    case "Defaultcurrency":
                        if (node.HasChildNodes)
                        {
                            Defaultcurrency = node.FirstChild.Value;
                        }
                        break;
                    case "Shop":
                        if (node.HasChildNodes)
                        {
                            Shop = node.FirstChild.Value;
                        }
                        break;
                    case "Anonymoususer":
                        if (node.HasChildNodes)
                        {
                            AnonymousUser = node.FirstChild.Value;
                        }
                        break;
                    case "Discountnameprefix":
                        if (node.HasChildNodes)
                        {
                            DiscountNamePrefix = node.FirstChild.Value;
                        }
                        break;
                    case "Mergecompareddiscountstogether":
                        if (node.HasChildNodes)
                        {
                            MergeDiscounts = node.FirstChild.Value;
                        }
                        break;
                    case "Productgroup":
                        if (node.HasChildNodes)
                        {
                            Group = node.FirstChild.Value;
                        }
                        break;
                    case "Usergroup":
                        if (node.HasChildNodes)
                        {
                            UserGroup = node.FirstChild.Value;
                        }
                        break;
                    case "Sourceusergroup":
                        if (node.HasChildNodes)
                        {
                            SourceUserGroup = node.FirstChild.Value;
                        }
                        break;
                    case "Sourceobjecttype":
                        if (node.HasChildNodes)
                        {
                            SourceObjectType = node.FirstChild.Value;
                        }
                        break;
                    case "Objecttype":
                        if (node.HasChildNodes)
                        {
                            ObjectType = node.FirstChild.Value;
                        }
                        break;
                    case "Salestypecustomer":
                        if (node.HasChildNodes)
                        {
                            SalesTypeCustomer = node.FirstChild.Value == "True";
                        }
                        break;
                    case "Salestypeallcustomer":
                        if (node.HasChildNodes)
                        {
                            SalesTypeAllCustomer = node.FirstChild.Value == "True";
                        }
                        break;
                    case "Salestypecustomerpricegroup":
                        if (node.HasChildNodes)
                        {
                            SalesTypeCustomerPriceGroup = node.FirstChild.Value == "True";
                        }
                        break;
                    case "Salestypecustomerdiscountgroup":
                        if (node.HasChildNodes)
                        {
                            SalesTypeCustomerDiscountGroup = node.FirstChild.Value == "True";
                        }
                        break;
                }
            }
        }

        #endregion AddInManager/ConfigurableAddIn Crap
        /// <inheritdoc />
        public override bool SchemaIsEditable => false; // Schema is never editable for ODataProvider

        /// <inheritdoc />
        public override string WorkingDirectory
        {
            get => _workingDirectory;
            set => _workingDirectory = value.Replace("\\", "/");
        }

        private SqlConnection Connection
        {
            get { return _connection ?? (_connection = (SqlConnection)Database.CreateConnection()); }
            set { _connection = value; }
        }
        public override void Close()
        {
            // Not used
        }

        public override Schema GetOriginalSourceSchema()
        {
            ObjectType = SourceObjectType;
            return _schema ?? (_schema = GetOriginalDestinationSchema());
        }

        public override void OverwriteSourceSchemaToOriginal()
        {
            _schema = GetOriginalSourceSchema();
        }

        public override Schema GetSchema()
        {
            return _schema ?? (_schema = GetOriginalDestinationSchema());
        }
        /// <summary>
        /// Gets the schema from the Destination Source. Currently this uses an SQL connection, but this should be moved to services
        /// </summary>
        /// <returns></returns>
        public override Schema GetOriginalDestinationSchema()
        {
            var result = new Schema();
            var sql = GetSqlForSchemaBuilding();
            if (ObjectType == "") { ObjectType = SourceObjectType; }
            if (ObjectType == "Custom")
            {
                using (var command = new SqlCommand(sql, Connection))
                {
                    command.CommandTimeout = 3600;
                    if (Connection.State != ConnectionState.Open)
                    {
                        Connection.Open();
                    }
                    Table currentTable = null;
                    using (var reader = command.ExecuteReader())
                    {
                        while (reader.Read())
                        {
                            if (reader["table_name"].ToString() == "sysdiagrams" || reader["table_name"].ToString() == "dtproperties")
                            {
                                continue;
                            }
                            if (currentTable == null)
                            {
                                currentTable = result.AddTable(reader["table_name"].ToString(), reader["table_schema"].ToString());
                            }
                            var table = reader["table_name"].ToString();
                            var sqlSchema = reader["table_schema"].ToString();
                            if ((currentTable.Name != table) || (currentTable.SqlSchema != sqlSchema))
                            {
                                currentTable = result.AddTable(reader["table_name"].ToString(), reader["table_schema"].ToString());
                            }
                            var column = reader["column_name"].ToString();
                            var limit = 0;
                            if (!(reader["CHARACTER_MAXIMUM_LENGTH"] is DBNull))
                            {
                                limit = (int)reader["CHARACTER_MAXIMUM_LENGTH"];
                            }

                            var type = reader[2].ToString();
                            var isIdentity = reader[4].ToString() == "1";
                            var isPrimaryKey = reader["IsPrimaryKey"].ToString() == "1";
                            currentTable.AddColumn(new SqlColumn(column, type, currentTable, limit, isIdentity,
                                                                   isPrimaryKey));
                        }
                    }
                    Connection.Close();
                }
            }
            else if (!string.IsNullOrEmpty(SourceObjectType))
            {
                Table table;
                switch (ObjectType)
                {
                    case "Order":
                        table = result.AddTable("ObjectTypeOrder");
                        table.AddColumn(new SqlColumn("EcomOrders.OrderCurrencyCode", typeof(string), SqlDbType.NVarChar, table, -1, false, false, false));
                        table.AddColumn(new SqlColumn("EcomOrders.OrderCustomerAddress", typeof(string), SqlDbType.NVarChar, table, -1, false, false, false));
                        table.AddColumn(new SqlColumn("EcomOrders.OrderCustomerAddress2", typeof(string), SqlDbType.NVarChar, table, -1, false, false, false));
                        table.AddColumn(new SqlColumn("EcomOrders.OrderCustomerZip", typeof(string), SqlDbType.NVarChar, table, -1, false, false, false));
                        table.AddColumn(new SqlColumn("EcomOrders.OrderCustomerEmail", typeof(string), SqlDbType.NVarChar, table, -1, false, false, false));
                        table.AddColumn(new SqlColumn("EcomOrders.OrderCustomerPhone", typeof(string), SqlDbType.NVarChar, table, -1, false, false, false));
                        table.AddColumn(new SqlColumn("EcomOrders.OrderCustomerCity", typeof(string), SqlDbType.NVarChar, table, -1, false, false, false));
                        table.AddColumn(new SqlColumn("EcomOrders.OrderCustomerCountryCode", typeof(string), SqlDbType.NVarChar, table, -1, false, false, false));
                        table.AddColumn(new SqlColumn("EcomOrders.OrderCustomerRegion", typeof(string), SqlDbType.NVarChar, table, -1, false, false, false));
                        table.AddColumn(new SqlColumn("EcomOrders.OrderDiscountPercentage", typeof(string), SqlDbType.NVarChar, table, -1, false, false, false));
                        table.AddColumn(new SqlColumn("EcomOrders.OrderDate", typeof(string), SqlDbType.NVarChar, table, -1, false, false, false));
                        table.AddColumn(new SqlColumn("EcomOrders.OrderId", typeof(string), SqlDbType.NVarChar, table, -1, false, false, false));
                        foreach (var item in Ecommerce.Services.OrderFields.GetOrderFields())
                        {
                            table.AddColumn(new SqlColumn("EcomOrders." + item.SystemName, typeof(string), SqlDbType.NVarChar, table, -1, false, false, false));
                        }
                        table = result.AddTable("ObjectTypeOrderLines");
                        table.AddColumn(new SqlColumn("EcomOrderLines.OrderLineProductNumber", typeof(string), SqlDbType.NVarChar, table, -1, false, false, false));
                        table.AddColumn(new SqlColumn("EcomOrderLines.OrderLineProductVariantId", typeof(string), SqlDbType.NVarChar, table, -1, false, false, false));
                        table.AddColumn(new SqlColumn("EcomOrderLines.OrderLineProductName", typeof(string), SqlDbType.NVarChar, table, -1, false, false, false));
                        table.AddColumn(new SqlColumn("EcomStockLocation.StockLocationName", typeof(string), SqlDbType.NVarChar, table, -1, false, false, false));
                        table.AddColumn(new SqlColumn("EcomOrderLines.OrderLineQuantity", typeof(string), SqlDbType.NVarChar, table, -1, false, false, false));
                        table.AddColumn(new SqlColumn("EcomOrderLines.OrderLineUnitId", typeof(string), SqlDbType.NVarChar, table, -1, false, false, false));
                        table.AddColumn(new SqlColumn("EcomVariantsOptions.VariantOptionName", typeof(string), SqlDbType.NVarChar, table, -1, false, false, false));
                        table.AddColumn(new SqlColumn("EcomOrderLines.OrderLineUnitPriceWithoutVAT", typeof(string), SqlDbType.NVarChar, table, -1, false, false, false));
                        table.AddColumn(new SqlColumn("EcomOrderLines.OrderLineDiscountPercentage", typeof(string), SqlDbType.NVarChar, table, -1, false, false, false));
                        table.AddColumn(new SqlColumn("EcomOrderLines.OrderLineUnitPriceBeforeDiscount", typeof(string), SqlDbType.NVarChar, table, -1, false, false, false));
                        table.AddColumn(new SqlColumn("EcomOrderLines.OrderLineTotalDiscountWithoutVAT", typeof(string), SqlDbType.NVarChar, table, -1, false, false, false));
                        foreach (var item in Ecommerce.Services.OrderLineFields.GetOrderLineFields())
                        {
                            table.AddColumn(new SqlColumn("EcomOrderLines." + item.SystemName, typeof(string), SqlDbType.NVarChar, table, -1, false, false, false));
                        }
                        break;
                    case "User":
                        table = result.AddTable("ObjectTypeUser");
                        table.AddColumn(new SqlColumn("AccessUser.AccessUserEmail", typeof(string), SqlDbType.NVarChar, table, -1, false, false, false));
                        table.AddColumn(new SqlColumn("AccessUser.AccessUserExternalId", typeof(string), SqlDbType.NVarChar, table, -1, false, false, false));
                        table.AddColumn(new SqlColumn("AccessUser.AccessUserCustomerNumber", typeof(string), SqlDbType.NVarChar, table, -1, false, false, false));
                        table.AddColumn(new SqlColumn("AccessUser.AccessUserCompany", typeof(string), SqlDbType.NVarChar, table, -1, false, false, false));
                        table.AddColumn(new SqlColumn("AccessUser.AccessUserName", typeof(string), SqlDbType.NVarChar, table, -1, false, false, false));
                        table.AddColumn(new SqlColumn("AccessUser.AccessUserAddress", typeof(string), SqlDbType.NVarChar, table, -1, false, false, false));
                        table.AddColumn(new SqlColumn("AccessUser.AccessUserAddress2", typeof(string), SqlDbType.NVarChar, table, -1, false, false, false));
                        table.AddColumn(new SqlColumn("AccessUser.AccessUserCountryCode", typeof(string), SqlDbType.NVarChar, table, -1, false, false, false));
                        table.AddColumn(new SqlColumn("AccessUser.AccessUserZip", typeof(string), SqlDbType.NVarChar, table, -1, false, false, false));
                        table.AddColumn(new SqlColumn("AccessUser.AccessUserCity", typeof(string), SqlDbType.NVarChar, table, -1, false, false, false));
                        table.AddColumn(new SqlColumn("AccessUser.AccessUserPhone", typeof(string), SqlDbType.NVarChar, table, -1, false, false, false));
                        table.AddColumn(new SqlColumn("AccessUser.AccessUserMobile", typeof(string), SqlDbType.NVarChar, table, -1, false, false, false));
                        table.AddColumn(new SqlColumn("AccessUser.AccessUserFax", typeof(string), SqlDbType.NVarChar, table, -1, false, false, false));
                        table.AddColumn(new SqlColumn("AccessUser.AccessUserCurrencyCharacter", typeof(string), SqlDbType.NVarChar, table, -1, false, false, false));
                        foreach (var item in new User().CustomFieldValues)
                        {
                            table.AddColumn(new SqlColumn("AccessUser." + item.CustomField.SystemName, typeof(string), SqlDbType.NVarChar, table, -1, false, false, false));
                        }
                        break;
                    case "UserAddress":
                        table = result.AddTable("ObjectTypeUserAddress");
                        table.AddColumn(new SqlColumn("AccessUserAddress.AccessUserAddressName", typeof(string), SqlDbType.NVarChar, table, -1, false, false, false));
                        table.AddColumn(new SqlColumn("AccessUserAddress.AccessUserAddressCompany", typeof(string), SqlDbType.NVarChar, table, -1, false, false, false));
                        table.AddColumn(new SqlColumn("AccessUserAddress.AccessUserAddressAddress", typeof(string), SqlDbType.NVarChar, table, -1, false, false, false));
                        table.AddColumn(new SqlColumn("AccessUserAddress.AccessUserAddressAddress2", typeof(string), SqlDbType.NVarChar, table, -1, false, false, false));
                        table.AddColumn(new SqlColumn("AccessUserAddress.AccessUserAddressCity", typeof(string), SqlDbType.NVarChar, table, -1, false, false, false));
                        table.AddColumn(new SqlColumn("AccessUserAddress.AccessUserAddressZip", typeof(string), SqlDbType.NVarChar, table, -1, false, false, false));
                        table.AddColumn(new SqlColumn("AccessUserAddress.AccessUserAddressCountryCode", typeof(string), SqlDbType.NVarChar, table, -1, false, false, false));
                        table.AddColumn(new SqlColumn("AccessUserAddress.AccessUserAddressPhone", typeof(string), SqlDbType.NVarChar, table, -1, false, false, false));
                        table.AddColumn(new SqlColumn("AccessUserAddress.AccessUserAddressCell", typeof(string), SqlDbType.NVarChar, table, -1, false, false, false));
                        table.AddColumn(new SqlColumn("AccessUserAddress.AccessUserAddressFax", typeof(string), SqlDbType.NVarChar, table, -1, false, false, false));
                        table.AddColumn(new SqlColumn("AccessUserAddress.AccessUserAddressEmail", typeof(string), SqlDbType.NVarChar, table, -1, false, false, false));
                        foreach (var item in new UserAddress().CustomFieldValues)
                        {
                            table.AddColumn(new SqlColumn("AccessUserAddress." + item.CustomField.SystemName, typeof(string), SqlDbType.NVarChar, table, -1, false, false, false));
                        }
                        break;
                    case "UserCustomer":
                        table = result.AddTable("ObjectTypeUserCustomer");
                        table.AddColumn(new SqlColumn("AccessUser.AccessUserEmail", typeof(string), SqlDbType.NVarChar, table, -1, false, false, false));
                        table.AddColumn(new SqlColumn("AccessUser.AccessUserCustomerNumber", typeof(string), SqlDbType.NVarChar, table, -1, false, false, false));
                        table.AddColumn(new SqlColumn("AccessUser.AccessUserName", typeof(string), SqlDbType.NVarChar, table, -1, false, false, false));
                        table.AddColumn(new SqlColumn("AccessUser.AccessUserAddress", typeof(string), SqlDbType.NVarChar, table, -1, false, false, false));
                        table.AddColumn(new SqlColumn("AccessUser.AccessUserAddress2", typeof(string), SqlDbType.NVarChar, table, -1, false, false, false));
                        table.AddColumn(new SqlColumn("AccessUser.AccessUserCountryCode", typeof(string), SqlDbType.NVarChar, table, -1, false, false, false));
                        table.AddColumn(new SqlColumn("AccessUser.AccessUserZip", typeof(string), SqlDbType.NVarChar, table, -1, false, false, false));
                        table.AddColumn(new SqlColumn("AccessUser.AccessUserCity", typeof(string), SqlDbType.NVarChar, table, -1, false, false, false));
                        table.AddColumn(new SqlColumn("AccessUser.AccessUserPhone", typeof(string), SqlDbType.NVarChar, table, -1, false, false, false));
                        table.AddColumn(new SqlColumn("AccessUser.AccessUserMobile", typeof(string), SqlDbType.NVarChar, table, -1, false, false, false));
                        table.AddColumn(new SqlColumn("AccessUser.AccessUserFax", typeof(string), SqlDbType.NVarChar, table, -1, false, false, false));
                        table.AddColumn(new SqlColumn("AccessUser.AccessUserCurrencyCharacter", typeof(string), SqlDbType.NVarChar, table, -1, false, false, false));
                        foreach (var item in new User().CustomFieldValues)
                        {
                            table.AddColumn(new SqlColumn("AccessUser." + item.CustomField.SystemName, typeof(string), SqlDbType.NVarChar, table, -1, false, false, false));
                        }
                        break;
                    case "UserCustomerShipToAddress":
                        table = result.AddTable("ObjectTypeUserCustomerShipToAddress");
                        table.AddColumn(new SqlColumn("AccessUserAddress.AccessUserAddressName", typeof(string), SqlDbType.NVarChar, table, -1, false, false, false));
                        table.AddColumn(new SqlColumn("AccessUserAddress.AccessUserAddressCompany", typeof(string), SqlDbType.NVarChar, table, -1, false, false, false));
                        table.AddColumn(new SqlColumn("AccessUserAddress.AccessUserAddressAddress", typeof(string), SqlDbType.NVarChar, table, -1, false, false, false));
                        table.AddColumn(new SqlColumn("AccessUserAddress.AccessUserAddressAddress2", typeof(string), SqlDbType.NVarChar, table, -1, false, false, false));
                        table.AddColumn(new SqlColumn("AccessUserAddress.AccessUserAddressCity", typeof(string), SqlDbType.NVarChar, table, -1, false, false, false));
                        table.AddColumn(new SqlColumn("AccessUserAddress.AccessUserAddressZip", typeof(string), SqlDbType.NVarChar, table, -1, false, false, false));
                        table.AddColumn(new SqlColumn("AccessUserAddress.AccessUserAddressCountryCode", typeof(string), SqlDbType.NVarChar, table, -1, false, false, false));
                        table.AddColumn(new SqlColumn("AccessUserAddress.AccessUserAddressPhone", typeof(string), SqlDbType.NVarChar, table, -1, false, false, false));
                        table.AddColumn(new SqlColumn("AccessUserAddress.AccessUserAddressCell", typeof(string), SqlDbType.NVarChar, table, -1, false, false, false));
                        table.AddColumn(new SqlColumn("AccessUserAddress.AccessUserAddressFax", typeof(string), SqlDbType.NVarChar, table, -1, false, false, false));
                        table.AddColumn(new SqlColumn("AccessUserAddress.AccessUserAddressEmail", typeof(string), SqlDbType.NVarChar, table, -1, false, false, false));
                        foreach (var item in new UserAddress().CustomFieldValues)
                        {
                            table.AddColumn(new SqlColumn("AccessUserAddress." + item.CustomField.SystemName, typeof(string), SqlDbType.NVarChar, table, -1, false, false, false));
                        }
                        break;
                }
            }
            else
            {
                Table table;
                switch (ObjectType)
                {
                    case "Product":
                        table = result.AddTable("ObjectTypeProduct");
                        table.AddColumn(new SqlColumn("@odata.etag", typeof(string), SqlDbType.NVarChar, table, -1, false, false, false));
                        table.AddColumn(new SqlColumn("EcomProducts.ProductNumber", typeof(string), SqlDbType.NVarChar, table, -1, false, false, false));
                        table.AddColumn(new SqlColumn("EcomProducts.ProductName", typeof(string), SqlDbType.NVarChar, table, -1, false, false, false));
                        table.AddColumn(new SqlColumn("EcomProducts.ProductDefaultUnitId", typeof(string), SqlDbType.NVarChar, table, -1, false, false, false));
                        table.AddColumn(new SqlColumn("EcomProducts.ProductEAN", typeof(string), SqlDbType.NVarChar, table, -1, false, false, false));
                        table.AddColumn(new SqlColumn("EcomProducts.ProductPrice", typeof(double), SqlDbType.Decimal, table, -1, false, false, false));
                        table.AddColumn(new SqlColumn("EcomProducts.ProductWeight", typeof(double), SqlDbType.Decimal, table, -1, false, false, false));
                        table.AddColumn(new SqlColumn("EcomProducts.ProductVolume", typeof(double), SqlDbType.Decimal, table, -1, false, false, false));
                        table.AddColumn(new SqlColumn("EcomProducts.ProductCost", typeof(double), SqlDbType.Decimal, table, -1, false, false, false));
                        table.AddColumn(new SqlColumn("EcomProducts.ProductPurchaseMinimumQuantity", typeof(double), SqlDbType.Decimal, table, -1, false, false, false));
                        table.AddColumn(new SqlColumn("EcomProducts.ProductManufacturerId", typeof(double), SqlDbType.Decimal, table, -1, false, false, false));
                        table.AddColumn(new SqlColumn("EcomProducts.ProductActive", typeof(double), SqlDbType.Decimal, table, -1, false, false, false));
                        foreach (var item in Application.ProductFields)
                        {
                            table.AddColumn(new SqlColumn("EcomProducts." + item.SystemName, typeof(string), SqlDbType.NVarChar, table, -1, false, false, false));
                        }
                        break;
                    case "ProductVariant":
                        table = result.AddTable("ObjectTypeProductVariant");
                        table.AddColumn(new SqlColumn("@odata.etag", typeof(string), SqlDbType.NVarChar, table, -1, false, false, false));
                        table.AddColumn(new SqlColumn("ObjectTypeProductVariantItemNo", typeof(string), SqlDbType.NVarChar, table, -1, false, false, false));
                        table.AddColumn(new SqlColumn("ObjectTypeProductVariantCode", typeof(string), SqlDbType.NVarChar, table, -1, false, false, false));
                        table.AddColumn(new SqlColumn("ObjectTypeProductVariantDescription", typeof(string), SqlDbType.NVarChar, table, -1, false, false, false));
                        break;
                    case "ProductUnitRelation":
                        table = result.AddTable("ObjectTypeProductUnitRelation");
                        table.AddColumn(new SqlColumn("@odata.etag", typeof(string), SqlDbType.NVarChar, table, -1, false, false, false));
                        table.AddColumn(new SqlColumn("EcomStockUnit.StockUnitProductId", typeof(string), SqlDbType.NVarChar, table, -1, false, false, false));
                        table.AddColumn(new SqlColumn("EcomStockUnit.StockUnitDescription", typeof(string), SqlDbType.NVarChar, table, -1, false, false, false));
                        table.AddColumn(new SqlColumn("EcomStockUnit.StockUnitQuantity", typeof(double), SqlDbType.Decimal, table, -1, false, false, false));
                        break;
                    case "ProductUnit":
                        table = result.AddTable("ObjectTypeProductUnit");
                        table.AddColumn(new SqlColumn("@odata.etag", typeof(string), SqlDbType.NVarChar, table, -1, false, false, false));
                        table.AddColumn(new SqlColumn("EcomVariantsOptions.VariantOptionId", typeof(string), SqlDbType.NVarChar, table, -1, false, false, false));
                        table.AddColumn(new SqlColumn("EcomVariantsOptions.VariantOptionName", typeof(string), SqlDbType.NVarChar, table, -1, false, false, false));
                        break;
                    case "ProductTranslation":
                        table = result.AddTable("ObjectTypeProductTranslation");
                        table.AddColumn(new SqlColumn("@odata.etag", typeof(string), SqlDbType.NVarChar, table, -1, false, false, false));
                        table.AddColumn(new SqlColumn("ObjectTypeProductTranslationItemNo", typeof(string), SqlDbType.NVarChar, table, -1, false, false, false));
                        table.AddColumn(new SqlColumn("ObjectTypeProductTranslationCode", typeof(string), SqlDbType.NVarChar, table, -1, false, false, false));
                        table.AddColumn(new SqlColumn("ObjectTypeProductTranslationDescription", typeof(string), SqlDbType.NVarChar, table, -1, false, false, false));
                        table.AddColumn(new SqlColumn("ObjectTypeProductTranslationLanguageCode", typeof(string), SqlDbType.NVarChar, table, -1, false, false, false));
                        break;
                    case "ProductUnitTranslation":
                        table = result.AddTable("ObjectTypeProductUnitTranslation");
                        table.AddColumn(new SqlColumn("@odata.etag", typeof(string), SqlDbType.NVarChar, table, -1, false, false, false));
                        table.AddColumn(new SqlColumn("EcomVariantsOptions.VariantOptionId", typeof(string), SqlDbType.NVarChar, table, -1, false, false, false));
                        table.AddColumn(new SqlColumn("EcomVariantsOptions.VariantOptionLanguageId", typeof(string), SqlDbType.NVarChar, table, -1, false, false, false));
                        table.AddColumn(new SqlColumn("EcomVariantsOptions.VariantOptionName", typeof(string), SqlDbType.NVarChar, table, -1, false, false, false));
                        break;
                    case "ProductGroup":
                        table = result.AddTable("ObjectTypeProductGroup");
                        table.AddColumn(new SqlColumn("@odata.etag", typeof(string), SqlDbType.NVarChar, table, -1, false, false, false));
                        table.AddColumn(new SqlColumn("EcomGroups.GroupId", typeof(string), SqlDbType.NVarChar, table, -1, false, false, false));
                        table.AddColumn(new SqlColumn("EcomGroups.GroupName", typeof(string), SqlDbType.NVarChar, table, -1, false, false, false));
                        table.AddColumn(new SqlColumn("ObjectTypeProductGroupRelations", typeof(string), SqlDbType.NVarChar, table, -1, false, false, false));
                        break;
                    case "StockLocation":
                        table = result.AddTable("ObjectTypeStockLocation");
                        table.AddColumn(new SqlColumn("@odata.etag", typeof(string), SqlDbType.NVarChar, table, -1, false, false, false));
                        table.AddColumn(new SqlColumn("EcomStockLocation.StockLocationName", typeof(string), SqlDbType.NVarChar, table, -1, false, false, false));
                        table.AddColumn(new SqlColumn("EcomStockLocation.StockLocationDescription", typeof(string), SqlDbType.NVarChar, table, -1, false, false, false));
                        break;
                    case "StockAmount":
                        table = result.AddTable("ObjectTypeStockAmount");
                        table.AddColumn(new SqlColumn("@odata.etag", typeof(string), SqlDbType.NVarChar, table, -1, false, false, false));
                        table.AddColumn(new SqlColumn("ObjectTypeStockAmountLocationCode", typeof(string), SqlDbType.NVarChar, table, -1, false, false, false));
                        table.AddColumn(new SqlColumn("ObjectTypeStockAmountItemNo", typeof(string), SqlDbType.NVarChar, table, -1, false, false, false));
                        table.AddColumn(new SqlColumn("ObjectTypeStockAmountVariantCode", typeof(string), SqlDbType.NVarChar, table, -1, false, false, false));
                        table.AddColumn(new SqlColumn("ObjectTypeStockAmountInventory", typeof(string), SqlDbType.NVarChar, table, -1, false, false, false));
                        break;
                    case "Order":
                        table = result.AddTable("ObjectTypeOrder");
                        table.AddColumn(new SqlColumn("@odata.etag", typeof(string), SqlDbType.NVarChar, table, -1, false, false, false));
                        table.AddColumn(new SqlColumn("EcomOrders.OrderCustomerName", typeof(string), SqlDbType.NVarChar, table, -1, false, false, false));
                        table.AddColumn(new SqlColumn("EcomOrders.OrderCustomerAddress", typeof(string), SqlDbType.NVarChar, table, -1, false, false, false));
                        table.AddColumn(new SqlColumn("EcomOrders.OrderCustomerAddress2", typeof(string), SqlDbType.NVarChar, table, -1, false, false, false));
                        table.AddColumn(new SqlColumn("EcomOrders.OrderCustomerZip", typeof(string), SqlDbType.NVarChar, table, -1, false, false, false));
                        table.AddColumn(new SqlColumn("EcomOrders.OrderCustomerCity", typeof(string), SqlDbType.NVarChar, table, -1, false, false, false));
                        table.AddColumn(new SqlColumn("ObjectTypeOrderSelltoPhoneNo", typeof(string), SqlDbType.NVarChar, table, -1, false, false, false));
                        table.AddColumn(new SqlColumn("ObjectTypeOrderSelltoEMail", typeof(string), SqlDbType.NVarChar, table, -1, false, false, false));
                        table.AddColumn(new SqlColumn("EcomOrders.OrderDeliveryAddress", typeof(string), SqlDbType.NVarChar, table, -1, false, false, false));
                        table.AddColumn(new SqlColumn("EcomOrders.OrderDeliveryAddress2", typeof(string), SqlDbType.NVarChar, table, -1, false, false, false));
                        table.AddColumn(new SqlColumn("EcomOrders.OrderDeliveryZip", typeof(string), SqlDbType.NVarChar, table, -1, false, false, false));
                        table.AddColumn(new SqlColumn("EcomOrders.OrderDeliveryCity", typeof(string), SqlDbType.NVarChar, table, -1, false, false, false));
                        table.AddColumn(new SqlColumn("EcomOrders.OrderCustomerCountryCode", typeof(string), SqlDbType.NVarChar, table, -1, false, false, false));
                        table.AddColumn(new SqlColumn("EcomOrders.OrderDiscountPercentage", typeof(string), SqlDbType.NVarChar, table, -1, false, false, false));
                        table.AddColumn(new SqlColumn("ObjectTypeOrderNo", typeof(string), SqlDbType.NVarChar, table, -1, false, false, false));
                        table.AddColumn(new SqlColumn("ObjectTypeOrderOrderDate", typeof(string), SqlDbType.NVarChar, table, -1, false, false, false));
                        table.AddColumn(new SqlColumn("ObjectTypeOrderPostingDate", typeof(string), SqlDbType.NVarChar, table, -1, false, false, false));
                        foreach (var item in Ecommerce.Services.OrderFields.GetOrderFields())
                        {
                            table.AddColumn(new SqlColumn("EcomOrders." + item.SystemName, typeof(string), SqlDbType.NVarChar, table, -1, false, false, false));
                        }
                        table = result.AddTable("ObjectTypeOrderLines");
                        table.AddColumn(new SqlColumn("@odata.etag", typeof(string), SqlDbType.NVarChar, table, -1, false, false, false));
                        table.AddColumn(new SqlColumn("ObjectTypeOrderLinesNo", typeof(string), SqlDbType.NVarChar, table, -1, false, false, false));
                        table.AddColumn(new SqlColumn("ObjectTypeOrderLinesDescription", typeof(string), SqlDbType.NVarChar, table, -1, false, false, false));
                        table.AddColumn(new SqlColumn("ObjectTypeOrderLinesTotalVATAmount", typeof(string), SqlDbType.NVarChar, table, -1, false, false, false));
                        table.AddColumn(new SqlColumn("EcomOrderLines.OrderLineUnitId", typeof(string), SqlDbType.NVarChar, table, -1, false, false, false));
                        table.AddColumn(new SqlColumn("ObjectTypeOrderLinesLineAmount", typeof(string), SqlDbType.NVarChar, table, -1, false, false, false));
                        table.AddColumn(new SqlColumn("ObjectTypeOrderLinesLineDiscountAmount", typeof(string), SqlDbType.NVarChar, table, -1, false, false, false));
                        table.AddColumn(new SqlColumn("ObjectTypeOrderLinesLineDiscountPercent", typeof(string), SqlDbType.NVarChar, table, -1, false, false, false));
                        table.AddColumn(new SqlColumn("ObjectTypeOrderLinesUnitPrice", typeof(string), SqlDbType.NVarChar, table, -1, false, false, false));
                        table.AddColumn(new SqlColumn("EcomOrderLines.OrderLineStockLocationId", typeof(string), SqlDbType.NVarChar, table, -1, false, false, false));
                        table.AddColumn(new SqlColumn("ObjectTypeOrderLinesVariantCode", typeof(string), SqlDbType.NVarChar, table, -1, false, false, false));
                        table.AddColumn(new SqlColumn("ObjectTypeOrderLinesQuantity", typeof(string), SqlDbType.NVarChar, table, -1, false, false, false));
                        foreach (var item in Ecommerce.Services.OrderLineFields.GetOrderLineFields())
                        {
                            table.AddColumn(new SqlColumn("EcomOrderLines." + item.SystemName, typeof(string), SqlDbType.NVarChar, table, -1, false, false, false));
                        }
                        break;
                    case "Currency":
                        table = result.AddTable("ObjectTypeCurrency");
                        table.AddColumn(new SqlColumn("@odata.etag", typeof(string), SqlDbType.NVarChar, table, -1, false, false, false));
                        table.AddColumn(new SqlColumn("EcomCurrencies.CurrencyCode", typeof(string), SqlDbType.NVarChar, table, -1, false, false, false));
                        table.AddColumn(new SqlColumn("EcomCurrencies.CurrencyName", typeof(string), SqlDbType.NVarChar, table, -1, false, false, false));
                        table.AddColumn(new SqlColumn("EcomCurrencies.CurrencyRate", typeof(string), SqlDbType.NVarChar, table, -1, false, false, false));
                        table.AddColumn(new SqlColumn("EcomCurrencies.CurrencyPayGatewayCode", typeof(string), SqlDbType.NVarChar, table, -1, false, false, false));
                        break;
                    case "Manufacturer":
                        table = result.AddTable("ObjectTypeManufacturer");
                        table.AddColumn(new SqlColumn("@odata.etag", typeof(string), SqlDbType.NVarChar, table, -1, false, false, false));
                        table.AddColumn(new SqlColumn("EcomManufacturers.ManufacturerId", typeof(string), SqlDbType.NVarChar, table, -1, false, false, false));
                        table.AddColumn(new SqlColumn("EcomManufacturers.ManufacturerName", typeof(string), SqlDbType.NVarChar, table, -1, false, false, false));
                        table.AddColumn(new SqlColumn("ObjectTypeManufacturerAddress", typeof(string), SqlDbType.NVarChar, table, -1, false, false, false));
                        table.AddColumn(new SqlColumn("ObjectTypeManufacturerAddress2", typeof(string), SqlDbType.NVarChar, table, -1, false, false, false));
                        table.AddColumn(new SqlColumn("EcomManufacturers.ManufacturerCity", typeof(string), SqlDbType.NVarChar, table, -1, false, false, false));
                        table.AddColumn(new SqlColumn("EcomManufacturers.ManufacturerZipCode", typeof(string), SqlDbType.NVarChar, table, -1, false, false, false));
                        table.AddColumn(new SqlColumn("EcomManufacturers.ManufacturerCountry", typeof(string), SqlDbType.NVarChar, table, -1, false, false, false));
                        table.AddColumn(new SqlColumn("EcomManufacturers.ManufacturerPhone", typeof(string), SqlDbType.NVarChar, table, -1, false, false, false));
                        table.AddColumn(new SqlColumn("EcomManufacturers.ManufacturerEmail", typeof(string), SqlDbType.NVarChar, table, -1, false, false, false));
                        table.AddColumn(new SqlColumn("EcomManufacturers.ManufacturerFax", typeof(string), SqlDbType.NVarChar, table, -1, false, false, false));
                        table.AddColumn(new SqlColumn("EcomManufacturers.ManufacturerWeb", typeof(string), SqlDbType.NVarChar, table, -1, false, false, false));
                        break;
                    case "Country":
                        table = result.AddTable("ObjectTypeCountry");
                        table.AddColumn(new SqlColumn("@odata.etag", typeof(string), SqlDbType.NVarChar, table, -1, false, false, false));
                        table.AddColumn(new SqlColumn("EcomCountries.CountryCode2", typeof(string), SqlDbType.NVarChar, table, -1, false, false, false));
                        table.AddColumn(new SqlColumn("EcomCountryText.CountryTextName", typeof(string), SqlDbType.NVarChar, table, -1, false, false, false));
                        table.AddColumn(new SqlColumn("EcomCountries.CountryNumber", typeof(string), SqlDbType.NVarChar, table, -1, false, false, false));
                        table.AddColumn(new SqlColumn("ObjectTypeCountryAddressFormat", typeof(string), SqlDbType.NVarChar, table, -1, false, false, false));
                        break;
                    case "User":
                        table = result.AddTable("ObjectTypeUser");
                        table.AddColumn(new SqlColumn("@odata.etag", typeof(string), SqlDbType.NVarChar, table, -1, false, false, false));
                        table.AddColumn(new SqlColumn("ObjectTypeUserEMail", typeof(string), SqlDbType.NVarChar, table, -1, false, false, false));
                        table.AddColumn(new SqlColumn("AccessUser.AccessUserExternalId", typeof(string), SqlDbType.NVarChar, table, -1, false, false, false));
                        table.AddColumn(new SqlColumn("AccessUser.AccessUserCustomerNumber", typeof(string), SqlDbType.NVarChar, table, -1, false, false, false));
                        table.AddColumn(new SqlColumn("AccessUser.AccessUserCompany", typeof(string), SqlDbType.NVarChar, table, -1, false, false, false));
                        table.AddColumn(new SqlColumn("AccessUser.AccessUserName", typeof(string), SqlDbType.NVarChar, table, -1, false, false, false));
                        table.AddColumn(new SqlColumn("AccessUser.AccessUserAddress", typeof(string), SqlDbType.NVarChar, table, -1, false, false, false));
                        table.AddColumn(new SqlColumn("AccessUser.AccessUserAddress2", typeof(string), SqlDbType.NVarChar, table, -1, false, false, false));
                        table.AddColumn(new SqlColumn("AccessUser.AccessUserCountryCode", typeof(string), SqlDbType.NVarChar, table, -1, false, false, false));
                        table.AddColumn(new SqlColumn("AccessUser.AccessUserZip", typeof(string), SqlDbType.NVarChar, table, -1, false, false, false));
                        table.AddColumn(new SqlColumn("AccessUser.AccessUserCity", typeof(string), SqlDbType.NVarChar, table, -1, false, false, false));
                        table.AddColumn(new SqlColumn("AccessUser.AccessUserPhone", typeof(string), SqlDbType.NVarChar, table, -1, false, false, false));
                        table.AddColumn(new SqlColumn("AccessUser.AccessUserMobile", typeof(string), SqlDbType.NVarChar, table, -1, false, false, false));
                        table.AddColumn(new SqlColumn("AccessUser.AccessUserFax", typeof(string), SqlDbType.NVarChar, table, -1, false, false, false));
                        table.AddColumn(new SqlColumn("AccessUser.AccessUserCurrencyCharacter", typeof(string), SqlDbType.NVarChar, table, -1, false, false, false));
                        foreach (var item in new User().CustomFieldValues)
                        {
                            table.AddColumn(new SqlColumn("AccessUser." + item.CustomField.SystemName, typeof(string), SqlDbType.NVarChar, table, -1, false, false, false));
                        }
                        break;
                    case "UserAddress":
                        table = result.AddTable("ObjectTypeUserAddress");
                        table.AddColumn(new SqlColumn("@odata.etag", typeof(string), SqlDbType.NVarChar, table, -1, false, false, false));
                        table.AddColumn(new SqlColumn("ObjectTypeUserAddressCode", typeof(string), SqlDbType.NVarChar, table, -1, false, false, false));
                        table.AddColumn(new SqlColumn("AccessUserAddress.AccessUserAddressCompany", typeof(string), SqlDbType.NVarChar, table, -1, false, false, false));
                        table.AddColumn(new SqlColumn("AccessUserAddress.AccessUserAddressAddress", typeof(string), SqlDbType.NVarChar, table, -1, false, false, false));
                        table.AddColumn(new SqlColumn("AccessUserAddress.AccessUserAddressAddress2", typeof(string), SqlDbType.NVarChar, table, -1, false, false, false));
                        table.AddColumn(new SqlColumn("AccessUserAddress.AccessUserAddressCity", typeof(string), SqlDbType.NVarChar, table, -1, false, false, false));
                        table.AddColumn(new SqlColumn("AccessUserAddress.AccessUserAddressZip", typeof(string), SqlDbType.NVarChar, table, -1, false, false, false));
                        table.AddColumn(new SqlColumn("AccessUserAddress.AccessUserAddressCountryCode", typeof(string), SqlDbType.NVarChar, table, -1, false, false, false));
                        table.AddColumn(new SqlColumn("AccessUserAddress.AccessUserAddressPhone", typeof(string), SqlDbType.NVarChar, table, -1, false, false, false));
                        table.AddColumn(new SqlColumn("AccessUserAddress.AccessUserAddressCell", typeof(string), SqlDbType.NVarChar, table, -1, false, false, false));
                        table.AddColumn(new SqlColumn("AccessUserAddress.AccessUserAddressFax", typeof(string), SqlDbType.NVarChar, table, -1, false, false, false));
                        table.AddColumn(new SqlColumn("AccessUserAddress.AccessUserAddressEmail", typeof(string), SqlDbType.NVarChar, table, -1, false, false, false));
                        foreach (var item in new UserAddress().CustomFieldValues)
                        {
                            table.AddColumn(new SqlColumn("AccessUserAddress." + item.CustomField.SystemName, typeof(string), SqlDbType.NVarChar, table, -1, false, false, false));
                        }
                        break;
                    case "UserCustomer":
                        table = result.AddTable("ObjectTypeUserCustomer");
                        table.AddColumn(new SqlColumn("@odata.etag", typeof(string), SqlDbType.NVarChar, table, -1, false, false, false));
                        table.AddColumn(new SqlColumn("ObjectTypeUserCustomerEMail", typeof(string), SqlDbType.NVarChar, table, -1, false, false, false));
                        table.AddColumn(new SqlColumn("ObjectTypeUserCustomerNo", typeof(string), SqlDbType.NVarChar, table, -1, false, false, false));
                        table.AddColumn(new SqlColumn("AccessUser.AccessUserName", typeof(string), SqlDbType.NVarChar, table, -1, false, false, false));
                        table.AddColumn(new SqlColumn("AccessUser.AccessUserAddress", typeof(string), SqlDbType.NVarChar, table, -1, false, false, false));
                        table.AddColumn(new SqlColumn("AccessUser.AccessUserAddress2", typeof(string), SqlDbType.NVarChar, table, -1, false, false, false));
                        table.AddColumn(new SqlColumn("AccessUser.AccessUserCountryCode", typeof(string), SqlDbType.NVarChar, table, -1, false, false, false));
                        table.AddColumn(new SqlColumn("AccessUser.AccessUserZip", typeof(string), SqlDbType.NVarChar, table, -1, false, false, false));
                        table.AddColumn(new SqlColumn("AccessUser.AccessUserCity", typeof(string), SqlDbType.NVarChar, table, -1, false, false, false));
                        table.AddColumn(new SqlColumn("AccessUser.AccessUserPhone", typeof(string), SqlDbType.NVarChar, table, -1, false, false, false));
                        table.AddColumn(new SqlColumn("AccessUser.AccessUserMobile", typeof(string), SqlDbType.NVarChar, table, -1, false, false, false));
                        table.AddColumn(new SqlColumn("AccessUser.AccessUserFax", typeof(string), SqlDbType.NVarChar, table, -1, false, false, false));
                        table.AddColumn(new SqlColumn("AccessUser.AccessUserCurrencyCharacter", typeof(string), SqlDbType.NVarChar, table, -1, false, false, false));
                        foreach (var item in new User().CustomFieldValues)
                        {
                            table.AddColumn(new SqlColumn("AccessUser." + item.CustomField.SystemName, typeof(string), SqlDbType.NVarChar, table, -1, false, false, false));
                        }
                        break;
                    case "UserCustomerShipToAddress":
                        table = result.AddTable("ObjectTypeUserCustomerShipToAddress");
                        table.AddColumn(new SqlColumn("@odata.etag", typeof(string), SqlDbType.NVarChar, table, -1, false, false, false));
                        table.AddColumn(new SqlColumn("AccessUserAddress.AccessUserAddressName", typeof(string), SqlDbType.NVarChar, table, -1, false, false, false));
                        table.AddColumn(new SqlColumn("AccessUserAddress.AccessUserAddressCompany", typeof(string), SqlDbType.NVarChar, table, -1, false, false, false));
                        table.AddColumn(new SqlColumn("AccessUserAddress.AccessUserAddressAddress", typeof(string), SqlDbType.NVarChar, table, -1, false, false, false));
                        table.AddColumn(new SqlColumn("AccessUserAddress.AccessUserAddressAddress2", typeof(string), SqlDbType.NVarChar, table, -1, false, false, false));
                        table.AddColumn(new SqlColumn("AccessUserAddress.AccessUserAddressCity", typeof(string), SqlDbType.NVarChar, table, -1, false, false, false));
                        table.AddColumn(new SqlColumn("AccessUserAddress.AccessUserAddressZip", typeof(string), SqlDbType.NVarChar, table, -1, false, false, false));
                        table.AddColumn(new SqlColumn("AccessUserAddress.AccessUserAddressCountryCode", typeof(string), SqlDbType.NVarChar, table, -1, false, false, false));
                        table.AddColumn(new SqlColumn("AccessUserAddress.AccessUserAddressPhone", typeof(string), SqlDbType.NVarChar, table, -1, false, false, false));
                        table.AddColumn(new SqlColumn("AccessUserAddress.AccessUserAddressCell", typeof(string), SqlDbType.NVarChar, table, -1, false, false, false));
                        table.AddColumn(new SqlColumn("AccessUserAddress.AccessUserAddressFax", typeof(string), SqlDbType.NVarChar, table, -1, false, false, false));
                        table.AddColumn(new SqlColumn("AccessUserAddress.AccessUserAddressEmail", typeof(string), SqlDbType.NVarChar, table, -1, false, false, false));
                        foreach (var item in new UserAddress().CustomFieldValues)
                        {
                            table.AddColumn(new SqlColumn("AccessUserAddress." + item.CustomField.SystemName, typeof(string), SqlDbType.NVarChar, table, -1, false, false, false));
                        }
                        break;
                    case "UserSalesperson":
                        table = result.AddTable("ObjectTypeUserSalesperson");
                        table.AddColumn(new SqlColumn("@odata.etag", typeof(string), SqlDbType.NVarChar, table, -1, false, false, false));
                        table.AddColumn(new SqlColumn("AccessUser.AccessUserExternalId", typeof(string), SqlDbType.NVarChar, table, -1, false, false, false));
                        table.AddColumn(new SqlColumn("AccessUser.AccessUserName", typeof(string), SqlDbType.NVarChar, table, -1, false, false, false));
                        table.AddColumn(new SqlColumn("AccessUser.AccessUserJobTitle", typeof(string), SqlDbType.NVarChar, table, -1, false, false, false));
                        table.AddColumn(new SqlColumn("AccessUser.AccessUserPhone", typeof(string), SqlDbType.NVarChar, table, -1, false, false, false));
                        table.AddColumn(new SqlColumn("ObjectTypeUserSalespersonEMail", typeof(string), SqlDbType.NVarChar, table, -1, false, false, false));
                        foreach (var item in new User().CustomFieldValues)
                        {
                            table.AddColumn(new SqlColumn("AccessUser." + item.CustomField.SystemName, typeof(string), SqlDbType.NVarChar, table, -1, false, false, false));
                        }
                        break;
                    case "Discount":
                        table = result.AddTable("ObjectTypeDiscount");
                        table.AddColumn(new SqlColumn("@odata.etag", typeof(string), SqlDbType.NVarChar, table, -1, false, false, false));
                        table.AddColumn(new SqlColumn("EcomDiscount.DiscountValidFrom", typeof(string), SqlDbType.NVarChar, table, -1, false, false, false));
                        table.AddColumn(new SqlColumn("EcomDiscount.DiscountPercentage", typeof(string), SqlDbType.NVarChar, table, -1, false, false, false));
                        table.AddColumn(new SqlColumn("EcomDiscount.DiscountValidTo", typeof(string), SqlDbType.NVarChar, table, -1, false, false, false));
                        break;
                    case "Price":
                        table = result.AddTable("ObjectTypePrice");
                        table.AddColumn(new SqlColumn("@odata.etag", typeof(string), SqlDbType.NVarChar, table, -1, false, false, false));
                        table.AddColumn(new SqlColumn("EcomPrices.PriceProductId", typeof(string), SqlDbType.NVarChar, table, -1, false, false, false));
                        table.AddColumn(new SqlColumn("EcomPrices.PriceCurrency", typeof(string), SqlDbType.NVarChar, table, -1, false, false, false));
                        table.AddColumn(new SqlColumn("EcomPrices.PriceQuantity", typeof(string), SqlDbType.NVarChar, table, -1, false, false, false));
                        table.AddColumn(new SqlColumn("EcomPrices.PriceAmount", typeof(string), SqlDbType.NVarChar, table, -1, false, false, false));
                        table.AddColumn(new SqlColumn("EcomPrices.PriceUnitId", typeof(string), SqlDbType.NVarChar, table, -1, false, false, false));
                        table.AddColumn(new SqlColumn("EcomPrices.PriceValidFrom", typeof(string), SqlDbType.NVarChar, table, -1, false, false, false));
                        table.AddColumn(new SqlColumn("EcomPrices.PriceValidTo", typeof(string), SqlDbType.NVarChar, table, -1, false, false, false));
                        table.AddColumn(new SqlColumn("EcomPrices.PriceIsWithVat", typeof(string), SqlDbType.NVarChar, table, -1, false, false, false));
                        break;
                    case "PriceAndDiscount":
                        table = result.AddTable("ObjectTypePriceAndDiscount");
                        table.AddColumn(new SqlColumn("@odata.etag", typeof(string), SqlDbType.NVarChar, table, -1, false, false, false));
                        table.AddColumn(new SqlColumn("EcomPrices.PriceCurrency", typeof(string), SqlDbType.NVarChar, table, -1, false, false, false));
                        table.AddColumn(new SqlColumn("ObjectTypePriceAndDiscountStartingDate", typeof(string), SqlDbType.NVarChar, table, -1, false, false, false));
                        table.AddColumn(new SqlColumn("ObjectTypePriceAndDiscountEndingDate", typeof(string), SqlDbType.NVarChar, table, -1, false, false, false));
                        table.AddColumn(new SqlColumn("ObjectTypePriceAndDiscountProductNo", typeof(string), SqlDbType.NVarChar, table, -1, false, false, false));
                        table.AddColumn(new SqlColumn("EcomPrices.PriceUnitId", typeof(string), SqlDbType.NVarChar, table, -1, false, false, false));
                        table.AddColumn(new SqlColumn("EcomPrices.PriceQuantity", typeof(string), SqlDbType.NVarChar, table, -1, false, false, false));
                        table.AddColumn(new SqlColumn("EcomPrices.PriceAmount", typeof(string), SqlDbType.NVarChar, table, -1, false, false, false));
                        table.AddColumn(new SqlColumn("EcomDiscount.DiscountPercentage", typeof(string), SqlDbType.NVarChar, table, -1, false, false, false));
                        table.AddColumn(new SqlColumn("EcomPrices.PriceIsWithVat", typeof(string), SqlDbType.NVarChar, table, -1, false, false, false));
                        break;
                }
            }
            _schema = result;
            return result;
        }

        private static string GetSqlForSchemaBuilding()
        {
            const string sql = "SELECT c.table_name,  c.column_name, Data_type, CHARACTER_MAXIMUM_LENGTH,hasIdentity, c.table_schema," +
                               " CASE WHEN c.table_name IN (SELECT table_name FROM sys.objects join INFORMATION_SCHEMA.KEY_COLUMN_USAGE ON name=constraint_name WHERE TYPE = 'PK' AND c.TABLE_CATALOG = TABLE_CATALOG AND c.TABLE_SCHEMA = TABLE_SCHEMA AND c.TABLE_NAME = TABLE_NAME AND c.COLUMN_NAME = COLUMN_NAME ) THEN 1 ELSE 0 END AS IsPrimaryKey " +
                               " FROM INFORMATION_SCHEMA.COLUMNS  c " +
                               " LEFT JOIN (" +
                               " SELECT Name,OBJECT_NAME(id) as tableName, COLUMNPROPERTY(id, name, 'IsIdentity') as hasIdentity, OBJECTPROPERTY(id,'IsPrimaryKey') as isPrimaryKey FROM syscolumns WHERE COLUMNPROPERTY(id,name,'IsIdentity') !=2) as id " +
                               " ON c.COLUMN_NAME=name AND c.TABLE_NAME=tableName, " +
                               " INFORMATION_SCHEMA.TABLES " +
                               " WHERE c.TABLE_NAME=INFORMATION_SCHEMA.TABLES.TABLE_NAME AND TABLE_TYPE<>'view' ORDER BY c.TABLE_NAME,ORDINAL_POSITION";
            return sql;
        }

        public override void OverwriteDestinationSchemaToOriginal()
        {
            _schema = GetOriginalDestinationSchema();
        }

        public override string Serialize()
        {
            var document = new XDocument(new XDeclaration("1.0", "utf-8", string.Empty));
            var root = new XElement("Parameters");
            document.Add(root);
            root.Add(CreateParameterNode(GetType(), "Source object type", SourceObjectType));
            root.Add(CreateParameterNode(GetType(), "Object type", ObjectType));
            root.Add(CreateParameterNode(GetType(), "Destination language", DestinationLanguage));
            root.Add(CreateParameterNode(GetType(), "Source language code", SourceLanguageCode));
            root.Add(CreateParameterNode(GetType(), "Default currency", Defaultcurrency));
            root.Add(CreateParameterNode(GetType(), "Customer has contacts", Customerhascontacts));
            root.Add(CreateParameterNode(GetType(), "Shop", Shop));
            root.Add(CreateParameterNode(GetType(), "Product group", Group));
            root.Add(CreateParameterNode(GetType(), "User group", UserGroup));
            root.Add(CreateParameterNode(GetType(), "Source user group", SourceUserGroup));
            root.Add(CreateParameterNode(GetType(), "Anonymous user", AnonymousUser));
            root.Add(CreateParameterNode(GetType(), "Discount name prefix", DiscountNamePrefix));
            root.Add(CreateParameterNode(GetType(), "Merge compared discounts together", MergeDiscounts));
            root.Add(CreateParameterNode(GetType(), "Sales type customer", SalesTypeCustomer.ToString()));
            root.Add(CreateParameterNode(GetType(), "Sales type all customer", SalesTypeAllCustomer.ToString()));
            root.Add(CreateParameterNode(GetType(), "Sales type customer price group", SalesTypeCustomerPriceGroup.ToString()));
            root.Add(CreateParameterNode(GetType(), "Sales type customer discount group", SalesTypeCustomerDiscountGroup.ToString()));
            return document.ToString();
        }

        public override string ValidateSourceSettings()
        {
            if (string.IsNullOrEmpty(SourceObjectType))
            {
                return "Source object type can not be empty. Please select any source object type";
            }
            return null;
        }

        public override string ValidateDestinationSettings()
        {
            if (string.IsNullOrEmpty(ObjectType))
            {
                return "Object type can not be empty. Please select any object type";
            }
            return null;
        }

        public override void SaveAsXml(XmlTextWriter textWriter)
        {
            textWriter.WriteElementString("Sourceobjecttype", SourceObjectType);
            textWriter.WriteElementString("Objecttype", ObjectType);
            textWriter.WriteElementString("Defaultlanguage", DestinationLanguage);
            textWriter.WriteElementString("Sourcelanguagecode", SourceLanguageCode);
            textWriter.WriteElementString("Defaultcurrency", Defaultcurrency);
            textWriter.WriteElementString("Customerhascontacts", Customerhascontacts);
            textWriter.WriteElementString("Shop", Shop);
            textWriter.WriteElementString("Productgroup", Group);
            textWriter.WriteElementString("Usergroup", UserGroup);
            textWriter.WriteElementString("Sourceusergroup", SourceUserGroup);
            textWriter.WriteElementString("Anonymoususer", AnonymousUser);
            textWriter.WriteElementString("Discountnameprefix", DiscountNamePrefix);
            textWriter.WriteElementString("Mergecompareddiscountstogether", MergeDiscounts);
            textWriter.WriteElementString("Salestypecustomer", SalesTypeCustomer.ToString());
            textWriter.WriteElementString("Salestypeallcustomer", SalesTypeAllCustomer.ToString());
            textWriter.WriteElementString("Salestypecustomerpricegroup", SalesTypeCustomerPriceGroup.ToString());
            textWriter.WriteElementString("Salestypecustomerdiscountgroup", SalesTypeCustomerDiscountGroup.ToString());
            GetSchema().SaveAsXml(textWriter);
        }
        public override void UpdateSourceSettings(ISource source)
        {
            var newProvider = (ODataProvider)source;
            SourceObjectType = newProvider.SourceObjectType;
            SourceUserGroup = newProvider.SourceUserGroup;
        }
        public override void UpdateDestinationSettings(IDestination destination)
        {
            var newProvider = (ODataProvider)destination;
            ObjectType = newProvider.ObjectType;
            DestinationLanguage = newProvider.DestinationLanguage;
            SourceLanguageCode = newProvider.SourceLanguageCode;
            Defaultcurrency = newProvider.Defaultcurrency;
            Customerhascontacts = newProvider.Customerhascontacts;
            Shop = newProvider.Shop;
            Group = newProvider.Group;
            UserGroup = newProvider.UserGroup;
            AnonymousUser = newProvider.AnonymousUser;
            DiscountNamePrefix = newProvider.DiscountNamePrefix;
            MergeDiscounts = newProvider.MergeDiscounts;
            SalesTypeCustomer = newProvider.SalesTypeCustomer;
            SalesTypeAllCustomer = newProvider.SalesTypeAllCustomer;
            SalesTypeCustomerPriceGroup = newProvider.SalesTypeCustomerPriceGroup;
            SalesTypeCustomerDiscountGroup = newProvider.SalesTypeCustomerDiscountGroup;
        }

        public override bool RunJob(Job job)
        {
            ReplaceMappingConditionalsWithValuesFromRequest(job);
            var endpointProvider = (IODataBaseProvider)job.Source;
            try
            {
                var mapping = job.Mappings[0];
                if (mapping.DestinationTable == null && mapping.SourceTable == null)
                {
                    return false;
                }
                if (string.IsNullOrEmpty(endpointProvider.GetEndpointId()))
                {
                    Logger?.Log($"This data integration activity cannot run because source does not have an endpoint defined.");
                    return false;
                }
                bool deleteMissingRows = false;
                if (endpointProvider.GetMode().Equals("Delete"))
                {
                    deleteMissingRows = true;
                }
                var productService = Ecommerce.Services.Products;
                var variantOptionService = Ecommerce.Services.VariantOptions;
                var variantService = Ecommerce.Services.Variants;
                var variantGroupService = Ecommerce.Services.VariantGroups;
                var groupService = Ecommerce.Services.ProductGroups;
                var stockService = Ecommerce.Services.StockService;
                var orderService = Ecommerce.Services.Orders;
                var countryService = Ecommerce.Services.Countries;
                var discountService = Ecommerce.Services.Discounts;
                var productReader = new ProductReader(productService, groupService);
                var productWriter = new ProductWriter(productService);
                var unitOfMeasureService = new Ecommerce.Stocks.UnitOfMeasureService();
                var variantReader = new ProductVariantReader(variantOptionService, variantGroupService, productService);
                var variantWriter = new ProductVariantWriter(variantService, variantOptionService, variantGroupService, productService);
                var unitReader = new ProductUnitRelationReader(variantOptionService, variantGroupService, stockService, unitOfMeasureService);
                var unitWriter = new ProductUnitRelationWriter(variantOptionService, variantService, stockService, unitOfMeasureService);
                var itemCategoryReader = new ProductGroupReader(groupService);
                var itemCategoryWriter = new ProductGroupWriter(groupService);
                var stockLocationReader = new StockLocationReader(stockService);
                var stockLocationWriter = new StockLocationWriter(stockService);
                var currencyWriter = new CurrencyWriter(_currenciesService);
                var currencyReader = new CurrencyReader(_currenciesService);
                var importAll = mapping.DestinationTable == null || mapping.SourceTable == null || job.CreateMappingAtRuntime;

                var odataDestinationWriter = new ODataDestinationWriter(Logger, mapping, importAll, DestinationLanguage, orderService, deleteMissingRows, endpointProvider.GetEndpointId());
                switch (ObjectType)
                {
                    case "Product":
                        RunWriter(job, mapping, importAll, ObjectType, () => { return new ODataProductWriter(odataDestinationWriter, productWriter, productReader, Shop, _groupService.GetGroup(Group)); });
                        break;
                    case "ProductVariant":
                        RunWriter(job, mapping, importAll, ObjectType, () => { return new ODataProductVariantWriter(odataDestinationWriter, variantReader, variantWriter, productReader, productWriter); });
                        break;
                    case "ProductUnitRelation":
                        RunWriter(job, mapping, importAll, ObjectType, () => { return new ODataProductUnitRelationWriter(odataDestinationWriter, unitWriter, unitReader, variantReader, productReader); });
                        break;
                    case "ProductUnit":
                        var existingVariantGroup = variantGroupService.GetVariantGroup("NavUnits");
                        if (existingVariantGroup == null)
                        {
                            var newVariantGroup = new VariantGroup { Id = "NavUnits", Unit = true };
                            newVariantGroup.SetName(DestinationLanguage, "NAV Units");
                            variantGroupService.Save(newVariantGroup);
                        }
                        RunWriter(job, mapping, importAll, ObjectType, () => { return new ODataProductUnitWriter(odataDestinationWriter, unitWriter, unitReader); });
                        break;
                    case "ProductTranslation":
                        RunWriter(job, mapping, importAll, ObjectType, () => { return new ODataProductTranslationWriter(odataDestinationWriter, variantReader, variantWriter, productReader, productWriter, SourceLanguageCode); });
                        break;
                    case "ProductUnitTranslation":
                        RunWriter(job, mapping, importAll, ObjectType, () => { return new ODataProductUnitTranslationWriter(odataDestinationWriter, unitWriter, unitReader, SourceLanguageCode); });
                        break;
                    case "ProductGroup":
                        using (var writer = new ODataProductGroupWriter(odataDestinationWriter, Shop, _groupService.GetGroup(Group), itemCategoryReader, itemCategoryWriter))
                        {
                            if ((mapping.Active && mapping.GetColumnMappings().Count > 0) || importAll)
                            {
                                using (var reader = job.Source.GetReader(mapping))
                                {
                                    WriteData(reader, mapping, writer, "Product group");
                                    if (writer.NotImplementedGroups.Count > 0)
                                    {
                                        writer.TryAddingSkippedObjecs();
                                    }
                                    writer.RemoveRowsNotInEndpoint();
                                }
                            }
                        }
                        break;
                    case "StockLocation":
                        RunWriter(job, mapping, importAll, ObjectType, () => { return new ODataStockLocationWriter(odataDestinationWriter, stockLocationWriter, stockLocationReader); });
                        break;
                    case "StockAmount":
                        RunWriter(job, mapping, importAll, ObjectType, () => { return new ODataStockAmountWriter(odataDestinationWriter, variantReader, stockLocationReader, unitWriter, unitReader, productReader); });
                        break;
                    case "Order":
                        RunWriter(job, mapping, importAll, ObjectType, () => { return new ODataOrderWriter(odataDestinationWriter, stockLocationReader, _currenciesService, countryService, _languageService, Shop, AnonymousUser, job.Mappings.Where(obj => obj.DestinationTable.Name == "ObjectTypeOrderLines").FirstOrDefault()); });
                        break;
                    case "Currency":
                        RunWriter(job, mapping, importAll, ObjectType, () => { return new ODataCurrencyWriter(odataDestinationWriter, currencyReader, currencyWriter); });
                        break;
                    case "Manufacturer":
                        RunWriter(job, mapping, importAll, ObjectType, () => { return new ODataManufacturerWriter(odataDestinationWriter); });
                        break;
                    case "Country":
                        RunWriter(job, mapping, importAll, ObjectType, () => { return new ODataCountryWriter(odataDestinationWriter, countryService); });
                        break;
                    case "User":
                        RunWriter(job, mapping, importAll, ObjectType, () => { return new ODataUserWriter(odataDestinationWriter, UserGroup); });
                        break;
                    case "UserAddress":
                        RunWriter(job, mapping, importAll, ObjectType, () => { return new ODataUserAddressWriter(odataDestinationWriter); });
                        break;
                    case "UserCustomer":
                        RunWriter(job, mapping, importAll, ObjectType, () => { return new ODataUserCustomerWriter(odataDestinationWriter, Customerhascontacts, UserGroup); });
                        break;
                    case "UserCustomerShipToAddress":
                        RunWriter(job, mapping, importAll, ObjectType, () => { return new ODataUserCustomerShiptoAddressWriter(odataDestinationWriter); });
                        break;
                    case "UserSalesperson":
                        RunWriter(job, mapping, importAll, ObjectType, () => { return new ODataUserSalespersonWriter(odataDestinationWriter, UserGroup); });
                        break;
                    case "Discount":
                        ODataDiscountWriter oDataDiscountWriter = new ODataDiscountWriter(odataDestinationWriter, unitReader, discountService, Shop, DiscountNamePrefix, MergeDiscounts, SalesTypeCustomer, SalesTypeAllCustomer, SalesTypeCustomerDiscountGroup);
                        Logger?.Log($"Starting OData import for {ObjectType}.");
                        oDataDiscountWriter.Write(new Dictionary<string, object>());
                        Logger?.Log($"Finished OData import for {ObjectType}.");
                        break;
                    case "Price":
                        ODataPriceWriter oDataPriceWriter = new ODataPriceWriter(odataDestinationWriter, currencyReader, Shop, SalesTypeCustomer, SalesTypeAllCustomer, SalesTypeCustomerPriceGroup);
                        Logger?.Log($"Starting OData import for {ObjectType}.");
                        oDataPriceWriter.Write(new Dictionary<string, object>());
                        Logger?.Log($"Finished OData import for {ObjectType}.");
                        break;
                    case "PriceAndDiscount":
                        ODataPriceAndDiscountWriter oDataPriceAndDiscountWriter = new ODataPriceAndDiscountWriter(odataDestinationWriter, currencyReader, Shop, unitReader, discountService, DiscountNamePrefix, MergeDiscounts);
                        Logger?.Log($"Starting OData import for {ObjectType}.");
                        oDataPriceAndDiscountWriter.Write(new Dictionary<string, object>());
                        Logger?.Log($"Finished OData import for {ObjectType}.");
                        break;
                    case "Custom":
                        DynamicwebProvider.DynamicwebProvider dynamicwebProvider = new DynamicwebProvider.DynamicwebProvider
                        {
                            Logger = Logger,
                            SkipFailingRows = true,
                            RemoveMissingAfterImport = deleteMissingRows
                        };
                        dynamicwebProvider.RunJob(job);
                        break;
                }
            }
            catch (Exception ex)
            {
                LogManager.System.GetLogger(LogCategory.Application, "Dataintegration").Error($"{GetType().Name} error: {ex.Message} Stack: {ex.StackTrace}", ex);
                if (ex is TimeoutException)
                {
                    Logger?.Error(ex.Message);
                    return false;
                }
                if (_restOfSourceRow != null)
                {
                    endpointProvider.SaveHighWaterMarkFile(_restOfSourceRow);
                }
                return false;
            }
            return true;
        }

        private void RunWriter<T>(Job job, Mapping mapping, bool importAll, string type, Func<T> createWriter) where T : IDisposable, IODataDestinationWriter, IDestinationWriter
        {
            using (var writer = createWriter())
            {
                if ((mapping.Active && mapping.GetColumnMappings().Count > 0) || importAll)
                {
                    using (var reader = job.Source.GetReader(mapping))
                    {
                        WriteData(reader, mapping, writer, type);
                        writer.RemoveRowsNotInEndpoint();
                    }
                }
            }
        }

        private void WriteData(ISourceReader sourceReader, Mapping mapping, IDestinationWriter writer, string type)
        {
            Logger?.Log($"Starting OData import for {type}.");
            try
            {
                while (!sourceReader.IsDone())
                {
                    var sourceRow = sourceReader.GetNext();
                    _currentSourceRow = sourceRow;
                    ProcessInputRow(mapping, sourceRow);
                    writer.Write(sourceRow);
                }
            }
            catch (Exception e)
            {
                Logger?.Log(e.ToString());
                Logger?.Log("HighWaterMarkFile is started.");
                _restOfSourceRow = new List<Dictionary<string, object>> { _currentSourceRow };
                while (!sourceReader.IsDone())
                {
                    _restOfSourceRow.Add(sourceReader.GetNext());
                }
                throw e;
            }

            Logger?.Log($"Finished OData import for {type}.");
        }
        private Hashtable GetDefaultCurrency()
        {
            var options = new Hashtable();
            Currency currency = _currenciesService.GetDefaultCurrency();
            if (currency != null)
            {
                options.Add(currency.GetName(DestinationLanguage), currency.GetName(DestinationLanguage));
            }
            return options;
        }
        private Hashtable GetUserGroups(bool onlyParrentGroups)
        {
            var options = new Hashtable();
            var userGroups = Security.UserManagement.Group.GetGroups();
            foreach (var item in userGroups)
            {
                if (!onlyParrentGroups || (item.ParentID == 0 && onlyParrentGroups))
                {
                    options.Add(item.ID, item.Name);
                }
            }
            return options;
        }
        private Hashtable GetShopGroups(string shopId)
        {
            var options = new Hashtable();
            if (!string.IsNullOrEmpty(shopId))
            {
                var shop = _shopService.GetShop(shopId);
                if (shop != null)
                {
                    foreach (var group in shop.GetGroups(_languageService.GetDefaultLanguageId()))
                    {
                        options.Add(group.Id, group.Name);
                    }
                }
            }
            return options;
        }

        public List<string> GetParametersToHide(string dropdownName, string optionKey)
        {
            List<string> result = new List<string>();
            if (dropdownName == "Object type")
            {
                result.Add("Destination language");
                result.Add("Shop");
                result.Add("Source language code");
                result.Add("Product group");
                result.Add("User group");
                result.Add("Default currency");
                result.Add("Customer has contacts");
                result.Add("Anonymous user");
                result.Add("Last date modified");
                result.Add("Discount name prefix");
                result.Add("Merge compared discounts together");
                result.Add("Sales type customer");
                result.Add("Sales type all customer");
                result.Add("Sales type customer price group");
                result.Add("Sales type customer discount group");
                switch (optionKey)
                {
                    case "Product":
                        result.Remove("Destination language");
                        result.Remove("Shop");
                        result.Remove("Product group");
                        result.Remove("Last date modified");
                        break;
                    case "ProductVariant":
                        result.Remove("Destination language");
                        break;
                    case "ProductUnitRelation":
                        result.Remove("Destination language");
                        break;
                    case "ProductUnit":
                        result.Remove("Destination language");
                        break;
                    case "ProductTranslation":
                        result.Remove("Destination language");
                        result.Remove("Source language code");
                        break;
                    case "ProductUnitTranslation":
                        result.Remove("Destination language");
                        result.Remove("Source language code");
                        break;
                    case "ProductGroup":
                        result.Remove("Destination language");
                        result.Remove("Shop");
                        result.Remove("Product group");
                        break;
                    case "StockLocation":
                        result.Remove("Destination language");
                        break;
                    case "StockAmount":
                        result.Remove("Destination language");
                        result.Remove("Last date modified");
                        break;
                    case "Order":
                        result.Remove("Destination language");
                        result.Remove("Shop");
                        result.Remove("Anonymous user");
                        result.Remove("Last date modified");
                        break;
                    case "Currency":
                        result.Remove("Default currency");
                        result.Remove("Destination language");
                        result.Remove("Last date modified");
                        break;
                    case "Manufacturer":
                        result.Remove("Last date modified");
                        break;
                    case "Country":
                        result.Remove("Destination language");
                        break;
                    case "User":
                        result.Remove("User group");
                        result.Remove("Last date modified");
                        break;
                    case "UserAddress":
                        break;
                    case "UserCustomer":
                        result.Remove("User group");
                        result.Remove("Customer has contacts");
                        result.Remove("Last date modified");
                        break;
                    case "UserCustomerShipToAddress":
                        result.Remove("Last date modified");
                        break;
                    case "UserSalesperson":
                        result.Remove("User group");
                        break;
                    case "Discount":
                        result.Remove("Destination language");
                        result.Remove("Shop");
                        result.Remove("Discount name prefix");
                        result.Remove("Merge compared discounts together");
                        result.Remove("Sales type customer");
                        result.Remove("Sales type all customer");
                        result.Remove("Sales type customer discount group");
                        break;
                    case "Price":
                        result.Remove("Destination language");
                        result.Remove("Shop");
                        result.Remove("Sales type customer");
                        result.Remove("Sales type all customer");
                        result.Remove("Sales type customer price group");
                        break;
                    case "PriceAndDiscount":
                        result.Remove("Destination language");
                        result.Remove("Shop");
                        result.Remove("Discount name prefix");
                        result.Remove("Merge compared discounts together");
                        break;
                    case "Custom":
                        break;
                }
            }
            else if (dropdownName == "Source object type")
            {
                result.Add("Source user group");
                switch (optionKey)
                {
                    case "User":
                        result.Remove("Source user group");
                        break;
                    case "UserAddress":
                        result.Remove("Source user group");
                        break;
                    case "UserCustomer":
                        result.Remove("Source user group");
                        break;
                    case "UserCustomerShipToAddress":
                        result.Remove("Source user group");
                        break;
                }
            }
            return result;
        }

        public List<string> GetSectionsToHide(string dropdownName, string optionKey)
        {
            List<string> result = new List<string>();
            //if (dropdownName == "Group")
            //{
            //    result.Add("Language settings");
            //}
            return result;
        }

        public string GetEndpointId()
        {
            return "";
        }

        public string GetSourceObjectType()
        {
            return SourceObjectType;
        }

        public string GetSourceUserGroup()
        {
            return SourceUserGroup;
        }

        public string GetMode()
        {
            return "";
        }

        public string GetEndpointType()
        {
            return "";
        }

        public void SaveHighWaterMarkFile(List<Dictionary<string, object>> sourceRow)
        {
        }
    }
}
