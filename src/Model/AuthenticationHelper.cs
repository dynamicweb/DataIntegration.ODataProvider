using Dynamicweb.Configuration;
using Dynamicweb.DataIntegration.EndpointManagement;
using Dynamicweb.Logging;
using Dynamicweb.Security.SystemTools;
using Microsoft.Identity.Client;
using System;
using System.Collections.Generic;

namespace Dynamicweb.DataIntegration.Providers.ODataProvider.Model
{
    [Obsolete("Delete this class when updating package reference for Dynamicweb.DataIntegration to version 3.1.0 or higher")]
    internal class AuthenticationHelper
    {
        private List<string> _tokenBasedTypes = new List<string>() { "OAuth2", "OAuth2Crm", "S2S", "UserImpersonation" };
        private static readonly bool EnableLogging = SystemConfiguration.Instance.GetBoolean("/Globalsettings/Modules/EndpointManagement/EnableLog");
        private static readonly string LogFolderName = "EndpointManagement";
        private static ILogger _msalLogger = null;

        internal bool IsTokenBased(EndpointAuthentication endpointAuthentication)
        {
            string authenticationType = endpointAuthentication.Type.ToString();
            return _tokenBasedTypes.Contains(authenticationType);
        }

        internal string GetToken(Endpoint endpoint, EndpointAuthentication endpointAuthentication)
        {
            if (IsTokenBased(endpointAuthentication))
            {
                string authenticationType = endpointAuthentication.Type.ToString();
                return authenticationType.Equals("s2s", StringComparison.OrdinalIgnoreCase) ? GetS2SToken(endpointAuthentication, endpoint) : OAuthHelper.GetToken(endpointAuthentication);
            }
            else
            {
                return null;
            }
        }

        private string GetS2SToken(EndpointAuthentication endpointAuthentication, Endpoint endpoint)
        {
            string accessToken = null;
            if (endpoint != null && endpointAuthentication?.Parameters != null)
            {
                if (GetCredentials(endpointAuthentication, out string tenantId, out string clientId, out string clientSecret))
                {
                    IConfidentialClientApplication confidentialClient = CreateBuilder(tenantId, clientId, clientSecret);
                    if (confidentialClient != null)
                    {
                        try
                        {
                            var url = new Uri(endpoint.Url);
                            var scopes = new List<string> { $"{url.Scheme}://{url.Host}/.default", "offline_access" };
                            accessToken = confidentialClient.AcquireTokenForClient(scopes).ExecuteAsync().Result.AccessToken;
                        }
                        catch (Exception ex)
                        {
                            string msg = $"GetTokenForClientCredentialsFlow failed: {ex?.Message}. Tenant:{tenantId} Client:{clientId}.";
                            throw new Exception(msg, ex);
                        }
                    }
                }
            }
            return accessToken;
        }

        private IConfidentialClientApplication CreateBuilder(string tenantId, string clientId, string clientSecret)
        {
            var authorityUri = $"https://login.microsoftonline.com/{tenantId}";
            string redirectUrl = GetRedirectUrl("/Admin/Public/Module/EndpointManagement/EndpointAuthorization.aspx");
            var confidentialClient = ConfidentialClientApplicationBuilder
                   .Create(clientId)
                   .WithClientSecret(clientSecret)
                   .WithAuthority(new Uri(authorityUri))
                   .WithRedirectUri(redirectUrl)
                   .WithLogging(LogCallbackMethod)
                   .Build();
            return confidentialClient;
        }

        private string GetRedirectUrl(string path)
        {
            return $"{GetHostUrl()}{path}";
        }

        private string GetHostUrl()
        {
            System.Text.StringBuilder ret = new System.Text.StringBuilder();

            var url = GetRequestUrl();

            ret.AppendFormat("{0}://{1}", url.Scheme, url.Host);

            if (url.Port != 80)
                ret.AppendFormat(":{0}", url.Port);

            return ret.ToString();
        }

        private Uri GetRequestUrl()
        {
            var hostUri = Context.Current?.Request != null ? Context.Current.Request.Url : null;
            if (hostUri == null)
            {
                throw new Exception($"OAuthHelper: Can not determine website Host Url");
            }
            return hostUri;
        }

        private bool GetCredentials(EndpointAuthentication endpointAuthentication, out string tenantId, out string clientId, out string clientSecret)
        {
            tenantId = null;
            clientId = null;
            clientSecret = null;
            if (endpointAuthentication?.Parameters != null)
            {
                if (endpointAuthentication.Parameters.TryGetValue("TenantId", out tenantId) &&
                    endpointAuthentication.Parameters.TryGetValue("ClientId", out clientId) &&
                    TryGetParameterDecryptedValue("ClientSecret", endpointAuthentication, out clientSecret))
                {
                    return true;
                }
            }
            return false;
        }

        private bool TryGetParameterDecryptedValue(string key, EndpointAuthentication endpointAuthentication, out string value)
        {
            value = null;
            if (endpointAuthentication.Parameters != null && endpointAuthentication.Parameters.TryGetValue(key, out value))
            {
                if (endpointAuthentication.Encrypted)
                    value = Crypto.Decrypt(value);
                return true;
            }
            else return false;
        }

        private ILogger GetMsalLogger()
        {
            if (EnableLogging)
            {
                if (_msalLogger == null)
                {
                    string logfileName = "MSAL.NET.log";
                    _msalLogger = LogManager.Current.GetLogger(LogFolderName, logfileName);
                }
                return _msalLogger;
            }
            return null;
        }

        private void LogCallbackMethod(Microsoft.Identity.Client.LogLevel level, string message, bool containsPii)
        {
            var logger = GetMsalLogger();
            if (logger != null)
            {
                switch (level)
                {
                    case Microsoft.Identity.Client.LogLevel.Error:
                        logger.Error(message);
                        break;
                    case Microsoft.Identity.Client.LogLevel.Warning:
                        logger.Warn(message);
                        break;
                    case Microsoft.Identity.Client.LogLevel.Info:
                        logger.Info(message);
                        break;
                    default:
                        logger.Log(message);
                        break;
                }
            }
        }
    }
}
