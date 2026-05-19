package org.eclipse.slm.irs.config;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

import java.util.List;
import java.util.Map;

@JsonIgnoreProperties(ignoreUnknown = true)
public class AasServersConfig {

    private List<AasServer> aasServers;

    public List<AasServer> getAasServers() {
        return aasServers;
    }
    public void setAasServers(List<AasServer> aasServers) {
        this.aasServers = aasServers;
    }

    public static class AasServiceConfig {
        private AasServiceType serviceType;
        private String serviceUrl;

        public AasServiceType getServiceType() { return serviceType; }
        public void setServiceType(AasServiceType serviceType) { this.serviceType = serviceType; }

        public String getServiceUrl() { return serviceUrl; }
        public void setServiceUrl(String serviceUrl) { this.serviceUrl = serviceUrl; }
    }

    public static class AasServer {
        private String name;
        private String url;
        private Map<AasServiceType, AasServiceConfig> serviceUrls = Map.of();
        private Auth auth;

        public String getName() { return name; }
        public void setName(String name) { this.name = name; }

        public String getUrl() { return url; }
        public void setUrl(String url) {
            this.url = url;
        }

        public Auth getAuth() { return auth; }
        public void setAuth(Auth auth) { this.auth = auth; }

        public Map<AasServiceType, AasServiceConfig> getServiceUrls() { return serviceUrls; }
        public void setServiceUrls(Map<AasServiceType, AasServiceConfig> serviceUrls) { this.serviceUrls = serviceUrls; }

        public String getDiscovery() {
            if (serviceUrls.containsKey(AasServiceType.DISCOVERY)) {
                return serviceUrls.get(AasServiceType.DISCOVERY).getServiceUrl();
            } else {
                return url;
            }
        }

        public String getShellRegistry() {
            if (serviceUrls.containsKey(AasServiceType.SHELL_REGISTRY)) {
                return serviceUrls.get(AasServiceType.SHELL_REGISTRY).getServiceUrl();
            } else {
              return url;
            }
        }

        public String getShellRepository() {
            if (serviceUrls.containsKey(AasServiceType.SHELL_REPOSITORY)) {
                return serviceUrls.get(AasServiceType.SHELL_REPOSITORY).getServiceUrl();
            } else {
                return url;
            }
        }

        public String getSubmodelRegistry() {
            if (serviceUrls.containsKey(AasServiceType.SUBMODEL_REGISTRY)) {
                return serviceUrls.get(AasServiceType.SUBMODEL_REGISTRY).getServiceUrl();
            } else {
                return url;
            }
        }

        public String getSubmodelRepository() {
            if (serviceUrls.containsKey(AasServiceType.SUBMODEL_REPOSITORY)) {
                return serviceUrls.get(AasServiceType.SUBMODEL_REPOSITORY).getServiceUrl();
            } else {
                return url;
            }
        }

        public static class Auth {
            private String authType;
            private String clientId;
            private String clientSecret;
            private String tokenUrl;
            private String apiKey;
            private String secret;
            private String loginUrl;
            private String username;
            private String password;

            public String getAuthType() { return authType; }
            public void setAuthType(String authType) { this.authType = authType; }

            public String getClientId() { return clientId; }
            public void setClientId(String clientId) { this.clientId = clientId; }

            public String getClientSecret() { return clientSecret; }
            public void setClientSecret(String clientSecret) { this.clientSecret = clientSecret; }

            public String getTokenUrl() { return tokenUrl; }
            public void setTokenUrl(String tokenUrl) { this.tokenUrl = tokenUrl; }

            public String getApiKey() { return apiKey; }
            public void setApiKey(String apiKey) { this.apiKey = apiKey; }

            public String getSecret() { return secret; }
            public void setSecret(String secret) { this.secret = secret; }

            public String getLoginUrl() { return loginUrl; }
            public void setLoginUrl(String loginUrl) { this.loginUrl = loginUrl; }

            public String getUsername() {
                return username;
            }

            public void setUsername(String username) {
                this.username = username;
            }

            public String getPassword() {
                return password;
            }

            public void setPassword(String password) {
                this.password = password;
            }
        }
    }
}
