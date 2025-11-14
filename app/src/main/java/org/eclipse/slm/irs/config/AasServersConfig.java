package org.eclipse.slm.irs.config;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.PropertySource;

import java.util.List;

@Configuration
@ConfigurationProperties(prefix = "")
@PropertySource(value = "classpath:config.yml", factory = YamlPropertySourceFactory.class)
public class AasServersConfig {
    private List<AasServer> aasServers;

    public List<AasServer> getAasServers() {
        return aasServers;
    }
    public void setAasServers(List<AasServer> aasServers) {
        this.aasServers = aasServers;
    }

    public static class AasServer {
        private String name;
        private String url;
        private Auth auth;
        private int pollingIntervalS;

        public String getName() { return name; }
        public void setName(String name) { this.name = name; }

        public String getUrl() { return url; }
        public void setUrl(String url) { this.url = url; }

        public Auth getAuth() { return auth; }
        public void setAuth(Auth auth) { this.auth = auth; }

        public int getPollingIntervalS() { return pollingIntervalS; }
        public void setPollingIntervalS(int pollingIntervalS) { this.pollingIntervalS = pollingIntervalS; }

        public static class Auth {
            private String authType;
            private String clientId;
            private String clientSecret;
            private String tokenUrl;
            private String apiKey;
            private String secret;
            private String loginUrl;

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
        }
    }
}

