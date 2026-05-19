package org.eclipse.slm.irs.clients.aas;

import org.eclipse.slm.aas.clients.auth.ApiKeyAuthRequestInterceptor;
import org.eclipse.slm.aas.clients.auth.AuthRequestInterceptor;
import org.eclipse.slm.aas.clients.auth.BasicAuthRequestInterceptor;
import org.eclipse.slm.aas.clients.auth.OAuth2AuthRequestInterceptor;
import org.eclipse.slm.irs.config.AasServersConfig;
import org.springframework.stereotype.Component;

@Component
public abstract class AbstractAasClientFactory<T> {

    protected abstract T createClient(String url, AuthRequestInterceptor authRequestInterceptor);

    protected abstract String getUrl(AasServersConfig.AasServer aasServerConfig);

    public T create(AasServersConfig.AasServer aasServerConfig) {
        AuthRequestInterceptor authRequestInterceptor = null;
        if (aasServerConfig.getAuth() != null) {
            authRequestInterceptor = switch (aasServerConfig.getAuth().getAuthType()) {
                case "basic" -> new BasicAuthRequestInterceptor(
                        aasServerConfig.getAuth().getUsername(),
                        aasServerConfig.getAuth().getPassword()
                );
                case "oauth2" -> new OAuth2AuthRequestInterceptor(
                        aasServerConfig.getAuth().getTokenUrl(),
                        aasServerConfig.getAuth().getClientId(),
                        aasServerConfig.getAuth().getClientSecret()
                );
                case "apikey" -> new ApiKeyAuthRequestInterceptor(aasServerConfig.getAuth().getApiKey());

                default -> throw new IllegalArgumentException("Unsupported auth type: " + aasServerConfig.getAuth().getAuthType());
            };
        }
        return createClient(this.getUrl(aasServerConfig), authRequestInterceptor);
    }
}