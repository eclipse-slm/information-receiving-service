package org.eclipse.slm.irs.clients.aas;

import org.eclipse.slm.common.aas.clients.auth.ApiKeyAuthRequestInterceptor;
import org.eclipse.slm.common.aas.clients.auth.OAuth2AuthRequestInterceptor;
import org.eclipse.slm.common.aas.clients.shellregistry.AasRegistryClient;
import org.eclipse.slm.irs.config.AasServersConfig;
import org.springframework.stereotype.Component;

@Component
public class ShellRegistryClientFactory {

    public AasRegistryClient create(AasServersConfig.AasServer aasServerConfig) {
        var authInterceptor = switch (aasServerConfig.getAuth().getAuthType()) {
            case "oauth2" -> new OAuth2AuthRequestInterceptor(
                    aasServerConfig.getAuth().getTokenUrl(),
                    aasServerConfig.getAuth().getClientId(),
                    aasServerConfig.getAuth().getClientSecret()
                );

            case "apikey" -> new ApiKeyAuthRequestInterceptor(aasServerConfig.getAuth().getApiKey());

            default -> throw new IllegalArgumentException("Unsupported auth type: " + aasServerConfig.getAuth().getAuthType());
        };

        var client = new AasRegistryClient(aasServerConfig.getUrl(), authInterceptor);
        return client;
    }

}
