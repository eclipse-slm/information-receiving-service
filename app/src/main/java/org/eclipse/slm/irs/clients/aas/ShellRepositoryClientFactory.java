package org.eclipse.slm.irs.clients.aas;

import org.eclipse.slm.common.aas.clients.auth.ApiKeyAuthRequestInterceptor;
import org.eclipse.slm.common.aas.clients.auth.BearerTokenAuthRequestInterceptor;
import org.eclipse.slm.common.aas.clients.auth.OAuth2AuthRequestInterceptor;
import org.eclipse.slm.common.aas.clients.shellrepository.AasRepositoryClient;
import org.eclipse.slm.irs.config.AasServersConfig;
import org.springframework.stereotype.Component;

@Component
public class ShellRepositoryClientFactory {

    public AasRepositoryClient create(AasServersConfig.AasServer aasServerConfig) throws Exception {
        var authInterceptor = switch (aasServerConfig.getAuth().getAuthType()) {
            case "oauth2" -> {
                var feignRequestInterceptor = new OAuth2AuthRequestInterceptor(
                    aasServerConfig.getAuth().getTokenUrl(),
                    aasServerConfig.getAuth().getClientId(),
                    aasServerConfig.getAuth().getClientSecret()
                );
                var accessToken = feignRequestInterceptor.fetchToken();
                yield new BearerTokenAuthRequestInterceptor(accessToken);
            }

            case "apikey" -> new ApiKeyAuthRequestInterceptor(aasServerConfig.getAuth().getApiKey());

            default -> throw new IllegalArgumentException("Unsupported auth type: " + aasServerConfig.getAuth().getAuthType());
        };

        var client = new AasRepositoryClient(aasServerConfig.getUrl(), authInterceptor);
        return client;
    }

}
