package org.eclipse.slm.irs.clients.aas;

import org.eclipse.slm.aas.clients.auth.AuthRequestInterceptor;
import org.eclipse.slm.aas.clients.submodelrepository.SubmodelRepositoryClient;
import org.eclipse.slm.irs.config.AasServersConfig;
import org.springframework.stereotype.Component;

@Component
public class SubmodelRepositoryClientFactory extends AbstractAasClientFactory<SubmodelRepositoryClient> {

    @Override
    protected SubmodelRepositoryClient createClient(String url, AuthRequestInterceptor authRequestInterceptor) {
        var client = new SubmodelRepositoryClient(url, authRequestInterceptor);
        return client;
    }

    @Override
    protected String getUrl(AasServersConfig.AasServer aasServerConfig) {
        return aasServerConfig.getSubmodelRepository();
    }
}
