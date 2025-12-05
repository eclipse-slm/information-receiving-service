package org.eclipse.slm.irs.clients.aas;

import org.eclipse.slm.common.aas.clients.auth.AuthRequestInterceptor;
import org.eclipse.slm.common.aas.clients.submodelrepository.SubmodelRepositoryClient;
import org.springframework.stereotype.Component;

@Component
public class SubmodelRepositoryClientFactory extends AbstractAasClientFactory<SubmodelRepositoryClient> {

    @Override
    protected SubmodelRepositoryClient createClient(String url, AuthRequestInterceptor authRequestInterceptor) {
        var client = new SubmodelRepositoryClient(url, authRequestInterceptor);
        return client;
    }
}
