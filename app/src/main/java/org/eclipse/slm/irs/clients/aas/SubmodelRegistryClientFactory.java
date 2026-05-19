package org.eclipse.slm.irs.clients.aas;

import org.eclipse.slm.aas.clients.auth.AuthRequestInterceptor;
import org.eclipse.slm.aas.clients.submodelregistry.SubmodelRegistryClient;
import org.eclipse.slm.irs.config.AasServersConfig;
import org.springframework.stereotype.Component;

@Component
public class SubmodelRegistryClientFactory extends AbstractAasClientFactory<SubmodelRegistryClient> {

    @Override
    protected SubmodelRegistryClient createClient(String url, AuthRequestInterceptor authRequestInterceptor) {
        var client = new SubmodelRegistryClient(url, authRequestInterceptor);
        return client;
    }

}
