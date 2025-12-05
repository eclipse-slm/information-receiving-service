package org.eclipse.slm.irs.clients.aas;

import org.eclipse.slm.common.aas.clients.auth.AuthRequestInterceptor;
import org.eclipse.slm.common.aas.clients.shellregistry.AasRegistryClient;
import org.springframework.stereotype.Component;

@Component
public class ShellRegistryClientFactory extends AbstractAasClientFactory<AasRegistryClient> {

    @Override
    protected AasRegistryClient createClient(String url, AuthRequestInterceptor authRequestInterceptor) {
        var client = new AasRegistryClient(url, authRequestInterceptor);
        return client;
    }

}
