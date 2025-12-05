package org.eclipse.slm.irs.clients.aas;

import org.eclipse.slm.common.aas.clients.auth.AuthRequestInterceptor;
import org.eclipse.slm.common.aas.clients.discovery.DiscoveryClient;
import org.springframework.stereotype.Component;

@Component
public class DiscoveryClientFactory extends AbstractAasClientFactory<DiscoveryClient> {

    @Override
    protected DiscoveryClient createClient(String url, AuthRequestInterceptor authRequestInterceptor) {
        var client = new DiscoveryClient(url, authRequestInterceptor);
        return client;
    }

}
