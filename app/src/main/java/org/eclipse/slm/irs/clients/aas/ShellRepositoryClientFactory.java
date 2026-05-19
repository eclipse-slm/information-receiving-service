package org.eclipse.slm.irs.clients.aas;

import org.eclipse.slm.aas.clients.auth.AuthRequestInterceptor;
import org.eclipse.slm.aas.clients.shellrepository.AasRepositoryClient;
import org.eclipse.slm.irs.config.AasServersConfig;
import org.springframework.stereotype.Component;

@Component
public class ShellRepositoryClientFactory extends AbstractAasClientFactory<AasRepositoryClient> {

    @Override
    protected AasRepositoryClient createClient(String url, AuthRequestInterceptor authRequestInterceptor) {
        var client = new AasRepositoryClient(url, authRequestInterceptor);
        return client;
    }

}
