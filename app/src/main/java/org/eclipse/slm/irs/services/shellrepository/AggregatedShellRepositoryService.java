package org.eclipse.slm.irs.services.shellrepository;

import org.eclipse.digitaltwin.aas4j.v3.model.AssetAdministrationShell;
import org.eclipse.slm.aas.clients.base.FeignResponseException;
import org.eclipse.slm.aas.model.shellrepository.exceptions.ShellNotFoundException;
import org.eclipse.slm.irs.clients.aas.ShellRepositoryClientFactory;
import org.eclipse.slm.irs.config.AasServersConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

@Component
public class AggregatedShellRepositoryService implements ShellRepositoryService {

    private final static Logger LOG = LoggerFactory.getLogger(AggregatedShellRepositoryService.class);


    private final AasServersConfig aasServersConfig;

    private final ShellRepositoryClientFactory shellRepositoryClientFactory;

    public AggregatedShellRepositoryService(AasServersConfig aasServersConfig,
                                            ShellRepositoryClientFactory shellRepositoryClientFactory) {
        this.aasServersConfig = aasServersConfig;
        this.shellRepositoryClientFactory = shellRepositoryClientFactory;
    }

    @Override
    public AssetAdministrationShell getShellById(String aasId) throws ShellNotFoundException {

        for (var aasServerConfig : aasServersConfig.getAasServers()) {
            try {
                var shellRepositoryClient = shellRepositoryClientFactory.create(aasServerConfig);
                var aas = shellRepositoryClient.getAas(aasId);

                if (aas.isPresent()) {
                    if (aas.get().getId() == null) {
                        continue;
                    }
                    return aas.get();
                }
            }
            catch (Exception e) {
                if (e instanceof FeignResponseException) {
                    var fe = (FeignResponseException) e;
                    if (fe.getStatusCode() == 401) {
                        LOG.error("Failed to authenticate at AAS server '{}'", aasServerConfig.getName());
                    } else {
                        LOG.error("Failed to get AAS with id '{}' from AAS server '{}': {}", aasId, aasServerConfig.getName(), fe.getMessage(), fe);
                    }
                }
                else if (e.getMessage() != null && e.getMessage().contains("timed out")) {
                    LOG.error("Connection to AAS server '{}' timed out", aasServerConfig.getName());
                }
                else {
                    LOG.error("Failed to get AAS with id '{}' from AAS server '{}': {}", aasId, aasServerConfig.getName(), e.getMessage(), e);
                }
            }
        }

        throw new ShellNotFoundException(aasId);
    }
}
