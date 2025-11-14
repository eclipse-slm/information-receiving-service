package org.eclipse.slm.irs.services.shellrepository;

import org.eclipse.digitaltwin.aas4j.v3.model.AssetAdministrationShell;
import org.eclipse.slm.common.aas.model.shellrepository.exceptions.ShellNotFoundException;
import org.eclipse.slm.irs.clients.aas.shellrepository.ShellRepositoryClientFactory;
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
                LOG.error("Error while getting AAS with id {} from AAS server {}: {}", aasId, aasServerConfig.getUrl(), e.getMessage());
            }
        }

        throw new ShellNotFoundException(aasId);
    }
}
