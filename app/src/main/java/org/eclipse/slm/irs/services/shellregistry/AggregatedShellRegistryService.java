package org.eclipse.slm.irs.services.shellregistry;

import org.eclipse.digitaltwin.aas4j.v3.model.AssetAdministrationShellDescriptor;
import org.eclipse.slm.common.aas.model.shellregistry.exceptions.ShellDescriptorNotFoundException;
import org.eclipse.slm.irs.clients.aas.shellregistry.ShellRegistryClientFactory;
import org.eclipse.slm.irs.config.AasServersConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

@Component
public class AggregatedShellRegistryService implements ShellRegistryService {

    private final static Logger LOG = LoggerFactory.getLogger(AggregatedShellRegistryService.class);

    private final AasServersConfig aasServersConfig;

    private final ShellRegistryClientFactory shellRegistryClientFactory;

    public AggregatedShellRegistryService(AasServersConfig aasServersConfig,
                                          ShellRegistryClientFactory shellRegistryClientFactory) {
        this.aasServersConfig = aasServersConfig;
        this.shellRegistryClientFactory = shellRegistryClientFactory;
    }

    @Override
    public AssetAdministrationShellDescriptor getShellDescriptorById(String aasId) throws ShellDescriptorNotFoundException {
        for (var aasServerConfig : aasServersConfig.getAasServers()) {
            try {
                var shellRegistryClient = shellRegistryClientFactory.create(aasServerConfig);
                var aasDescriptor = shellRegistryClient.getAasDescriptor(aasId);

                if (aasDescriptor.isPresent()) {
                    if (aasDescriptor.get().getId() == null) {
                        continue;
                    }
                    return aasDescriptor.get();
                }
            }
            catch (Exception e) {
                LOG.error("Error while getting AAS Descriptor with id {} from AAS server {}: {}", aasId, aasServerConfig.getUrl(), e.getMessage());
                LOG.error("Stacktrace: ", e);
            }
        }

        throw new ShellDescriptorNotFoundException(aasId);
    }
}
