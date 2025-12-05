package org.eclipse.slm.irs.services.submodelregistry;

import org.eclipse.digitaltwin.aas4j.v3.model.SubmodelDescriptor;
import org.eclipse.slm.common.aas.model.submodelregistry.exceptions.SubmodelDescriptorNotFoundException;
import org.eclipse.slm.irs.clients.aas.SubmodelRegistryClientFactory;
import org.eclipse.slm.irs.config.AasServersConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

@Component
public class AggregatedSubmodelRegistryService implements SubmodelRegistryService {

    private final static Logger LOG = LoggerFactory.getLogger(AggregatedSubmodelRegistryService.class);

    private final AasServersConfig aasServersConfig;

    private final SubmodelRegistryClientFactory submodelRegistryClientFactory;

    public AggregatedSubmodelRegistryService(AasServersConfig aasServersConfig,
                                             SubmodelRegistryClientFactory submodelRegistryClientFactory) {
        this.aasServersConfig = aasServersConfig;
        this.submodelRegistryClientFactory = submodelRegistryClientFactory;
    }

    @Override
    public SubmodelDescriptor getSubmodelDescriptorById(String submodelId) throws SubmodelDescriptorNotFoundException {
        for (var aasServerConfig : aasServersConfig.getAasServers()) {
            try {
                var registryClient = submodelRegistryClientFactory.create(aasServerConfig);
                var submodelDescriptor = registryClient.getSubmodelDescriptor(submodelId);

                if (submodelDescriptor.isPresent()) {
                    if (submodelDescriptor.get().getId() == null) {
                        continue;
                    }
                    return submodelDescriptor.get();
                }
            }
            catch (Exception e) {
                LOG.error("Error while getting Submodel Descriptor with id {} from AAS server {}: {}", submodelId, aasServerConfig.getUrl(), e.getMessage());
                LOG.error("Stacktrace: ", e);
            }
        }

        throw new SubmodelDescriptorNotFoundException(submodelId);
    }
}
