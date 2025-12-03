package org.eclipse.slm.irs.services.submodelrepository;

import org.eclipse.digitaltwin.aas4j.v3.model.Submodel;
import org.eclipse.slm.common.aas.clients.base.FeignResponseException;
import org.eclipse.slm.common.aas.model.shellrepository.exceptions.ShellNotFoundException;
import org.eclipse.slm.common.aas.model.submodelrepository.exceptions.SubmodelNotFoundException;
import org.eclipse.slm.common.aas.model.submodelrepository.responses.SubmodelQueryResult;
import org.eclipse.slm.irs.clients.aas.SubmodelRepositoryClientFactory;
import org.eclipse.slm.irs.config.AasServersConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.util.Map;

@Component
public class AggregatedSubmodelRepositoryService implements SubmodelRepositoryService {

    private final static Logger LOG = LoggerFactory.getLogger(AggregatedSubmodelRepositoryService.class);

    private final AasServersConfig aasServersConfig;

    private final SubmodelRepositoryClientFactory submodelRepositoryClientFactory;

    public AggregatedSubmodelRepositoryService(AasServersConfig aasServersConfig,
                                               SubmodelRepositoryClientFactory submodelRepositoryClientFactory) {
        this.aasServersConfig = aasServersConfig;
        this.submodelRepositoryClientFactory = submodelRepositoryClientFactory;
    }

    @Override
    public Submodel getSubmodelById(String submodelId) throws ShellNotFoundException {
        for (var aasServerConfig : aasServersConfig.getAasServers()) {
            try {
                var submodelRepositoryClient = submodelRepositoryClientFactory.create(aasServerConfig);
                var submodel = submodelRepositoryClient.getSubmodel(submodelId);

                if (submodel.isPresent()) {
                    if (submodel.get().getId() == null) {
                        continue;
                    }
                    return submodel.get();
                }
            }
            catch (Exception e) {
                LOG.error("Error while getting Submodel with id {} from AAS server {}: {}", submodelId, aasServerConfig.getUrl(), e.getMessage());
            }
        }

        throw SubmodelNotFoundException.forSubmodelId(submodelId);
    }

    @Override
    public SubmodelQueryResult querySubmodels(Integer limit, String base64UrlEncodedCursor, Map<String, Object> query) {
        for (var aasServerConfig : aasServersConfig.getAasServers()) {
            try {
                var submodelRepositoryClient = submodelRepositoryClientFactory.create(aasServerConfig);
                var result = submodelRepositoryClient.querySubmodel(limit, base64UrlEncodedCursor, query);

                return result;
            }
            catch (Exception e) {
                if (e instanceof FeignResponseException feignResponseException) {
                    if (feignResponseException.getStatusCode() == 400) {
                        LOG.debug("Bad request while querying submodels from AAS server, query probably not supported {}: {}", aasServerConfig.getUrl(), e.getMessage());
                        continue;
                    }
                }
                LOG.error("Error while querying submodels from AAS server {}: {}", aasServerConfig.getUrl(), e.getMessage());
            }
        }

        throw new SubmodelNotFoundException("Queried submodels could not be found!");
    }
}
