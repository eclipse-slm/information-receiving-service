package org.eclipse.slm.irs.services.discovery;

import org.eclipse.slm.aas.clients.base.FeignResponseException;
import org.eclipse.slm.aas.model.discovery.AssetLink;
import org.eclipse.slm.aas.model.discovery.expcetions.DiscoveryClientRuntimeException;
import org.eclipse.slm.irs.clients.aas.DiscoveryClientFactory;
import org.eclipse.slm.irs.config.AasServersConfig;
import org.eclipse.slm.irs.config.ConfigLoader;
import org.eclipse.slm.irs.utils.Base64Util;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Base64;
import java.util.List;

@Component
public class AggregatedDiscoveryService implements DiscoveryService {

    private final static Logger LOG = LoggerFactory.getLogger(AggregatedDiscoveryService.class);

    private final AasServersConfig aasServersConfig;

    private final DiscoveryClientFactory discoveryClientFactory;

    public AggregatedDiscoveryService(AasServersConfig aasServersConfig, DiscoveryClientFactory discoveryClientFactory) {
        this.aasServersConfig = aasServersConfig;
        this.discoveryClientFactory = discoveryClientFactory;
    }

    @Override
    public List<String> getAllAssetAdministrationShellIdsByAssetId(String assetIdBase64Encoded) {
        var allAasIds = new ArrayList<String>();

        for (var aasServerConfig : aasServersConfig.getAasServers()) {
            var discoveryClient = discoveryClientFactory.create(aasServerConfig);

            try {
                var aasIds = discoveryClient.getAllAssetAdministrationShellIdsByAssetId(assetIdBase64Encoded);
                allAasIds.addAll(Arrays.stream(aasIds).toList());
            }
            catch (Exception e) {
                if (e instanceof FeignResponseException) {
                    var fe = (FeignResponseException) e;
                    if (fe.getStatusCode() == 401) {
                        LOG.error("Failed to authenticate at AAS server '{}'", aasServerConfig.getName());
                    } else {
                        LOG.error("Failed to get AAS IDs for asset ID '{}' from AAS server '{}': {}", assetIdBase64Encoded, aasServerConfig.getName(), fe.getMessage(), fe);
                    }
                }
                else if (e.getMessage().contains("timed out")) {
                    LOG.error("Connection to AAS server '{}' timed out", aasServerConfig.getName());
                }
                else {
                    try {
                        var assetId = Base64Util.decodeFromBase64(assetIdBase64Encoded);
                        var assetLink = new AssetLink().name("globalAssetId").value(assetId);
                        var aasIds = discoveryClient.getAllAssetAdministrationShellIdsByAssetLink(assetLink);
                        allAasIds.addAll(Arrays.stream(aasIds).toList());
                    } catch (Exception e2) {
                        LOG.error("Failed to get AAS IDs for asset ID '{}' from AAS server '{}': {}", assetIdBase64Encoded, aasServerConfig.getName(), e2.getMessage(), e2);
                    }
                }
            }
        }

        return allAasIds;
    }
}
