package org.eclipse.slm.irs.services.discovery;

import org.eclipse.slm.aas.model.discovery.AssetLink;
import org.eclipse.slm.irs.clients.aas.DiscoveryClientFactory;
import org.eclipse.slm.irs.config.AasServersConfig;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

@Component
public class AggregatedDiscoveryService implements DiscoveryService {

    private final AasServersConfig aasServersConfig;

    private final DiscoveryClientFactory discoveryClientFactory;

    public AggregatedDiscoveryService(AasServersConfig aasServersConfig, DiscoveryClientFactory discoveryClientFactory) {
        this.aasServersConfig = aasServersConfig;
        this.discoveryClientFactory = discoveryClientFactory;
    }

    @Override
    public List<String> getAllAssetAdministrationShellIdsByAssetId(String assetId) {
        var allAasIds = new ArrayList<String>();

        for (var aasServerConfig : aasServersConfig.getAasServers()) {
            var discoveryClient = discoveryClientFactory.create(aasServerConfig);

            var aasIds = discoveryClient.getAllAssetAdministrationShellIdsByAssetId(assetId);

            allAasIds.addAll(Arrays.stream(aasIds).toList());
        }

        return allAasIds;
    }
}
