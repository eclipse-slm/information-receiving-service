package org.eclipse.slm.irs.services.discovery;

import org.eclipse.slm.aas.model.discovery.AssetLink;
import org.eclipse.slm.irs.clients.aas.DiscoveryClientFactory;
import org.eclipse.slm.irs.config.AasServersConfig;
import org.eclipse.slm.irs.utils.Base64Util;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Base64;
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
    public List<String> getAllAssetAdministrationShellIdsByAssetId(String assetIdBase64Encoded) {
        var allAasIds = new ArrayList<String>();

        for (var aasServerConfig : aasServersConfig.getAasServers()) {
            var discoveryClient = discoveryClientFactory.create(aasServerConfig);

            try {
                var aasIds = discoveryClient.getAllAssetAdministrationShellIdsByAssetId(assetIdBase64Encoded);
                allAasIds.addAll(Arrays.stream(aasIds).toList());
            } catch (Exception e) {
                var assetId = Base64Util.decodeFromBase64(assetIdBase64Encoded);
                var assetLink = new AssetLink().name("globalAssetId").value(assetId);
                var aasIds = discoveryClient.getAllAssetAdministrationShellIdsByAssetLink(assetLink);
                allAasIds.addAll(Arrays.stream(aasIds).toList());
            }
        }

        return allAasIds;
    }
}
