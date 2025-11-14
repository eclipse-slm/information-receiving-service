package org.eclipse.slm.irs.services.discovery;

import java.util.List;

public interface DiscoveryService {
    List<String> getAllAssetAdministrationShellIdsByAssetId(String assetId);
}
