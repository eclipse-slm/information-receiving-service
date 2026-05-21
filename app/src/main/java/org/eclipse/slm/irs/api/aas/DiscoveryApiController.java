package org.eclipse.slm.irs.api.aas;

import io.swagger.v3.oas.annotations.Hidden;
import org.eclipse.digitaltwin.aas4j.v3.model.SpecificAssetId;
import org.eclipse.slm.aas.model.discovery.AssetLink;
import org.eclipse.slm.aas.model.discovery.respones.GetAasIdsByAssetLinkResults;
import org.eclipse.slm.irs.exceptions.MethodNotImplementedException;
import org.eclipse.slm.irs.exceptions.MethodNotSupportedException;
import org.eclipse.slm.irs.services.discovery.DiscoveryService;
import org.eclipse.slm.irs.utils.Base64Util;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.server.ResponseStatusException;

import java.util.ArrayList;
import java.util.List;

@RestController
public class DiscoveryApiController implements LookupApi {

    private final DiscoveryService discoveryService;

    public DiscoveryApiController(DiscoveryService discoveryService) {
        this.discoveryService = discoveryService;
    }

    @Override
    public ResponseEntity<GetAasIdsByAssetLinkResults> getAllAssetAdministrationShellIdsByAssetLink(List<String> assetIds, Integer limit, String cursor) {
        validateAssetIds(assetIds);

        var allAasIds = new ArrayList<String>();

        for (var assetId : assetIds) {
            var aasIds = discoveryService.getAllAssetAdministrationShellIdsByAssetId(assetId);
            allAasIds.addAll(aasIds);
        }

        var response = new GetAasIdsByAssetLinkResults();
        response.setResult(allAasIds);
        return ResponseEntity.ok(response);
    }

    private void validateAssetIds(List<String> assetIds) {
        if (assetIds == null) {
            throw new ResponseStatusException(HttpStatus.BAD_REQUEST, "Query parameter 'assetIds' must not be null.");
        }

        for (String assetId : assetIds) {
            if (assetId == null || assetId.isBlank()) {
                throw new ResponseStatusException(HttpStatus.BAD_REQUEST, "Each 'assetId' value must be non-empty and Base64 encoded.");
            }
            try {
                Base64Util.decodeFromBase64(assetId);
            } catch (IllegalArgumentException ex) {
                throw new ResponseStatusException(HttpStatus.BAD_REQUEST, "Each 'assetId' value must be Base64 encoded.");
            }
        }
    }

    @Override
    public ResponseEntity<GetAasIdsByAssetLinkResults> searchAllAssetAdministrationShellIdsByAssetLink(Integer limit, String cursor, List<AssetLink> assetLinks) {
        throw new MethodNotImplementedException();
    }


    //region Hidden Methods | Not relevant for IRS
    @Hidden
    @Override
    public ResponseEntity<List<SpecificAssetId>> getAllAssetLinksById(String aasIdentifier) {
        throw new MethodNotSupportedException();
    }

    @Hidden
    @Override
    public ResponseEntity<Void> deleteAllAssetLinksById(String aasIdentifier) {
        throw new MethodNotSupportedException();
    }

    @Hidden
    @Override
    public ResponseEntity<List<SpecificAssetId>> postAllAssetLinksById(String aasIdentifier, List<SpecificAssetId> body) {
        throw new MethodNotSupportedException();
    }

    //endregion Hidden Methods | Not relevant for IRS
}
