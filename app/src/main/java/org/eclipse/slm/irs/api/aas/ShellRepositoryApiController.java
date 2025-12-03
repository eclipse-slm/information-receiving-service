package org.eclipse.slm.irs.api.aas;

import io.swagger.v3.oas.annotations.Hidden;
import io.swagger.v3.oas.annotations.tags.Tag;
import jakarta.validation.Valid;
import jakarta.validation.constraints.Min;
import org.eclipse.digitaltwin.aas4j.v3.model.AssetAdministrationShell;
import org.eclipse.digitaltwin.aas4j.v3.model.AssetInformation;
import org.eclipse.digitaltwin.aas4j.v3.model.Reference;
import org.eclipse.digitaltwin.basyx.http.Base64UrlEncodedIdentifier;
import org.eclipse.digitaltwin.basyx.http.pagination.Base64UrlEncodedCursor;
import org.eclipse.digitaltwin.basyx.http.pagination.PagedResult;
import org.eclipse.slm.common.aas.repositories.api.shells.AasRepositoryHTTPApi;
import org.eclipse.slm.irs.exceptions.MethodNotImplementedException;
import org.eclipse.slm.irs.exceptions.MethodNotSupportedException;
import org.eclipse.slm.irs.services.shellrepository.ShellRepositoryService;
import org.springframework.core.io.Resource;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.multipart.MultipartFile;

import java.util.List;

@RestController
@RequestMapping("")
@Tag(name = "Shell Repository API")
public class ShellRepositoryApiController implements AasRepositoryHTTPApi {

    private final ShellRepositoryService shellRepositoryService;

    public ShellRepositoryApiController(ShellRepositoryService shellRepositoryService) {
        this.shellRepositoryService = shellRepositoryService;
    }

    @Override
    public ResponseEntity<PagedResult> getAllAssetAdministrationShells(@Valid List<Base64UrlEncodedIdentifier> list, @Valid String s, @Min(0L) @Valid Integer integer, @Valid Base64UrlEncodedCursor base64UrlEncodedCursor) {
        throw new MethodNotImplementedException();
    }

    @Override
    public ResponseEntity<PagedResult> getAllSubmodelReferencesAasRepository(Base64UrlEncodedIdentifier base64UrlEncodedIdentifier, @Min(0L) @Valid Integer integer, @Valid Base64UrlEncodedCursor base64UrlEncodedCursor) {
        throw new MethodNotImplementedException();
    }

    @Override
    public ResponseEntity<AssetAdministrationShell> getAssetAdministrationShellById(String base64UrlEncodedAasIdentifier) {
        var aasIdentifier = Base64UrlEncodedIdentifier.fromEncodedValue(base64UrlEncodedAasIdentifier);

        var aas = shellRepositoryService.getShellById(aasIdentifier.getIdentifier());

        return ResponseEntity.ok(aas);
    }

    @Override
    public ResponseEntity<AssetInformation> getAssetInformationAasRepository(Base64UrlEncodedIdentifier base64UrlEncodedIdentifier) {
        throw new MethodNotImplementedException();
    }

    //region Hidden Methods | Not relevant for IRS
    @Hidden
    @Override
    public ResponseEntity<AssetAdministrationShell> postAssetAdministrationShell(@Valid AssetAdministrationShell assetAdministrationShell) {
        throw new MethodNotSupportedException();
    }

    @Hidden
    @Override
    public ResponseEntity<Reference> postSubmodelReferenceAasRepository(Base64UrlEncodedIdentifier base64UrlEncodedIdentifier, @Valid Reference reference) {
        throw new MethodNotSupportedException();
    }

    @Hidden
    @Override
    public ResponseEntity<Void> putAssetAdministrationShellById(Base64UrlEncodedIdentifier base64UrlEncodedIdentifier, @Valid AssetAdministrationShell assetAdministrationShell) {
        throw new MethodNotSupportedException();
    }

    @Hidden
    @Override
    public ResponseEntity<Void> putAssetInformationAasRepository(Base64UrlEncodedIdentifier base64UrlEncodedIdentifier, @Valid AssetInformation assetInformation) {
        throw new MethodNotSupportedException();
    }

    @Hidden
    @Override
    public ResponseEntity<Void> deleteAssetAdministrationShellById(Base64UrlEncodedIdentifier base64UrlEncodedIdentifier) {
        throw new MethodNotSupportedException();
    }

    @Hidden
    @Override
    public ResponseEntity<Void> deleteSubmodelReferenceByIdAasRepository(Base64UrlEncodedIdentifier base64UrlEncodedIdentifier, Base64UrlEncodedIdentifier base64UrlEncodedIdentifier1) {
        throw new MethodNotSupportedException();
    }

    @Hidden
    @Override
    public ResponseEntity<Resource> getThumbnailAasRepository(Base64UrlEncodedIdentifier base64UrlEncodedIdentifier) {
        throw new MethodNotSupportedException();
    }

    @Hidden
    @Override
    public ResponseEntity<Void> putThumbnailAasRepository(Base64UrlEncodedIdentifier base64UrlEncodedIdentifier, String s, @Valid MultipartFile multipartFile) {
        throw new MethodNotSupportedException();
    }

    @Hidden
    @Override
    public ResponseEntity<Void> deleteThumbnailAasRepository(Base64UrlEncodedIdentifier base64UrlEncodedIdentifier) {
        throw new MethodNotSupportedException();
    }

    //endregion Hidden Methods | Not relevant for IRS
}
