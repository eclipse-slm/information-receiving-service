package org.eclipse.slm.irs.api.aas;

import io.swagger.v3.oas.annotations.Hidden;
import org.eclipse.digitaltwin.aas4j.v3.model.AssetAdministrationShellDescriptor;
import org.eclipse.digitaltwin.aas4j.v3.model.AssetKind;
import org.eclipse.digitaltwin.aas4j.v3.model.SubmodelDescriptor;
import org.eclipse.slm.aas.model.shellregistry.respones.GetAssetAdministrationShellDescriptorsResult;
import org.eclipse.slm.aas.model.shellregistry.respones.GetSubmodelDescriptorsResult;
import org.eclipse.slm.irs.exceptions.MethodNotImplementedException;
import org.eclipse.slm.irs.exceptions.MethodNotSupportedException;
import org.eclipse.slm.irs.services.shellregistry.ShellRegistryService;
import org.eclipse.slm.irs.utils.Base64Util;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.RestController;

@RestController
public class ShellRegistryApiController implements ShellRegistryApi {

    private final ShellRegistryService shellRegistryService;

    public ShellRegistryApiController(ShellRegistryService shellRegistryService) {
        this.shellRegistryService = shellRegistryService;
    }

    @Override
    public ResponseEntity<GetAssetAdministrationShellDescriptorsResult> getAllAssetAdministrationShellDescriptors(Integer limit, String cursor, AssetKind assetKind, String assetType) {
        throw new MethodNotImplementedException();
    }

    @Override
    public ResponseEntity<GetSubmodelDescriptorsResult> getAllSubmodelDescriptorsThroughSuperpath(String aasIdentifierEncoded, Integer limit, String cursor) {
        throw new MethodNotImplementedException();
    }

    @Override
    public ResponseEntity<AssetAdministrationShellDescriptor> getAssetAdministrationShellDescriptorById(String aasIdentifierEncoded) {
        var aasIdentifier = Base64Util.decodeFromBase64(aasIdentifierEncoded);
        var shellDescriptor = this.shellRegistryService.getShellDescriptorById(aasIdentifier);

        return ResponseEntity.ok(shellDescriptor);
    }

    @Override
    public ResponseEntity<SubmodelDescriptor> getSubmodelDescriptorByIdThroughSuperpath(String aasIdentifierEncoded, String submodelIdentifier) {
        return null;
    }

    //region Hidden Methods | Not relevant for IRS
    @Hidden
    @Override
    public ResponseEntity<Void> deleteAssetAdministrationShellDescriptorById(String aasIdentifierEncoded) {
        throw new MethodNotSupportedException();
    }

    @Hidden
    @Override
    public ResponseEntity<Void> deleteSubmodelDescriptorByIdThroughSuperpath(String aasIdentifierEncoded, String submodelIdentifier) {
        throw new MethodNotSupportedException();
    }

    @Hidden
    @Override
    public ResponseEntity<AssetAdministrationShellDescriptor> postAssetAdministrationShellDescriptor(AssetAdministrationShellDescriptor body) {
        throw new MethodNotSupportedException();
    }

    @Hidden
    @Override
    public ResponseEntity<SubmodelDescriptor> postSubmodelDescriptorThroughSuperpath(String aasIdentifierEncoded, SubmodelDescriptor body) {
        throw new MethodNotSupportedException();

    }

    @Hidden
    @Override
    public ResponseEntity<Void> putAssetAdministrationShellDescriptorById(String aasIdentifierEncoded, AssetAdministrationShellDescriptor body) {
        throw new MethodNotSupportedException();
    }

    @Hidden
    @Override
    public ResponseEntity<Void> putSubmodelDescriptorByIdThroughSuperpath(String aasIdentifierEncoded, String submodelIdentifier, SubmodelDescriptor body) {
        throw new MethodNotSupportedException();
    }
    //endregion Hidden Methods | Not relevant for IRS
}
