package org.eclipse.slm.irs.api.aas;

import jakarta.validation.Valid;
import jakarta.validation.constraints.Min;
import org.eclipse.digitaltwin.aas4j.v3.model.SubmodelDescriptor;
import org.eclipse.slm.aas.model.submodelregistry.respones.GetSubmodelDescriptorsResult;
import org.eclipse.slm.aas.repositories.api.submodels.SubmodelRegistryHTTPApi;
import org.eclipse.slm.irs.exceptions.MethodNotImplementedException;
import org.eclipse.slm.irs.services.submodelregistry.SubmodelRegistryService;
import org.eclipse.slm.irs.utils.Base64Util;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.RestController;

@RestController
public class SubmodelRegistryApiController implements SubmodelRegistryHTTPApi {

    private final SubmodelRegistryService submodelRegistryService;

    public SubmodelRegistryApiController(SubmodelRegistryService submodelRegistryService) {
        this.submodelRegistryService = submodelRegistryService;
    }

    @Override
    public ResponseEntity<GetSubmodelDescriptorsResult> getAllSubmodelDescriptors(@Min(1L) @Valid Integer integer, @Valid String s) {
        throw new MethodNotImplementedException();
    }

    @Override
    public ResponseEntity<SubmodelDescriptor> getSubmodelDescriptorById(String submodelIdBase64Encoded) {
        var submodelId = Base64Util.decodeFromBase64(submodelIdBase64Encoded);
        var submodelDescriptor = this.submodelRegistryService.getSubmodelDescriptorById(submodelId);

        return ResponseEntity.ok(submodelDescriptor);
    }

}
