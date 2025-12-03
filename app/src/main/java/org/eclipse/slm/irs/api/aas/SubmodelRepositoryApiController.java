package org.eclipse.slm.irs.api.aas;

import io.swagger.v3.oas.annotations.Hidden;
import io.swagger.v3.oas.annotations.Operation;
import jakarta.validation.Valid;
import jakarta.validation.constraints.Min;
import org.eclipse.digitaltwin.aas4j.v3.model.OperationRequest;
import org.eclipse.digitaltwin.aas4j.v3.model.OperationResult;
import org.eclipse.digitaltwin.aas4j.v3.model.Submodel;
import org.eclipse.digitaltwin.aas4j.v3.model.SubmodelElement;
import org.eclipse.digitaltwin.basyx.core.pagination.CursorResult;
import org.eclipse.digitaltwin.basyx.core.pagination.PaginationInfo;
import org.eclipse.digitaltwin.basyx.http.Base64UrlEncodedIdentifier;
import org.eclipse.digitaltwin.basyx.http.Base64UrlEncodedIdentifierSize;
import org.eclipse.digitaltwin.basyx.http.pagination.Base64UrlEncodedCursor;
import org.eclipse.digitaltwin.basyx.http.pagination.PagedResult;
import org.eclipse.digitaltwin.basyx.http.pagination.PagedResultPagingMetadata;
import org.eclipse.digitaltwin.basyx.pagination.GetSubmodelElementsResult;
import org.eclipse.digitaltwin.basyx.submodelrepository.http.SubmodelRepositoryHTTPApi;
import org.eclipse.digitaltwin.basyx.submodelrepository.http.pagination.GetSubmodelsResult;
import org.eclipse.digitaltwin.basyx.submodelservice.value.SubmodelElementValue;
import org.eclipse.digitaltwin.basyx.submodelservice.value.SubmodelValueOnly;
import org.eclipse.slm.irs.exceptions.MethodNotSupportedException;
import org.eclipse.slm.irs.services.submodelrepository.SubmodelRepositoryService;
import org.springframework.core.io.Resource;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.multipart.MultipartFile;

import java.util.ArrayList;
import java.util.List;

@RestController()
public class SubmodelRepositoryApiController implements SubmodelRepositoryHTTPApi {

    private final SubmodelRepository repository;

    private final SubmodelRepositoryService submodelRepositoryService;

    public SubmodelRepositoryApiController(SubmodelRepository repository, SubmodelRepositoryService submodelRepositoryService) {
        this.repository = repository;
        this.submodelRepositoryService = submodelRepositoryService;
    }
    
    @Override
    public ResponseEntity<PagedResult> getAllSubmodels(@Base64UrlEncodedIdentifierSize(min = 1, max = 3072) @Valid Base64UrlEncodedIdentifier semanticId, @Valid String idShort, @Min(1L) @Valid Integer limit, @Valid Base64UrlEncodedCursor cursor, @Valid String level, @Valid String extent) {
        if (limit == null) {
            limit = 100;
        }

        String decodedCursor = "";
        if (cursor != null) {
            decodedCursor = cursor.getDecodedCursor();
        }

        PaginationInfo pInfo = new PaginationInfo(limit, decodedCursor);

        CursorResult<List<Submodel>> cursorResult = repository.getAllSubmodels(pInfo);

        GetSubmodelsResult paginatedSubmodel = new GetSubmodelsResult();

        String encodedCursor = getEncodedCursorFromCursorResult(cursorResult);

        paginatedSubmodel.result(new ArrayList<>(cursorResult.getResult()));
        paginatedSubmodel.setPagingMetadata(new PagedResultPagingMetadata().cursor(encodedCursor));

        return new ResponseEntity<PagedResult>(paginatedSubmodel, HttpStatus.OK);
    }

    @Override
    public ResponseEntity<Submodel> getSubmodelById(Base64UrlEncodedIdentifier base64UrlEncodedSubmodelIdentifier, @Valid String level, @Valid String extent) {
        var submodelIdentifier = Base64UrlEncodedIdentifier.fromEncodedValue(base64UrlEncodedSubmodelIdentifier.getIdentifier());

        var aas = submodelRepositoryService.getSubmodelById(submodelIdentifier.getIdentifier());

        return ResponseEntity.ok(aas);
    }

    @Override
    public ResponseEntity<SubmodelValueOnly> getSubmodelByIdValueOnly(Base64UrlEncodedIdentifier submodelIdentifier, @Valid String level, @Valid String extent) {
        return new ResponseEntity<SubmodelValueOnly>(repository.getSubmodelByIdValueOnly(submodelIdentifier.getIdentifier()), HttpStatus.OK);
    }

    @Override
    public ResponseEntity<Submodel> getSubmodelByIdMetadata(Base64UrlEncodedIdentifier submodelIdentifier, @Valid String level) {
        return new ResponseEntity<Submodel>(repository.getSubmodelByIdMetadata(submodelIdentifier.getIdentifier()), HttpStatus.OK);

    }

    @Override
    public ResponseEntity<PagedResult> getAllSubmodelElements(Base64UrlEncodedIdentifier submodelIdentifier, @Min(1L) @Valid Integer limit, @Valid Base64UrlEncodedCursor cursor, @Valid String level, @Valid String extent) {
        if (limit == null) {
            limit = 100;
        }

        String decodedCursor = "";
        if (cursor != null) {
            decodedCursor = cursor.getDecodedCursor();
        }

        PaginationInfo pInfo = new PaginationInfo(limit, decodedCursor);
        CursorResult<List<SubmodelElement>> cursorResult = this.repository.getSubmodelElements(submodelIdentifier.getIdentifier(), pInfo);

        GetSubmodelElementsResult paginatedSubmodelElement = new GetSubmodelElementsResult();
        String encodedCursor = getEncodedCursorFromCursorResult(cursorResult);

        paginatedSubmodelElement.result(new ArrayList<>(cursorResult.getResult()));
        paginatedSubmodelElement.setPagingMetadata(new PagedResultPagingMetadata().cursor(encodedCursor));

        return new ResponseEntity<PagedResult>(paginatedSubmodelElement, HttpStatus.OK);
    }

    @Override
    public ResponseEntity<SubmodelElement> getSubmodelElementByPathSubmodelRepo(Base64UrlEncodedIdentifier submodelIdentifier, String idShortPath, @Valid String level, @Valid String extent) {
        return handleSubmodelElementValueNormalGetRequest(submodelIdentifier.getIdentifier(), idShortPath);
    }

    @Override
    public ResponseEntity<SubmodelElementValue> getSubmodelElementByPathValueOnlySubmodelRepo(Base64UrlEncodedIdentifier submodelIdentifier, String idShortPath, @Valid String level, @Valid String extent) {
        return handleSubmodelElementValueGetRequest(submodelIdentifier.getIdentifier(), idShortPath);
    }


    @Operation(tags = {"Submodel Repository API"})
    @Override
    public ResponseEntity<Resource> getFileByPath(Base64UrlEncodedIdentifier submodelIdentifier, String idShortPath) {
        return null;
    }


    private String getEncodedCursorFromCursorResult(CursorResult<?> cursorResult) {
        if (cursorResult == null || cursorResult.getCursor() == null) {
            return null;
        }

        return Base64UrlEncodedCursor.encodeCursor(cursorResult.getCursor());
    }

    private ResponseEntity<SubmodelElement> handleSubmodelElementValueNormalGetRequest(String submodelIdentifier, String idShortPath) {
        SubmodelElement submodelElement = this.repository.getSubmodelElement(submodelIdentifier, idShortPath);
        return new ResponseEntity<SubmodelElement>(submodelElement, HttpStatus.OK);
    }

    private ResponseEntity<SubmodelElementValue> handleSubmodelElementValueGetRequest(String submodelIdentifier, String idShortPath) {
        SubmodelElementValue value = this.repository.getSubmodelElementValue(submodelIdentifier, idShortPath);
        return new ResponseEntity<SubmodelElementValue>(value, HttpStatus.OK);
    }

    //region Hidden Methods | Not relevant for IRS
    @Hidden
    @Override
    public ResponseEntity<Submodel> postSubmodel(@Valid Submodel body) {
        throw new MethodNotSupportedException();
    }

    @Hidden
    @Override
    public ResponseEntity<Void> putSubmodelById(Base64UrlEncodedIdentifier submodelIdentifier, @Valid Submodel body, @Valid String level) {
        throw new MethodNotSupportedException();

    }

    @Hidden
    @Override
    public ResponseEntity<Void> deleteSubmodelById(Base64UrlEncodedIdentifier submodelIdentifier) {
        throw new MethodNotSupportedException();
    }

    @Hidden
    @Override
    public ResponseEntity<Void> deleteFileByPath(Base64UrlEncodedIdentifier submodelIdentifier, String idShortPath) {
        throw new MethodNotSupportedException();
    }

    @Hidden
    @Override
    public ResponseEntity<OperationResult> invokeOperationSubmodelRepo(Base64UrlEncodedIdentifier submodelIdentifier, String idShortPath, @Valid OperationRequest body, @Valid Boolean async) {
        throw new MethodNotSupportedException();
    }

    @Hidden
    @Override
    public ResponseEntity<Void> putSubmodelElementByPathSubmodelRepo(Base64UrlEncodedIdentifier submodelIdentifier, String idShortPath, @Valid SubmodelElement body, @Valid String level) {
        throw new MethodNotSupportedException();
    }

    @Hidden
    @Override
    public ResponseEntity<Void> patchSubmodelByIdValueOnly(Base64UrlEncodedIdentifier submodelIdentifier, @Valid List<SubmodelElement> body, @Valid String level) {
        throw new MethodNotSupportedException();
    }

    @Hidden
    @Override
    public ResponseEntity<Void> patchSubmodelElementByPathValueOnlySubmodelRepo(Base64UrlEncodedIdentifier submodelIdentifier, String idShortPath, @Valid SubmodelElementValue body, @Valid String level) {
        throw new MethodNotSupportedException();
    }

    @Hidden
    @Override
    public ResponseEntity<SubmodelElement> postSubmodelElementByPathSubmodelRepo(Base64UrlEncodedIdentifier submodelIdentifier, String idShortPath, @Valid SubmodelElement body, @Valid String level, @Valid String extent) {
        throw new MethodNotSupportedException();
    }

    @Hidden
    @Override
    public ResponseEntity<SubmodelElement> postSubmodelElementSubmodelRepo(Base64UrlEncodedIdentifier submodelIdentifier, @Valid SubmodelElement body) {
        throw new MethodNotSupportedException();
    }

    @Hidden
    @Override
    public ResponseEntity<Void> deleteSubmodelElementByPathSubmodelRepo(Base64UrlEncodedIdentifier submodelIdentifier, String idShortPath) {
        throw new MethodNotSupportedException();
    }

    @Hidden
    @Override
    public ResponseEntity<Void> putFileByPath(Base64UrlEncodedIdentifier submodelIdentifier, String idShortPath, String fileName, @Valid MultipartFile file) {
        throw new MethodNotSupportedException();
    }
    //endregion Hidden Methods | Not relevant for IRS
}
