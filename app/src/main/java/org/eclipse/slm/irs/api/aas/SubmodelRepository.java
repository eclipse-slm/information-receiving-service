package org.eclipse.slm.irs.api.aas;


import org.eclipse.digitaltwin.aas4j.v3.model.OperationVariable;
import org.eclipse.digitaltwin.aas4j.v3.model.Property;
import org.eclipse.digitaltwin.aas4j.v3.model.Submodel;
import org.eclipse.digitaltwin.aas4j.v3.model.SubmodelElement;
import org.eclipse.digitaltwin.basyx.core.exceptions.*;
import org.eclipse.digitaltwin.basyx.core.pagination.CursorResult;
import org.eclipse.digitaltwin.basyx.core.pagination.PaginationInfo;
import org.eclipse.digitaltwin.basyx.core.pagination.PaginationSupport;
import org.eclipse.digitaltwin.basyx.submodelservice.pathparsing.HierarchicalSubmodelElementParser;
import org.eclipse.digitaltwin.basyx.submodelservice.value.PropertyValue;
import org.eclipse.digitaltwin.basyx.submodelservice.value.SubmodelElementValue;
import org.eclipse.digitaltwin.basyx.submodelservice.value.SubmodelValueOnly;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.io.File;
import java.io.InputStream;
import java.util.List;
import java.util.TreeMap;
import java.util.stream.Collectors;

@Component
public class SubmodelRepository implements org.eclipse.digitaltwin.basyx.submodelrepository.SubmodelRepository {
    private final static Logger LOG = LoggerFactory.getLogger(SubmodelRepository.class);

    public SubmodelRepository() {
    }

    @Override
    public CursorResult<List<Submodel>> getAllSubmodels(PaginationInfo pInfo) {
        return null;
    }

    @Override
    public CursorResult<List<Submodel>> getAllSubmodels(String semanticId, PaginationInfo pInfo) {
        return null;
    }

    @Override
    public Submodel getSubmodel(String submodelId) throws ElementDoesNotExistException {
        return null;
//        var modelIdEncoded = Base64UrlEncodedIdentifier.fromEncodedValue(submodelId);
//        var datasource = this.datasourceService.getDatasourceForSubmodelId(modelIdEncoded.getIdentifier());
//        if (datasource.isEmpty()) {
//            throw new SubmodelNotFoundException(submodelId);
//        }
//        try {
//            var source = datasource.get().getSubmodelById(modelIdEncoded.getIdentifier());
//            return source.orElse(null);
//        } catch (IOException e) {
//            throw new RuntimeException(e);
//        }
    }

    @Override
    public void updateSubmodel(String submodelId, Submodel submodel) throws ElementDoesNotExistException {

    }

    @Override
    public void createSubmodel(Submodel submodel) throws CollidingIdentifierException, MissingIdentifierException {

    }

    @Override
    public void updateSubmodelElement(String submodelIdentifier, String idShortPath, SubmodelElement submodelElement) throws ElementDoesNotExistException {

    }

    @Override
    public void deleteSubmodel(String submodelId) throws ElementDoesNotExistException {

    }

    @Override
    public CursorResult<List<SubmodelElement>> getSubmodelElements(String submodelId, PaginationInfo pInfo) throws ElementDoesNotExistException {

        var submodel = this.getSubmodel(submodelId);

        TreeMap<String, SubmodelElement> submodelMap = submodel.getSubmodelElements().stream()
                .collect(Collectors.toMap(SubmodelElement::getIdShort, submodelElement -> submodelElement, (a, b) -> a, TreeMap::new));
        PaginationSupport<SubmodelElement> paginationSupport = new PaginationSupport<>(submodelMap, SubmodelElement::getIdShort);

        return paginationSupport.getPaged(pInfo);
    }

    @Override
    public SubmodelElement getSubmodelElement(String submodelId, String smeIdShort) throws ElementDoesNotExistException {

        var submodel = this.getSubmodel(submodelId);
        var parser = new HierarchicalSubmodelElementParser(submodel);

        return parser.getSubmodelElementFromIdShortPath(smeIdShort);
    }

    @Override
    public SubmodelElementValue getSubmodelElementValue(String submodelId, String smeIdShort) throws ElementDoesNotExistException {
        var submodel = this.getSubmodel(submodelId);
        var submodelElement = submodel.getSubmodelElements().stream()
                .filter(smElement -> smElement.getIdShort().equals(smeIdShort)).findFirst();

        return submodelElement.map(element -> new PropertyValue(((Property) element).getValue())).orElse(null);

    }

    @Override
    public void setSubmodelElementValue(String submodelId, String smeIdShort, SubmodelElementValue value) throws ElementDoesNotExistException {

    }

    @Override
    public void createSubmodelElement(String submodelId, SubmodelElement smElement) {

    }

    @Override
    public void createSubmodelElement(String submodelId, String idShortPath, SubmodelElement smElement) throws ElementDoesNotExistException {

    }

    @Override
    public void deleteSubmodelElement(String submodelId, String idShortPath) throws ElementDoesNotExistException {

    }

    @Override
    public String getName() {
        return org.eclipse.digitaltwin.basyx.submodelrepository.SubmodelRepository.super.getName();
    }

    @Override
    public OperationVariable[] invokeOperation(String submodelId, String idShortPath, OperationVariable[] input) throws ElementDoesNotExistException {
        return new OperationVariable[0];
    }

    @Override
    public SubmodelValueOnly getSubmodelByIdValueOnly(String submodelId) throws ElementDoesNotExistException {
        return null;
    }

    @Override
    public Submodel getSubmodelByIdMetadata(String submodelId) throws ElementDoesNotExistException {
        return null;
    }

    @Override
    public File getFileByPathSubmodel(String submodelId, String idShortPath) throws ElementDoesNotExistException, ElementNotAFileException, FileDoesNotExistException {
        return null;
    }

    @Override
    public void setFileValue(String submodelId, String idShortPath, String fileName, String contentType, InputStream inputStream) throws ElementDoesNotExistException, ElementNotAFileException {

    }

    @Override
    public void deleteFileValue(String submodelId, String idShortPath) throws ElementDoesNotExistException, ElementNotAFileException, FileDoesNotExistException {

    }

    @Override
    public void patchSubmodelElements(String submodelId, List<SubmodelElement> submodelElementList) {

    }

    @Override
    public InputStream getFileByFilePath(String submodelId, String filePath) {
        return null;
    }
}
