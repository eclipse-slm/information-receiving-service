package org.eclipse.slm.irs.services.submodelrepository;

import org.eclipse.digitaltwin.aas4j.v3.model.Submodel;
import org.eclipse.slm.common.aas.model.submodelrepository.exceptions.SubmodelNotFoundException;
import org.eclipse.slm.common.aas.model.submodelrepository.responses.SubmodelQueryResult;

import java.util.Map;

public interface SubmodelRepositoryService {

    Submodel getSubmodelById(String submodelId) throws SubmodelNotFoundException;

    SubmodelQueryResult querySubmodels(Integer limit, String base64UrlEncodedCursor, Map<String, Object> query);
}
