package org.eclipse.slm.irs.api.aas;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.eclipse.slm.aas.model.submodelrepository.responses.SubmodelQueryResult;
import org.eclipse.slm.aas.repositories.api.submodels.SubmodelRepositoryQueryApi;
import org.eclipse.slm.irs.services.submodelrepository.SubmodelRepositoryService;
import org.springframework.web.bind.annotation.RestController;

import java.util.Map;

@RestController
public class SubmodelRepositoryQueryApiController implements SubmodelRepositoryQueryApi {

    private final SubmodelRepositoryService submodelRepositoryService;

    public SubmodelRepositoryQueryApiController(SubmodelRepositoryService submodelRepositoryService) {
        this.submodelRepositoryService = submodelRepositoryService;
    }

    @Override
    public SubmodelQueryResult querySubmodels(Integer limit, String base64UrlEncodedCursor, String query) {
        var mapper = new ObjectMapper();
        try {
            var queryMap = mapper.readValue(query, new TypeReference<Map<String, Object>>() {});

            var result = this.submodelRepositoryService.querySubmodels(limit, base64UrlEncodedCursor, queryMap);
            return result;
        } catch (JsonProcessingException e) {
            throw new RuntimeException(e);
        }
    }

}
