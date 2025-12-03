package org.eclipse.slm.irs.api.aas;

import org.eclipse.slm.common.aas.repositories.submodels.SubmodelRepositoryRestControllerExceptionHandler;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class SubmodelRepositoryApiConfig {

    @Bean
    public SubmodelRepositoryRestControllerExceptionHandler submodelRepositoryRestControllerExceptionHandler() {
        return new SubmodelRepositoryRestControllerExceptionHandler();
    }

}
