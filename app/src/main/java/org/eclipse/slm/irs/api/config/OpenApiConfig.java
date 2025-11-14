package org.eclipse.slm.irs.api.config;

import io.swagger.v3.oas.models.Components;
import io.swagger.v3.oas.models.OpenAPI;
import io.swagger.v3.oas.models.info.Contact;
import io.swagger.v3.oas.models.info.Info;
import io.swagger.v3.oas.models.security.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.util.Arrays;

@Configuration
public class OpenApiConfig {

    private final static Logger LOG = LoggerFactory.getLogger(OpenApiConfig.class);

    static {
        io.swagger.v3.core.jackson.ModelResolver.enumsAsRef = true;
    }

    @Bean
    public OpenAPI openAPI(
            @Value("${open-api.title}") String title,
            @Value("${open-api.description}") String description,
            @Value("${open-api.version}") String version,
            @Value("${open-api.contact.name}") String contactName,
            @Value("${open-api.contact.url}") String contactUrl,
            @Value("${open-api.contact.email}") String contactMail,
            @Value("${jwt.auth.issuer-uri}") String jwtIssuerUri,
            @Value("${security.enabled}") boolean securityEnabled
    ) {
        var authServerUrl = jwtIssuerUri + "/protocol/openid-connect/auth";
        var tokenServerUrl = jwtIssuerUri + "/protocol/openid-connect/token";

        var openApi = new OpenAPI()
                .info(new Info()
                        .title(title)
                        .description(description)
                        .version(version)
                        .contact(new Contact()
                                .name(contactName)
                                .url(contactUrl)
                                .email(contactMail)));

        if (securityEnabled) {
            openApi.components(new Components()
                    .addSecuritySchemes("spring_oauth", new SecurityScheme()
                            .type(SecurityScheme.Type.OAUTH2)
                            .description("Oauth2 flow")
                            .flows(new OAuthFlows()
                                    .authorizationCode(new OAuthFlow()
                                            .authorizationUrl(authServerUrl)
                                            .refreshUrl(tokenServerUrl)
                                            .tokenUrl(tokenServerUrl)
                                            .scopes(new Scopes()))
                            )
                    )
                    .addSecuritySchemes("bearer_auth", new SecurityScheme()
                            .type(SecurityScheme.Type.HTTP)
                            .scheme("bearer")
                            .bearerFormat("JWT")
                    )
            );
            openApi.security(Arrays.asList(
                    new SecurityRequirement().addList("bearer_auth"),
                    new SecurityRequirement().addList("spring_oauth")
            ));

        }

        return openApi;
    }

}
