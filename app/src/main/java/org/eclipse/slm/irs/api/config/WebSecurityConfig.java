package org.eclipse.slm.irs.api.config;

import org.apache.commons.lang3.ArrayUtils;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.autoconfigure.web.ServerProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpStatus;
import org.springframework.security.config.annotation.method.configuration.EnableMethodSecurity;
import org.springframework.security.config.annotation.web.builders.HttpSecurity;
import org.springframework.security.config.annotation.web.configuration.EnableWebSecurity;
import org.springframework.security.config.annotation.web.configurers.AbstractHttpConfigurer;
import org.springframework.security.config.annotation.web.configurers.HeadersConfigurer;
import org.springframework.security.config.http.SessionCreationPolicy;
import org.springframework.security.oauth2.jwt.JwtDecoder;
import org.springframework.security.oauth2.jwt.JwtDecoders;
import org.springframework.security.web.SecurityFilterChain;
import org.springframework.security.web.util.matcher.AntPathRequestMatcher;
import org.springframework.web.cors.CorsConfiguration;
import org.springframework.web.cors.UrlBasedCorsConfigurationSource;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Stream;

@EnableWebSecurity
@EnableMethodSecurity
@Configuration
public class WebSecurityConfig {

    private static final String[] STATIC_AUTH_WHITELIST = {
            // -- Swagger UI v3 (OpenAPI)
            "/v3/api-docs/**",
            "/swagger-ui.html",
            "/swagger-ui/**",
            "/actuator/health"
    };

    private final JwtConverter jwtConverter;

    public WebSecurityConfig(JwtConverter jwtConverter) {
        this.jwtConverter = jwtConverter;
    }

    @Bean
    SecurityFilterChain filterChain(
            HttpSecurity http,
            ServerProperties serverProperties,
            @Value("${security.enabled:true}") boolean securityEnabled,
            @Value("${security.origins:[]}") String[] origins,
            @Value("${security.auth-white-list:[]}") String[] configuredAuthWhiteList)
            throws Exception {

        if (securityEnabled) {
            // Configure authorization
            var authWhiteList = (String[]) ArrayUtils.addAll(configuredAuthWhiteList, STATIC_AUTH_WHITELIST);
            http.authorizeHttpRequests(requests -> requests
                    .requestMatchers(Stream.of(authWhiteList).map(AntPathRequestMatcher::new).toArray(AntPathRequestMatcher[]::new)).permitAll()
                    .anyRequest().authenticated());
            http.oauth2ResourceServer(oauth2 -> oauth2.jwt(jwt -> jwt.jwtAuthenticationConverter(jwtConverter)));
            // State-less session (state in access-token only)
            http.sessionManagement(sess -> sess.sessionCreationPolicy(
                    SessionCreationPolicy.STATELESS));
            // Disable CSRF because of state-less session-management
            http.csrf(AbstractHttpConfigurer::disable);
            // Disable 'X-Frame-Options' response header
            http.headers((headers) -> headers.frameOptions(HeadersConfigurer.FrameOptionsConfig::disable));
            // Return 401 (unauthorized) instead of 302 (redirect to login) when
            // authorization is missing or invalid
            http.exceptionHandling(eh -> eh.authenticationEntryPoint((request, response, authException) -> {
                response.addHeader(HttpHeaders.WWW_AUTHENTICATE, "Bearer realm=\"Restricted Content\"");
                response.sendError(HttpStatus.UNAUTHORIZED.value(), HttpStatus.UNAUTHORIZED.getReasonPhrase());
            }));
            // Enable and configure CORS
            http.cors(cors -> cors.configurationSource(corsConfigurationSource(origins)));
            // If SSL enabled, disable http (https only)
            if (serverProperties.getSsl() != null && serverProperties.getSsl().isEnabled()) {
                http.requiresChannel(channel -> channel.anyRequest().requiresSecure());
            }
        }
        else {
            // Enable and configure CORS
            String[] unsecure_origins = {"*"};
            http.cors(cors -> cors.configurationSource(corsConfigurationSource(unsecure_origins)));

            // Disable CSRF because of state-less session-management
            http.csrf(csrf -> csrf.disable());

            http.authorizeHttpRequests(r -> r.anyRequest().permitAll());
        }

        return http.build();
    }

    @Bean
    @ConditionalOnProperty(name = "security.enabled", havingValue = "true")
    public JwtDecoder jwtDecoder(@Value("${jwt.auth.issuer-uri}") String jwtIssuerUri) {
        return JwtDecoders.fromIssuerLocation(jwtIssuerUri);
    }

    private UrlBasedCorsConfigurationSource corsConfigurationSource(String[] origins) {
        final var configuration = new CorsConfiguration();
        configuration.setAllowedOrigins(Arrays.asList(origins));
        configuration.setAllowedMethods(List.of("*"));
        configuration.setAllowedHeaders(List.of("*"));
        configuration.setExposedHeaders(List.of("*"));

        final var source = new UrlBasedCorsConfigurationSource();
        source.registerCorsConfiguration("/**", configuration);
        return source;
    }
}
