package org.eclipse.slm.irs.config;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.PropertyNamingStrategies;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.io.File;
import java.io.IOException;

@Configuration
public class ConfigLoader {

    private final static Logger LOG = LoggerFactory.getLogger(ConfigLoader.class);

    private final String configFilePath;

    public ConfigLoader(@Value("${config.file-path:}")String configFilePath) {
        this.configFilePath = configFilePath;
    }

    @Bean
    public AasServersConfig aasServersConfig() throws IOException {
        String appConfigEnv = System.getenv("APP_CONFIG");
        if (appConfigEnv != null && !appConfigEnv.isEmpty()) {
            // Load config from environment variable 'APP_CONFIG'
            try {
                var jsonMapper = new ObjectMapper();
                jsonMapper.setPropertyNamingStrategy(PropertyNamingStrategies.KEBAB_CASE);
                var appConfig = jsonMapper.readValue(appConfigEnv, AasServersConfig.class);
                return appConfig;
            } catch (Exception e) {
                LOG.error("Failed to parse APP_CONFIG environment variable: " + e.getMessage());
                throw e;
            }
        } else {
            // Load config from YAML
            var yamlMapper = new ObjectMapper(new YAMLFactory());
            yamlMapper.setPropertyNamingStrategy(PropertyNamingStrategies.KEBAB_CASE);

            File externalFile = new File(this.configFilePath);
            if (externalFile.exists()) {
                return yamlMapper.readValue(externalFile, AasServersConfig.class);
            }

            var classpathStream = getClass().getClassLoader().getResourceAsStream(configFilePath.replace("classpath:", ""));
            if (classpathStream != null) {
                return yamlMapper.readValue(classpathStream, AasServersConfig.class);
            }
        }

        throw new RuntimeException("Failed to load config from environment variable 'APP_CONFIG' or file path: " + this.configFilePath);
    }

}
