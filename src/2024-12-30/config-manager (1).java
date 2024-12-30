package org.example.config;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

public class ConfigManager {
    private static final String DEFAULT_CONFIG_FILE = "application.properties";
    private final Properties properties;
    private final String activeProfile;

    public enum Environment {
        DEV("dev"),
        STAGING("staging"),
        PROD("prod");

        private final String value;

        Environment(String value) {
            this.value = value;
        }

        public String getValue() {
            return value;
        }

        public static Environment fromString(String value) {
            for (Environment env : Environment.values()) {
                if (env.value.equalsIgnoreCase(value)) {
                    return env;
                }
            }
            return DEV; // Default to DEV if not found
        }
    }

    public ConfigManager() {
        this(determineActiveProfile());
    }

    public ConfigManager(String profile) {
        this.properties = new Properties();
        this.activeProfile = profile;
        loadConfigurations();
    }

    public ConfigManager(Environment env) {
        this(env.getValue());
    }

    private static String determineActiveProfile() {
        // Check system property first
        String profile = System.getProperty("app.environment");
        if (profile == null || profile.isEmpty()) {
            // Then check environment variable
            profile = System.getenv("APP_ENVIRONMENT");
        }
        return profile != null ? profile : Environment.DEV.getValue();
    }

    private void loadConfigurations() {
        try {
            // Load default properties first
            loadPropertiesFile(DEFAULT_CONFIG_FILE);

            // Load environment-specific properties
            String envConfigFile = String.format("application-%s.properties", activeProfile);
            loadPropertiesFile(envConfigFile);

            // Load local overrides if they exist (for development)
            if (Environment.fromString(activeProfile) == Environment.DEV) {
                loadPropertiesFile("application-local.properties");
            }

            // Finally, load environment variable overrides
            loadEnvironmentOverrides();

            System.out.println("Loaded configuration for environment: " + activeProfile);
        } catch (Exception e) {
            System.err.println("Error loading configurations: " + e.getMessage());
            throw new RuntimeException("Failed to load configurations", e);
        }
    }

    private void loadPropertiesFile(String filename) {
        try (InputStream input = getClass().getClassLoader().getResourceAsStream(filename)) {
            if (input != null) {
                Properties props = new Properties();
                props.load(input);
                // Merge with existing properties (new properties take precedence)
                this.properties.putAll(props);
                System.out.println("Loaded configuration file: " + filename);
            } else {
                System.out.println("Configuration file not found: " + filename + " (skipping)");
            }
        } catch (IOException e) {
            System.err.println("Error loading " + filename + ": " + e.getMessage());
        }
    }

    private void loadEnvironmentOverrides() {
        // Environment variables take precedence over all other configurations
        mapEnvVariable("KAFKA_BOOTSTRAP_SERVERS", "kafka.bootstrap.servers");
        mapEnvVariable("KAFKA_GROUP_ID", "kafka.group.id");
        mapEnvVariable("KAFKA_SOURCE_TOPIC", "kafka.source.topic");
        mapEnvVariable("KAFKA_DLQ_TOPIC", "kafka.dlq.topic");
        mapEnvVariable("ELASTICSEARCH_HOST", "elasticsearch.host");
        mapEnvVariable("ELASTICSEARCH_PORT", "elasticsearch.port");
        mapEnvVariable("ELASTICSEARCH_SCHEME", "elasticsearch.scheme");
        mapEnvVariable("BATCH_SIZE", "batch.size");
        mapEnvVariable("BATCH_TIMEOUT_MS", "batch.timeout.ms");
        mapEnvVariable("INDEX_PREFIX", "index.prefix");
    }

    private void mapEnvVariable(String envVar, String propertyKey) {
        String value = System.getenv(envVar);
        if (value != null && !value.isEmpty()) {
            properties.setProperty(propertyKey, value);
            System.out.println("Override from environment variable: " + envVar);
        }
    }

    public String getActiveProfile() {
        return activeProfile;
    }

    public boolean isDevEnvironment() {
        return Environment.fromString(activeProfile) == Environment.DEV;
    }

    public boolean isProdEnvironment() {
        return Environment.fromString(activeProfile) == Environment.PROD;
    }

    // ... (keep existing getter methods)
}