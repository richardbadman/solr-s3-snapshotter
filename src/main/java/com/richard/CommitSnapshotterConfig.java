package com.richard;

import java.util.HashMap;
import java.util.Map;

import org.apache.solr.core.SolrConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CommitSnapshotterConfig {

    private static class Property {

        private final boolean required;
        private final Class type;
        private final Object defaultValue;
        private Object value;

        private Property(boolean required, Class type, Object defaultValue) {
            this.required = required;
            this.type = type;
            this.defaultValue = defaultValue;
            this.value = defaultValue;
        }

        public boolean required() {
            return required;
        }

        public Class getType() {
            return type;
        }

        public Object getDefaultValue() {
            return defaultValue;
        }

        public Object getValue() {
            return value;
        }

        public void setValue(Object value, Class type) {
            this.value = value != null ? convert(value, type) : defaultValue;
        }

        private static <T> T convert(Object o, Class<T> t) {
            return t.cast(o);
        }

    }

    private static final Logger log = LoggerFactory.getLogger(CommitSnapshotterConfig.class);
    private static final String PARENT_KEY = "commitSnapshotter";

    private static Map<String, Property> keysAndValues = new HashMap<String, Property>() {{
        put("bucketName", new Property(true, String.class, "bucket-name-not-set"));
        put("accessKey", new Property(true, String.class, "access-key-not-set"));
        put("secretAccessKey", new Property(true, String.class, "secret-access-key-not-set"));
        put("endpoint", new Property(true, String.class, "endpoint-not-set"));
        put("region", new Property(true, String.class, "region-not-set"));
        put("minioEnabled", new Property(false, Boolean.class, false));
    }};

    private SolrConfig config;
    private boolean configValid;

    /*
        <commitSnapshotter>
          <bucketName>a_bucket</bucketName>
          <accessKey>ACCESSK3Y</accessKey>
          <secretAccessKey>S3CR3TACC3SSK3Y</secretAccessKey>
          <endpoint>http://localhost:9000/</endpoint>
          <region>us-east-1</region>
          <minioEnabled>true</minioEnabled>
        </commitSnapshotter>
     */

    // TODO - make static
    public CommitSnapshotterConfig(SolrConfig config) {
        this.config = config;
        configValid = false;
    }

    public CommitSnapshotterConfig init() {
        for( Map.Entry< String, Property> entry : keysAndValues.entrySet() ) {
            String value = config.get(PARENT_KEY + "/" + entry.getKey(), (String)null);
            Class type = entry.getValue().getType();
            if ( value == null && entry.getValue().required() ) {
                configValid = false;
                log.warn("Config not set for key {}, uploads disabled", entry.getKey());
                break;
            }
            if ( value == null && !entry.getValue().required ) {
                log.debug("{} not defined, using default value of ({}) instead.", entry.getKey(), entry.getValue().getDefaultValue());
                continue;
            }
            entry.getValue().setValue(value, type);
            configValid = true;
        }
        return this;
    }

    public boolean isConfigValid() {
        return configValid;
    }

    public String getAccessKey() {
        return (String) keysAndValues.get("accessKey").getValue();
    }

    public String getSecretAccessKey() {
        return (String) keysAndValues.get("secretAccessKey").getValue();
    }

    public String getBucketName() {
        return (String) keysAndValues.get("bucketName").getValue();
    }

    public String getEndpoint() {
        return (String) keysAndValues.get("endpoint").getValue();
    }

    public String getRegion() {
        return (String) keysAndValues.get("region").getValue();
    }

    public boolean minioEnabled() {
        return (boolean) keysAndValues.get("minioEnabled").getValue();
    }

}
