package com.richard;

import java.util.HashMap;
import java.util.Map;

import org.apache.solr.core.SolrConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CommitSnapshotterConfig {

    private static final Logger log = LoggerFactory.getLogger(CommitSnapshotterConfig.class);
    private static final String PARENT_KEY = "commitSnapshotter";

    private static Map<String, String> keysAndValues = new HashMap<String, String>() {{
        put("bucketName", "bucket-name-not-set");
        put("accessKey", "access-key-not-set");
        put("secretAccessKey", "secret-access-key-not-set");
        put("endpoint", "endpoint-not-set");
        put("region", "region-not-set");
        put("minioEnabled", "false");
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

    // TODO - make mini class for options, so you can have required fields (or ENUM)
    // TODO - make static
    public CommitSnapshotterConfig(SolrConfig config) {
        this.config = config;
        configValid = false;
    }

    public CommitSnapshotterConfig init() {
        for( Map.Entry< String, String> entry : keysAndValues.entrySet() ) {
            try {
                String value = config.get(PARENT_KEY + "/" + entry.getKey());
                entry.setValue(value);
            } catch ( RuntimeException e) {
                configValid = false;
                log.warn("Config not set for key {}, uploads disabled", entry.getKey());
                break;
            }
            configValid = true;
        }
        return this;
    }

    public boolean isConfigValid() {
        return configValid;
    }

    public String getAccessKey() {
        return keysAndValues.get("accessKey");
    }

    public String getSecretAccessKey() {
        return keysAndValues.get("secretAccessKey");
    }

    public String getBucketName() {
        return keysAndValues.get("bucketName");
    }

    public String getEndpoint() {
        return keysAndValues.get("endpoint");
    }

    public String getRegion() {
        return keysAndValues.get("region");
    }

    public boolean minioEnabled() {
        return "true".equals(keysAndValues.get("minioEnabled"));
    }

}
