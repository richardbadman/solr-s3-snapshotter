package com.richard;

import org.apache.solr.core.SolrConfig;

public class CommitSnapshotterConfig {

    private static final String PARENT_KEY = "/commitSnapshotter";
    private static final String BUCKET_KEY = "/bucketName";
    private static final String ACCESS_KEY_KEY = "/accessKey";
    private static final String SECRET_ACCESS_KEY_KEY = "/secretAccessKey";
    private static final String ENDPOINT_KEY = "/endpoint";
    private static final String REGION_KEY = "/region";

    private static String accessKey;
    private static String secretAccessKey;
    private static String bucketName;
    private static String endpoint;
    private static String region;

    /*
        <commitSnapshotter>
          <bucketName>a_bucket</bucketName>
          <accessKey>ACCESSK3Y</accessKey>
          <secretAccessKey>S3CR3TACC3SSK3Y</secretAccessKey>
          <endpoint>http://localhost:9000/</endpoint>
          <region>us-east-1</region>
        </commitSnapshotter>
     */
    // TODO - Add verification?
    // Throws runtimeexception if missing
    public CommitSnapshotterConfig(SolrConfig config) {
        accessKey = config.get(PARENT_KEY + "/" + ACCESS_KEY_KEY);
        secretAccessKey = config.get(PARENT_KEY + "/" + SECRET_ACCESS_KEY_KEY);
        bucketName = config.get(PARENT_KEY + "/" + BUCKET_KEY);
        endpoint = config.get(PARENT_KEY + "/" + ENDPOINT_KEY);
        region = config.get(PARENT_KEY + "/" + REGION_KEY);
    }

    public String getAccessKey() {
        return accessKey;
    }

    public String getSecretAccessKey() {
        return secretAccessKey;
    }

    public String getBucketName() {
        return bucketName;
    }

    public String getEndpoint() {
        return endpoint;
    }

    public String getRegion() {
        return region;
    }

}
