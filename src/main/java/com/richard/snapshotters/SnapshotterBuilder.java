package com.richard.snapshotters;

import com.amazonaws.ClientConfiguration;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.client.builder.AwsClientBuilder;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.richard.CommitSnapshotterConfig;

public class SnapshotterBuilder {
    // TODO make this static and the config class too >.>

    private String accessKey;
    private String secretAccessKey;
    private String endpoint;
    private String region;
    private boolean minioEnabled;

    public SnapshotterBuilder(CommitSnapshotterConfig config) {
        accessKey = config.getAccessKey();
        secretAccessKey = config.getSecretAccessKey();
        endpoint = config.getEndpoint();
        region = config.getRegion();
        minioEnabled = config.minioEnabled();
    }

    public AmazonS3 getS3CompatibleClient() {
        return minioEnabled ? getS3ClientForMinio() : getS3Client();
    }

    private AmazonS3 getS3Client() {
        return getBaseClient().build();
    }

    private AmazonS3 getS3ClientForMinio() {
        ClientConfiguration clientConfiguration = new ClientConfiguration();
        clientConfiguration.setSignerOverride("AWSS3V4SignerType");
        return getBaseClient()
                .withClientConfiguration(clientConfiguration)
                .build();
    }

    private AmazonS3ClientBuilder getBaseClient() {
        return AmazonS3ClientBuilder
                .standard()
                .withEndpointConfiguration(new AwsClientBuilder.EndpointConfiguration(
                        endpoint,
                        region
                ))
                .withPathStyleAccessEnabled(true)
                .withCredentials(new AWSStaticCredentialsProvider(
                        new BasicAWSCredentials(
                                accessKey,
                                secretAccessKey
                        )
                ));
    }

}
