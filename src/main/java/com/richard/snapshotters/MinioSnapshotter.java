package com.richard.snapshotters;

import org.apache.lucene.replicator.IndexInputInputStream;
import org.apache.lucene.store.IndexInput;

import com.amazonaws.ClientConfiguration;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.client.builder.AwsClientBuilder;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.richard.CommitSnapshotterConfig;

public class MinioSnapshotter {

    private AmazonS3 s3Client;
    private AWSStaticCredentialsProvider credentials;

    public MinioSnapshotter(CommitSnapshotterConfig config) {
        credentials = new AWSStaticCredentialsProvider(new BasicAWSCredentials(
                config.getAccessKey(),
                config.getSecretAccessKey()
        ));

        ClientConfiguration clientConfiguration = new ClientConfiguration();
        clientConfiguration.setSignerOverride("AWSS3V4SignerType");

        s3Client = AmazonS3ClientBuilder
                .standard()
                .withClientConfiguration(clientConfiguration)
                .withEndpointConfiguration(new AwsClientBuilder.EndpointConfiguration(
                        config.getEndpoint(),
                        config.getRegion()
                ))
                .withPathStyleAccessEnabled(true)
                .withCredentials(credentials)
                .build();
    }

    public AmazonS3 getS3Client() {
        return s3Client;
    }

    public boolean doesBucketExist(String bucketName) {
        return s3Client.doesBucketExistV2(bucketName);
    }

    public void upload(String bucketName, String prefix, String fileName, IndexInput indexInput) {
        String fullFileName = prefix + "/" + fileName;
        ObjectMetadata metadata = new ObjectMetadata();
        metadata.setContentLength(indexInput.length());

        s3Client.putObject(bucketName, fullFileName, new IndexInputInputStream(indexInput), metadata);
    }

    public void upload(String bucketName, String prefix, String fileName, String content) {
        String fullFileName = prefix + "/" + fileName;
        s3Client.putObject(bucketName, fullFileName, content);
    }

}
