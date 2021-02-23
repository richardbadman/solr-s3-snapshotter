package com.richard.snapshotters;

import org.apache.lucene.replicator.IndexInputInputStream;
import org.apache.lucene.store.IndexInput;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.ObjectMetadata;

public class S3Snapshotter {

    private AmazonS3 s3Client;

    public S3Snapshotter(AmazonS3 client) {
        s3Client = client;
    }

    public boolean doesBucketExist(String bucketName) {
        return s3Client != null && s3Client.doesBucketExistV2(bucketName);
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
