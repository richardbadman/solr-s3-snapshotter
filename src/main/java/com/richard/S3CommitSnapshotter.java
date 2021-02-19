package com.richard;

import java.io.IOException;
import java.nio.file.Paths;
import java.util.Collection;
import java.util.Collections;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.lucene.index.IndexCommit;
import org.apache.lucene.replicator.IndexInputInputStream;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.solr.core.IndexDeletionPolicyWrapper;
import org.apache.solr.core.SolrConfig;
import org.apache.solr.core.SolrCore;
import org.apache.solr.update.CommitUpdateCommand;
import org.apache.solr.update.DirectUpdateHandler2;
import org.apache.solr.update.UpdateHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.amazonaws.ClientConfiguration;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.client.builder.AwsClientBuilder;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.ObjectMetadata;

public class S3CommitSnapshotter extends DirectUpdateHandler2 {

    private static final Logger log = LoggerFactory.getLogger(S3CommitSnapshotter.class);
    private static final String COLLECTION_PATTERN = ".*_shard\\d+";

    private String bucketName;
    private String collectionShardName;
    private String accessKey;
    private String secretAccessKey;

    public S3CommitSnapshotter(SolrCore core) {
        super(core);
    }

    public S3CommitSnapshotter(SolrCore core, UpdateHandler updateHandler) {
        super(core, updateHandler);
    }

    @Override
    public void commit(CommitUpdateCommand cmd) throws IOException {
        super.commit(cmd);
        if (cmd.prepareCommit || cmd.softCommit) {
            log.info("Either prepare commit or soft commit - skipping");
            return;
        }
        setupConfig(core);
        IndexDeletionPolicyWrapper wrapper = core.getDeletionPolicy();
        IndexCommit commit = wrapper.getLatestCommit();
        Collection<String> fileNames = commit.getFileNames();

        upload(fileNames);
        wrapper.releaseCommitPoint(commit);
    }

    private void setupConfig(SolrCore core) {
        SolrConfig solrConfig = core.getSolrConfig();
        bucketName = solrConfig.get("s3snapshotwriter" + "/s3bucket");
        accessKey = solrConfig.get("s3snapshotwriter" + "/accesskey");
        secretAccessKey = solrConfig.get("s3snapshotwriter" + "/secretaccesskey");
        collectionShardName = getCollectionShardName(core.getName());
    }

    private String getCollectionShardName(String coreName) {
        Pattern pattern = Pattern.compile("(.*_shard[0-9]+)");
        Matcher matcher = pattern.matcher(coreName);
        if (matcher.find()) {
            return matcher.group(1);
        }
        log.warn("Unable to extract collection_shard from core name, using original core name");
        return coreName;
    }

    private void upload(Collection<String> fileNames) {
        AWSCredentials credentials = new BasicAWSCredentials(accessKey, secretAccessKey);
        ClientConfiguration clientConfiguration = new ClientConfiguration();
        clientConfiguration.setSignerOverride("AWSS3V4SignerType");
        AmazonS3 s3Client = AmazonS3ClientBuilder
                .standard()
                .withClientConfiguration(clientConfiguration)
                .withEndpointConfiguration(new AwsClientBuilder.EndpointConfiguration("http://localhost:9000", Regions.US_EAST_1.getName()))
                .withPathStyleAccessEnabled(true)
                .withCredentials(new AWSStaticCredentialsProvider(credentials))
                .build();

        try ( Directory directory = FSDirectory.open(Paths.get(core.getIndexDir()))) {
            long timestamp = System.currentTimeMillis();
            fileNames.parallelStream()
                    .forEach(file -> {
                        String prefix = new StringBuilder(collectionShardName)
                                .append("_")
                                .append(timestamp)
                                .append("/")
                                .append(file).toString();
                        try (IndexInput indexInput = directory.openInput(file, IOContext.READONCE)) {
                            ObjectMetadata metadata = new ObjectMetadata();
                            metadata.setContentLength(indexInput.length());
                            s3Client.putObject(bucketName, prefix, new IndexInputInputStream(indexInput), metadata);
                        } catch (IOException e) {
                            e.printStackTrace();
                        }

                    });
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
