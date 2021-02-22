package com.richard;

import java.io.IOException;
import java.nio.file.Paths;
import java.util.Collection;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.lucene.index.IndexCommit;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.solr.core.IndexDeletionPolicyWrapper;
import org.apache.solr.core.SolrCore;
import org.apache.solr.update.CommitUpdateCommand;
import org.apache.solr.update.DirectUpdateHandler2;
import org.apache.solr.update.UpdateHandler;
import org.jose4j.json.internal.json_simple.JSONArray;
import org.jose4j.json.internal.json_simple.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.richard.snapshotters.MinioSnapshotter;

public class S3CommitSnapshotter extends DirectUpdateHandler2 {
    // TODO
    /*
        - Add tons of exception handling
        - [DONE] Check if bucket exists
        - Encryption for access keys
        - a factory or something to differentiate between using minio and s3?
        - TESTS
     */

    private static final Logger log = LoggerFactory.getLogger(S3CommitSnapshotter.class);

    private String bucketName;
    private CommitSnapshotterConfig config;
    private MinioSnapshotter snapshotter;
    private String collectionShardName;

    public S3CommitSnapshotter(SolrCore core) {
        super(core);
        setup();
    }

    public S3CommitSnapshotter(SolrCore core, UpdateHandler updateHandler) {
        super(core, updateHandler);
        setup();
    }

    @Override
    public void commit(CommitUpdateCommand cmd) throws IOException {
        super.commit(cmd);
        if (cmd.prepareCommit || cmd.softCommit) {
            log.info("Either prepare commit or soft commit - skipping upload");
            return;
        }

        boolean bucketExists = snapshotter.doesBucketExist(bucketName);
        if (!bucketExists) {
            log.info("Bucket {} doesn't exist - skipping upload", bucketName);
            return;
        }
        IndexDeletionPolicyWrapper wrapper = core.getDeletionPolicy();
        IndexCommit commit = wrapper.getLatestCommit();
        Collection<String> fileNames = commit.getFileNames();

        upload(fileNames, snapshotter, commit.getGeneration());
        wrapper.releaseCommitPoint(commit);
    }

    private void setup() {
        config = new CommitSnapshotterConfig(core.getSolrConfig());
        collectionShardName = getCollectionShardName(core.getName());
        bucketName = config.getBucketName();
        snapshotter = new MinioSnapshotter(config.getAccessKey(), config.getSecretAccessKey());
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

    private void upload(Collection<String> fileNames, MinioSnapshotter snapshotter, long generation) {
        try ( Directory directory = FSDirectory.open(Paths.get(core.getIndexDir()))) {
            fileNames.parallelStream()
                    .forEach(file -> {
                        try (IndexInput indexInput = directory.openInput(file, IOContext.READONCE)) {
                            snapshotter.upload(bucketName, collectionShardName, file, indexInput);
                        } catch (IOException e) {
                            e.printStackTrace();
                        }

                    });
            String details = createDetailsFile(fileNames, generation);
            String detailFileName = ".snapshot_details_" + generation;
            snapshotter.upload(bucketName, collectionShardName, detailFileName, details);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private String createDetailsFile(Collection<String> fileNames, long generation) {
        JSONObject jsonObject = new JSONObject();
        jsonObject.put("generation", generation);
        jsonObject.put("files", new JSONArray(fileNames));
        return jsonObject.toJSONString();
    }
}
