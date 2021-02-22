import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.core.SolrConfig;
import org.junit.BeforeClass;
import org.junit.Test;

import com.richard.CommitSnapshotterConfig;

public class CommitSnapshotterConfigValidTest extends SolrTestCaseJ4 {

    private static final String BUCKET_NAME = "testing";
    private static final String ACCESS_KEY = "ar4nd0mk3y";
    private static final String SECRET_ACCESS_KEY = "s3cr3t4cc3ssk3y";
    private static final String ENDPOINT = "http://localhost:1234";
    private static final String REGION = "us-east-1";

    private CommitSnapshotterConfig config;

    @BeforeClass
    public static void beforeClass() throws Exception {
        initCore("test-config-valid.xml", "schema.xml");
    }

    @Test
    public void whenInitialisingConfigWithValidConfigInSolr_thenIsValidReturnsTrue() {
        config = new CommitSnapshotterConfig(h.getCore().getSolrConfig()).init();
        assertTrue(config.isConfigValid());

        assertEquals(config.getBucketName(), BUCKET_NAME);
        assertEquals(config.getAccessKey(), ACCESS_KEY);
        assertEquals(config.getSecretAccessKey(), SECRET_ACCESS_KEY);
        assertEquals(config.getEndpoint(), ENDPOINT);
        assertEquals(config.getRegion(), REGION);
    }


}

