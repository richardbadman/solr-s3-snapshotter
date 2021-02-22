import org.apache.solr.SolrTestCaseJ4;
import org.junit.BeforeClass;
import org.junit.Test;

import com.richard.CommitSnapshotterConfig;

public class CommitSnapshotterConfigInvalidTest extends SolrTestCaseJ4 {

    private static final String BUCKET_NAME = "testing";
    private static final String ACCESS_KEY = "ar4nd0mk3y";

    private CommitSnapshotterConfig config;

    @BeforeClass
    public static void beforeClass() throws Exception {
        initCore("test-config-invalid.xml", "schema.xml");
    }

    @Test
    public void whenInitialisingConfigWithValidConfigInSolr_thenExceptionIsMetAndIsValidReturnsFalse() {
        try {
            config = new CommitSnapshotterConfig(h.getCore().getSolrConfig()).init();
        } catch ( RuntimeException e ) {
            assertFalse(config.isConfigValid());

            assertEquals(config.getBucketName(), BUCKET_NAME);
            assertEquals(config.getAccessKey(), ACCESS_KEY);
            assertEquals(config.getSecretAccessKey(), "");
        }
    }


}
