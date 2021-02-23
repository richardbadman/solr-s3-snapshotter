import org.apache.solr.SolrTestCaseJ4;
import org.junit.BeforeClass;
import org.junit.Test;

import com.richard.CommitSnapshotterConfig;

public class CommitSnapshotterConfigInvalidTest extends SolrTestCaseJ4 {

    private CommitSnapshotterConfig config;

    @BeforeClass
    public static void beforeClass() throws Exception {
        initCore("test-config-invalid.xml", "schema.xml");
    }

    @Test
    public void whenInitialisingConfigWithValidConfigInSolr_thenExceptionIsMetAndIsValidReturnsFalse() {
        config = new CommitSnapshotterConfig(h.getCore().getSolrConfig()).init();
            assertFalse(config.isConfigValid());

            assertEquals("endpoint-not-set", config.getEndpoint());
            assertEquals("region-not-set", config.getRegion());
            assertFalse(config.minioEnabled());
    }


}
