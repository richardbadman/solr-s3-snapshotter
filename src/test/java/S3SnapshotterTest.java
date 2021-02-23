import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.when;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import com.amazonaws.services.s3.AmazonS3;
import com.richard.snapshotters.S3Snapshotter;

@RunWith(MockitoJUnitRunner.class)
public class S3SnapshotterTest {

    @Mock
    private AmazonS3 s3Client;

    @InjectMocks
    private S3Snapshotter snapshotter;

    private static final String BUCKET_NAME = "testing";

    @Before
    public void setUp() {
        when(s3Client.doesBucketExistV2(BUCKET_NAME)).thenReturn(true);
    }

    @Test
    public void whenBucketExists_thenDoesBucketExistReturnsTrue() {
        assertTrue(snapshotter.doesBucketExist("testing"));
    }

    @Test
    public void whenBucketDoesntExist_thenDoesBucketExistReturnsFalse() {
        assertFalse(snapshotter.doesBucketExist("invalid"));
    }
}
