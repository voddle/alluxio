package alluxio.client.cli.fs.command;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import alluxio.AlluxioURI;
import alluxio.Constants;
import alluxio.SystemErrRule;
import alluxio.SystemOutRule;
import alluxio.UnderFileSystemFactoryRegistryRule;
import alluxio.cli.fs.FileSystemShell;
import alluxio.cli.job.JobShell;
import alluxio.client.file.FileSystem;
import alluxio.client.file.URIStatus;
import alluxio.concurrent.jsr.CompletableFuture;
import alluxio.conf.Configuration;
import alluxio.conf.PropertyKey;
import alluxio.exception.AlluxioException;
import alluxio.grpc.GetStatusPOptions;
import alluxio.grpc.LoadMetadataPType;
import alluxio.master.LocalAlluxioCluster;
import alluxio.master.LocalAlluxioJobCluster;
import alluxio.testutils.BaseIntegrationTest;
import alluxio.testutils.LocalAlluxioClusterResource;
import alluxio.testutils.underfs.sleeping.SleepingUnderFileSystemFactory;
import alluxio.testutils.underfs.sleeping.SleepingUnderFileSystemOptions;

import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.client.builder.AwsClientBuilder;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import org.gaul.s3proxy.junit.S3ProxyRule;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.HashSet;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public final class LoadMetadataCommandV2IntegrationTest extends BaseIntegrationTest {
  private String mTestBucket = "test-bucket";
  private final String mTestContent = "TestContents";
  private final String mTestFile = "test_file";
  private final int mUserQuotaUnitBytes = 1000;
  public ByteArrayOutputStream mOutput = new ByteArrayOutputStream();
  public ByteArrayOutputStream mErrOutput = new ByteArrayOutputStream();
  public ExpectedException mException = ExpectedException.none();

  @Rule
  public SystemOutRule mOutputRule = new SystemOutRule(mOutput);

  @Rule
  public SystemErrRule mErrRule = new SystemErrRule(mErrOutput);

  @Rule
  public ExpectedException mThrown = ExpectedException.none();

  @Rule
  public S3ProxyRule mS3Proxy = S3ProxyRule.builder()
      .withBlobStoreProvider("transient")
      .withPort(8001)
      .withCredentials("_", "_")
      .build();

  public LocalAlluxioCluster mLocalAlluxioCluster;
  public FileSystem mFileSystem;
  public FileSystemShell mFsShell;
  protected LocalAlluxioJobCluster mLocalAlluxioJobCluster;
  protected JobShell mJobShell;

  @Rule
  public LocalAlluxioClusterResource mLocalAlluxioClusterResource =
      new LocalAlluxioClusterResource.Builder()
            .setProperty(PropertyKey.USER_FILE_BUFFER_BYTES, mUserQuotaUnitBytes)
            .setProperty(PropertyKey.UNDERFS_S3_ENDPOINT, "localhost:8001")
            .setProperty(PropertyKey.UNDERFS_S3_ENDPOINT_REGION, "us-west-2")
            .setProperty(PropertyKey.UNDERFS_S3_DISABLE_DNS_BUCKETS, true)
            .setProperty(PropertyKey.MASTER_MOUNT_TABLE_ROOT_UFS, "s3://" + mTestBucket)
            .setProperty(PropertyKey.S3A_ACCESS_KEY, mS3Proxy.getAccessKey())
            .setProperty(PropertyKey.S3A_SECRET_KEY, mS3Proxy.getSecretKey())
            .setStartCluster(false)
            .build();

  private static final long SLEEP_MS = Constants.SECOND_MS / 2;

  @Rule
  public UnderFileSystemFactoryRegistryRule mUnderfilesystemfactoryregistry =
      new UnderFileSystemFactoryRegistryRule(new SleepingUnderFileSystemFactory(
          new SleepingUnderFileSystemOptions()
              .setGetStatusMs(SLEEP_MS)
              .setExistsMs(SLEEP_MS)
              .setListStatusMs(SLEEP_MS)
              .setListStatusWithOptionsMs(SLEEP_MS)));

  private AmazonS3 mS3Client = null;

  @Before
  public void before() throws Exception {
    mS3Client = AmazonS3ClientBuilder
        .standard()
        .withPathStyleAccessEnabled(true)
        .withCredentials(
            new AWSStaticCredentialsProvider(
                new BasicAWSCredentials(mS3Proxy.getAccessKey(), mS3Proxy.getSecretKey())))
        .withEndpointConfiguration(
            new AwsClientBuilder.EndpointConfiguration(mS3Proxy.getUri().toString(),
                Regions.US_WEST_2.getName()))
        .build();
    mS3Client.createBucket(mTestBucket);

    mLocalAlluxioClusterResource.start();
    mLocalAlluxioCluster = mLocalAlluxioClusterResource.get();
    mFileSystem = mLocalAlluxioCluster.getClient();
    mFsShell = new FileSystemShell(Configuration.global());
  }

  @After
  public void after() throws Exception {
    mS3Client = null;
    if (mFsShell != null) {
      mFsShell.close();
    }
    if (mLocalAlluxioJobCluster != null) {
      mLocalAlluxioJobCluster.stop();
    }
    if (mJobShell != null) {
      mJobShell.close();
    }
  }

  @Test
  public void loadMetadataTestV2get() throws IOException, AlluxioException {
    for (int i = 0; i < 1; i++) {
      mS3Client.putObject(mTestBucket, mTestFile + i, mTestContent);
    }
    mOutput.reset();
    AlluxioURI uriDir = new AlluxioURI("/");
    CompletableFuture<Void> future = CompletableFuture.supplyAsync(() -> {
      mFsShell.run("loadMetadata", "-v2", "-R", "-a", uriDir.toString());
      return null;
    });
    FileSystemShell anotherFsShell = new FileSystemShell(Configuration.global());
    // Running
    anotherFsShell.run("loadMetadata", "-v2", "-o", "get", "-id", "0");
    assertTrue(mOutput.toString().contains("State: RUNNING"));
    mOutput.reset();
    // Cancel success
    anotherFsShell.run("loadMetadata", "-v2", "-o", "cancel", "-id", "0");
    // Get cancel
    anotherFsShell.run("loadMetadata", "-v2", "-o", "get", "-id", "0");
    assertTrue(mOutput.toString().contains("State: CANCELED"));
    mOutput.reset();
    mFsShell.run("loadMetadata", "-v2", "-R", uriDir.toString());
    // clean the mOutput stream
    mOutput.reset();
    // start a new loadMetadata task for SUCCEEDED test
    anotherFsShell.run("loadMetadata", "-v2", "-o", "get", "-id", "1");
    assertTrue(mOutput.toString().contains("State: SUCCEEDED"));
  }

  @Test
  public void loadMetadataTestV2cancel() {
    int fileCount = 10;
    for (int i = 0; i < fileCount; i++) {
      mS3Client.putObject(mTestBucket, mTestFile + i, mTestContent);
    }
    mOutput.reset();
    AlluxioURI uriDir = new AlluxioURI("/");
    CompletableFuture<Void> future = CompletableFuture.supplyAsync(() -> {
      mFsShell.run("loadMetadata", "-v2", "-R", "-a", uriDir.toString());
      return null;
    });
    String id;
    Pattern pattern = Pattern.compile("Task group (\\d+)");
    Matcher matcher = pattern.matcher(mOutput.toString());
    while (!matcher.find()) {
      matcher = pattern.matcher(mOutput.toString());
    }
    id = matcher.group(1);
    FileSystemShell anotherFsShell = new FileSystemShell(Configuration.global());

    // Cancel a running task
    anotherFsShell.run("loadMetadata", "-v2", "-o", "cancel", "-id", id);
    assertTrue(mOutput.toString().contains(String.format("Task group %s cancelled", id)));

    // Cancel a canceled task
    mOutput.reset();
    anotherFsShell.run("loadMetadata", "-v2", "-o", "cancel", "-id", id);
    assertTrue(mOutput.toString().contains(String.format("Task %s not found "
        + "or has already been canceled", id)));

    // Trying to cancel completed task
    mOutput.reset();
    mFsShell.run("loadMetadata", "-v2", "-R", "-a", uriDir.toString());
    matcher = pattern.matcher(mOutput.toString());
    while (!matcher.find()) {
      matcher = pattern.matcher(mOutput.toString());
    }
    id = matcher.group(1);
    mFsShell.run("loadMetadata", "-v2", "-o", "cancel", "-id", id);
    assertTrue(mOutput.toString().contains(String.format("Task %s not found or has"
        + "already been canceled", id)));
  }

  @Test
  public void loadMetadataTestV2Recursive() throws IOException, AlluxioException {
    int dirCount = 3;
    int fileCount = 10;
    for (int dirIndex = 0; dirIndex < dirCount; dirIndex++) {
      for (int fileIndex = 0; fileIndex < fileCount; fileIndex++) {
        mS3Client.putObject(mTestBucket, "test" + dirIndex + "/" + fileIndex, mTestContent);
      }
    }
    AlluxioURI uriDir = new AlluxioURI("/");

    mFsShell.run("loadMetadata", "-v2", "-a", uriDir.toString());
    assertTrue(mOutput.toString().contains("State: SUCCEEDED"));
    GetStatusPOptions getStatusPOptions =
        GetStatusPOptions.newBuilder().setLoadMetadataType(LoadMetadataPType.NEVER).build();
    URIStatus statusAfter = null;
    for (int i = 0; i < dirCount; i++) {
      statusAfter = mFileSystem.getStatus(new AlluxioURI("/test" + i), getStatusPOptions);
      assertEquals(statusAfter.getFileInfo().getLength(), 0);
    }
    // avoid legacy "State: SUCCEEDED"
    mOutput.reset();
    mFsShell.run("loadMetadata", "-v2", "-R", "-a", uriDir.toString());
    assertTrue(mOutput.toString().contains("State: SUCCEEDED"));
    for (int i = 0; i < dirCount; i++) {
      statusAfter = mFileSystem.getStatus(new AlluxioURI("/test" + i), getStatusPOptions);
      assertEquals(statusAfter.getFileInfo().getLength(), fileCount);
    }
  }

  @Test
  public void loadMetadataTestV2NestMounted() {
    int mntCount = 10;
    mFsShell.run("mkdir", "/mnt");
    for (int i = 0; i < mntCount; i++) {
      mS3Client.createBucket("test" + i);
      mFsShell.run("mount", "/mnt/test" + i, "s3://test" + i);
    }
    AlluxioURI uriDir = new AlluxioURI("/");
    mOutput.reset();
    mFsShell.run("loadMetadata", "-v2", "-R", "-a", uriDir.toString());
    Set<Integer> idRecord = new HashSet<>();
    Pattern pattern = Pattern.compile("Task id: (\\d+)");
    Matcher matcher = pattern.matcher(mOutput.toString());
    while (matcher.find()) {
      idRecord.add(Integer.valueOf(matcher.group(1)));
    }
    // mntCount + 1 because root mount point doesn't count in mntCount
    assertEquals(mntCount + 1, idRecord.size());
  }

  @Test
  public void loadMetadataTestV2NestMountedCancel() {
    int mntCount = 10;
    mFsShell.run("mkdir", "/mnt");
    // mount to /mnt/test0.../mnt/test<mntCount>
    for (int i = 0; i < mntCount; i++) {
      mS3Client.createBucket("test" + i);
      mFsShell.run("mount", "/mnt/test" + i, "s3://test" + i);
    }
    AlluxioURI uriDir = new AlluxioURI("/");
    mOutput.reset();
    CompletableFuture<Void> future = CompletableFuture.supplyAsync(() -> {
      mFsShell.run("loadMetadata", "-v2", "-R", "-a", uriDir.toString());
      return null;
    });

    String id;
    Pattern groupIdPattern = Pattern.compile("Task group (\\d+)");
    Matcher groupIdMatcher = groupIdPattern.matcher(mOutput.toString());
    while (!groupIdMatcher.find()) {
      groupIdMatcher = groupIdPattern.matcher(mOutput.toString());
    }
    id = groupIdMatcher.group(1);

    FileSystemShell anotherFsShell = new FileSystemShell(Configuration.global());
    anotherFsShell.run("loadMetadata", "-v2", "-o", "cancel", "-id", id);
    mOutput.reset();
    anotherFsShell.run("loadMetadata", "-v2", "-o", "get", "-id", id);

    Set<Integer> idRecord = new HashSet<>();
    int cancelRecord = 0;
    Pattern idPattern = Pattern.compile("Task id: (\\d+)");
    Pattern cancelPattern = Pattern.compile("State: CANCELED");
    Matcher idMatcher = idPattern.matcher(mOutput.toString());
    Matcher cancelMatcher = cancelPattern.matcher(mOutput.toString());
    while (idMatcher.find()) {
      idRecord.add(Integer.valueOf(idMatcher.group(1)));
    }
    while (cancelMatcher.find()) {
      cancelRecord += 1;
    }

    // mntCount + 1 because root mount point doesn't count in mntCount
    assertEquals(mntCount + 1, idRecord.size());
    assertEquals(mntCount + 1, cancelRecord);
  }
}
