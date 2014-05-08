/*
 * Copyright 2011-2014 Proofpoint, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.proofpoint.event.collector;

import com.google.common.base.Objects;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.google.common.io.Files;
import com.google.common.io.Resources;
import com.google.common.testing.FakeTicker;
import com.proofpoint.event.collector.combiner.CombinedStoredObject;
import com.proofpoint.event.collector.combiner.StorageSystem;
import com.proofpoint.event.collector.combiner.StoredObject;
import com.proofpoint.reporting.testing.TestingReportCollectionFactory;
import com.proofpoint.testing.SerialScheduledExecutorService;
import com.proofpoint.units.Duration;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import static com.proofpoint.event.collector.S3UploaderStats.Status.CORRUPT;
import static com.proofpoint.event.collector.S3UploaderStats.Status.FAILURE;
import static com.proofpoint.event.collector.S3UploaderStats.Status.SUCCESS;
import static com.proofpoint.event.collector.S3UploaderStats.Status.UPLOADED;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

public class TestS3Uploader
{
    private static final String ARBITRARY_EVENT_TYPE = "TapPrsMessage";
    private static final String UNKNOWN_EVENT_TYPE = "unknown";
    private static final Duration UPLOAD_DURATION = new Duration(3L, MILLISECONDS);

    private File tempStageDir;
    private S3Uploader uploader;
    private DummyStorageSystem storageSystem;
    private ServerConfig serverConfig;
    private SerialScheduledExecutorService executor;
    private TestingReportCollectionFactory testingReportCollectionFactory;
    private S3UploaderStats s3UploaderStats;
    private FakeTicker ticker;

    @BeforeMethod
    public void setup()
    {
        ticker = new FakeTicker();
        storageSystem = new DummyStorageSystem(ticker, UPLOAD_DURATION);
        tempStageDir = Files.createTempDir();
        tempStageDir.deleteOnExit();

        serverConfig = new ServerConfig()
                .setLocalStagingDirectory(tempStageDir)
                .setS3StagingLocation("s3://fake-location")
                .setRetryPeriod(new Duration(3, TimeUnit.MINUTES))
                .setRetryDelay(new Duration(1, TimeUnit.MINUTES));

        executor = new SerialScheduledExecutorService();

        testingReportCollectionFactory = new TestingReportCollectionFactory();
        s3UploaderStats = testingReportCollectionFactory.createReportCollection(S3UploaderStats.class);
    }

    @Test(expectedExceptions = NullPointerException.class, expectedExceptionsMessageRegExp = "s3UploaderStats is null")
    public void testConstructorNullS3UploaderStats()
    {
        new S3Uploader(storageSystem, serverConfig, new EventPartitioner(), executor, executor, null, ticker);
    }

    @Test(expectedExceptions = NullPointerException.class, expectedExceptionsMessageRegExp = "ticker is null")
    public void testConstructorNullTicker()
    {
        new S3Uploader(storageSystem, serverConfig, new EventPartitioner(), executor, executor, s3UploaderStats, null);
    }

    @Test
    public void testSuccessOnFailedDirExists()
    {
        new File(tempStageDir.getPath(), "failed").mkdir();
        uploader = new S3Uploader(storageSystem, serverConfig, new EventPartitioner(), executor, executor, s3UploaderStats, ticker);
    }

    @Test(expectedExceptions = IllegalArgumentException.class)
    public void testFailureOnFailedFileExists()
            throws IOException
    {
        new File(tempStageDir.getPath(), "failed").createNewFile();
        uploader = new S3Uploader(storageSystem, serverConfig, new EventPartitioner(), executor, executor, s3UploaderStats, ticker);
    }

    @Test
    public void testSuccessOnRetryDirExists()
    {
        new File(tempStageDir.getPath(), "retry").mkdir();
        uploader = new S3Uploader(storageSystem, serverConfig, new EventPartitioner(), executor, executor, s3UploaderStats, ticker);
    }

    @Test(expectedExceptions = IllegalArgumentException.class)
    public void testFailureOnRetryFileExists()
            throws IOException
    {
        new File(tempStageDir.getPath(), "retry").createNewFile();
        uploader = new S3Uploader(storageSystem, serverConfig, new EventPartitioner(), executor, executor, s3UploaderStats, ticker);
    }

    @Test
    public void testSuccessOnDirectoriesInStaging()
            throws IOException
    {
        new File(tempStageDir.getPath(), "directory").mkdir();
        uploader = new S3Uploader(storageSystem, serverConfig, new EventPartitioner(), executor, executor, s3UploaderStats, ticker);
        uploader.start();
    }

    @Test
    public void testUploadPendingFiles()
            throws Exception
    {
        File pendingFile = new File(tempStageDir, "pending.json.snappy");
        Files.copy(new File(Resources.getResource("pending.json.snappy").toURI()), pendingFile);

        assertTrue(pendingFile.exists());
        uploader = new S3Uploader(storageSystem, serverConfig, new EventPartitioner(), executor, executor, s3UploaderStats, ticker);
        uploader.start();
        assertFalse(pendingFile.exists());
        assertTrue(storageSystem.hasReceivedFile(pendingFile));

        S3UploaderStats s3UploaderStatsArgumentVerifier = testingReportCollectionFactory.getArgumentVerifier(S3UploaderStats.class);
        verify(s3UploaderStatsArgumentVerifier).processedFiles(ARBITRARY_EVENT_TYPE, UPLOADED);
        verify(s3UploaderStatsArgumentVerifier).uploadAttempts(ARBITRARY_EVENT_TYPE, SUCCESS);
        verify(s3UploaderStatsArgumentVerifier).processedTime(ARBITRARY_EVENT_TYPE);
        verifyNoMoreInteractions(s3UploaderStatsArgumentVerifier);

        S3UploaderStats s3UploaderStatsReportCollection = testingReportCollectionFactory.getReportCollection(S3UploaderStats.class);
        verify(s3UploaderStatsReportCollection.processedFiles(ARBITRARY_EVENT_TYPE, UPLOADED)).update(1);
        verify(s3UploaderStatsReportCollection.uploadAttempts(ARBITRARY_EVENT_TYPE, SUCCESS)).update(1);
        verify(s3UploaderStatsReportCollection.processedTime(ARBITRARY_EVENT_TYPE)).add(UPLOAD_DURATION);
        verifyNoMoreInteractions(s3UploaderStatsReportCollection.processedFiles(ARBITRARY_EVENT_TYPE, UPLOADED));
        verifyNoMoreInteractions(s3UploaderStatsReportCollection.uploadAttempts(ARBITRARY_EVENT_TYPE, SUCCESS));
        verifyNoMoreInteractions(s3UploaderStatsReportCollection.processedTime(ARBITRARY_EVENT_TYPE));
    }

    @Test
    public void testUploadPendingFilesFailure()
            throws Exception
    {
        File invalidJsonFile = new File(tempStageDir, "invalidjson.snappy");
        Files.copy(new File(Resources.getResource("invalidjson.snappy").toURI()), invalidJsonFile);

        assertTrue(invalidJsonFile.exists());
        uploader = new S3Uploader(storageSystem, serverConfig, new EventPartitioner(), executor, executor, s3UploaderStats, ticker);
        uploader.start();
        assertFalse(invalidJsonFile.exists());
        assertTrue(new File(tempStageDir.getPath() + "/failed", invalidJsonFile.getName()).exists());
        assertFalse(storageSystem.hasReceivedFile(invalidJsonFile));

        S3UploaderStats s3UploaderStatsArgumentVerifier = testingReportCollectionFactory.getArgumentVerifier(S3UploaderStats.class);
        verify(s3UploaderStatsArgumentVerifier).processedFiles(UNKNOWN_EVENT_TYPE, CORRUPT);
        verifyNoMoreInteractions(s3UploaderStatsArgumentVerifier);

        S3UploaderStats s3UploaderStatsReportCollection = testingReportCollectionFactory.getReportCollection(S3UploaderStats.class);
        verify(s3UploaderStatsReportCollection.processedFiles(UNKNOWN_EVENT_TYPE, CORRUPT)).update(1);
        verifyNoMoreInteractions(s3UploaderStatsReportCollection.processedFiles(UNKNOWN_EVENT_TYPE, CORRUPT));
    }

    @Test
    public void testFailsOnVerifyFile()
            throws Exception
    {
        File invalidJsonFile = new File(tempStageDir, "invalidjson2.snappy");
        Files.copy(new File(Resources.getResource("invalidjson2.snappy").toURI()), invalidJsonFile);

        assertTrue(invalidJsonFile.exists());
        uploader = new S3Uploader(storageSystem, serverConfig, new EventPartitioner(), executor, executor, s3UploaderStats, ticker);
        uploader.start();
        assertFalse(invalidJsonFile.exists());
        assertTrue(new File(tempStageDir.getPath() + "/failed", invalidJsonFile.getName()).exists());
        assertFalse(storageSystem.hasReceivedFile(invalidJsonFile));

        S3UploaderStats s3UploaderStatsArgumentVerifier = testingReportCollectionFactory.getArgumentVerifier(S3UploaderStats.class);
        verify(s3UploaderStatsArgumentVerifier).processedFiles(UNKNOWN_EVENT_TYPE, CORRUPT);
        verifyNoMoreInteractions(s3UploaderStatsArgumentVerifier);

        S3UploaderStats s3UploaderStatsReportCollection = testingReportCollectionFactory.getReportCollection(S3UploaderStats.class);
        verify(s3UploaderStatsReportCollection.processedFiles(UNKNOWN_EVENT_TYPE, CORRUPT)).update(1);
        verifyNoMoreInteractions(s3UploaderStatsReportCollection.processedFiles(UNKNOWN_EVENT_TYPE, CORRUPT));
    }

    @Test
    public void testRetryUntilSuccess()
            throws Exception
    {
        storageSystem.succeedOnAttempt(3);
        File pendingFile = new File(tempStageDir, "pending.json.snappy");
        Files.copy(new File(Resources.getResource("pending.json.snappy").toURI()), pendingFile);

        String retryDir = tempStageDir.getPath() + "/retry";

        assertEquals(storageSystem.getAttempts(pendingFile), 0);
        //attempt 1 to upload from staging directory fails and file is moved to retry directory
        assertTrue(pendingFile.exists());
        uploader = new S3Uploader(storageSystem, serverConfig, new EventPartitioner(), executor, executor, s3UploaderStats, ticker);
        uploader.start();
        assertFalse(pendingFile.exists());
        assertTrue(new File(retryDir, pendingFile.getName()).exists());
        assertFalse(storageSystem.hasReceivedFile(pendingFile));
        assertEquals(storageSystem.getAttempts(pendingFile), 1);

        //attempt 2: file is moved to staging from retry directory, and fails and hence gets back to retry directory
        executor.elapseTime(1, TimeUnit.MINUTES);
        assertFalse(pendingFile.exists());
        assertTrue(new File(retryDir, pendingFile.getName()).exists());
        assertFalse(storageSystem.hasReceivedFile(pendingFile));
        assertEquals(storageSystem.getAttempts(pendingFile), 2);

        //retryExecutor hasn't run again
        executor.elapseTime(1, TimeUnit.MINUTES);
        assertFalse(pendingFile.exists());
        assertTrue(new File(retryDir, pendingFile.getName()).exists());
        assertFalse(storageSystem.hasReceivedFile(pendingFile));
        assertEquals(storageSystem.getAttempts(pendingFile), 2);

        //attempt 3: file is moved to staging from retry directory, succeeds and hence is deleted from local directories
        executor.elapseTime(2, TimeUnit.MINUTES);
        assertFalse(pendingFile.exists());
        assertFalse(new File(retryDir, pendingFile.getName()).exists());
        assertTrue(storageSystem.hasReceivedFile(pendingFile));
        assertEquals(storageSystem.getAttempts(pendingFile), 3);
    }

    @Test
    public void testPulseMetricsForUploadAttempts()
            throws Exception
    {
        storageSystem.succeedOnAttempt(2);
        File pendingFile = new File(tempStageDir, "pending.json.snappy");
        Files.copy(new File(Resources.getResource("pending.json.snappy").toURI()), pendingFile);

        //attempt 1 to upload from staging directory fails and file is moved to retry directory
        uploader = new S3Uploader(storageSystem, serverConfig, new EventPartitioner(), executor, executor, s3UploaderStats, ticker);
        uploader.start();

        S3UploaderStats s3UploaderStatsArgumentVerifier = testingReportCollectionFactory.getArgumentVerifier(S3UploaderStats.class);
        verify(s3UploaderStatsArgumentVerifier).uploadAttempts(ARBITRARY_EVENT_TYPE, FAILURE);
        verifyNoMoreInteractions(s3UploaderStatsArgumentVerifier);

        S3UploaderStats s3UploaderStatsReportCollection = testingReportCollectionFactory.getReportCollection(S3UploaderStats.class);
        verify(s3UploaderStatsReportCollection.uploadAttempts(ARBITRARY_EVENT_TYPE, FAILURE)).update(1);
        verifyNoMoreInteractions(s3UploaderStatsReportCollection.uploadAttempts(ARBITRARY_EVENT_TYPE, FAILURE));
    }

    @Test
    public void testInvalidFilesInRetryDirectory()
            throws Exception
    {
        uploader = new S3Uploader(storageSystem, serverConfig, new EventPartitioner(), executor, executor, s3UploaderStats, ticker);
        uploader.start();

        executor.elapseTime(1, TimeUnit.MINUTES);
        String retryDir = tempStageDir.getPath() + "/retry";
        String failedDir = tempStageDir.getPath() + "/failed";

        File invalidJsonFile = new File(retryDir, "invalidjson.snappy");
        Files.copy(new File(Resources.getResource("invalidjson.snappy").toURI()), invalidJsonFile);

        File invalidFile = new File(retryDir, "invalidFile.snappy");
        Files.copy(new File(Resources.getResource("invalidjson2.snappy").toURI()), invalidFile);

        File directory = new File(retryDir, "subdir");
        directory.mkdir();

        assertTrue(invalidJsonFile.exists());
        assertTrue(invalidFile.exists());
        assertTrue(directory.exists());

        executor.elapseTime(3, TimeUnit.MINUTES);

        assertTrue(new File(failedDir, invalidJsonFile.getName()).exists());
        assertFalse(new File(retryDir, invalidJsonFile.getName()).exists());
        assertFalse(new File(tempStageDir.getPath(), invalidJsonFile.getName()).exists());

        assertTrue(new File(failedDir, invalidFile.getName()).exists());
        assertFalse(new File(retryDir, invalidFile.getName()).exists());
        assertFalse(new File(tempStageDir.getPath(), invalidFile.getName()).exists());

        assertTrue(directory.exists());
    }

    private class DummyStorageSystem
            implements StorageSystem
    {
        private final FakeTicker ticker;
        private final Duration uploadDuration;
        private Set<String> receivedFiles = Sets.newHashSet();
        private Map<String, Integer> retryCounts = Maps.newHashMap();
        private int succeedOnAttempt = 1;

        public DummyStorageSystem(FakeTicker ticker, Duration uploadDuration)
        {
            this.ticker = ticker;
            this.uploadDuration = uploadDuration;
        }

        public boolean hasReceivedFile(File file)
        {
            return receivedFiles.contains(file.getName());
        }

        public void succeedOnAttempt(int attempt)
        {
            succeedOnAttempt = attempt;
        }

        public int getAttempts(File file)
        {
            return Objects.firstNonNull(retryCounts.get(file.getName()), 0);
        }

        @Override
        public StoredObject putObject(URI location, File source)
        {
            int pastAttempts = getAttempts(source);
            int currentAttempt = pastAttempts + 1;
            retryCounts.put(source.getName(), currentAttempt);

            if (currentAttempt == succeedOnAttempt) {
                ticker.advance(uploadDuration.toMillis(), uploadDuration.getUnit());
                receivedFiles.add(source.getName());
                return new StoredObject(URI.create("s3://dummyUri/bucket/day/hour"));
            }
            throw new RuntimeException("Exception in DummyStorageSystem");
        }

        @Override
        public List<URI> listDirectories(URI storageArea)
        {
            throw new UnsupportedOperationException("Not supported yet.");
        }

        @Override
        public List<StoredObject> listObjects(URI storageArea)
        {
            throw new UnsupportedOperationException("Not supported yet.");
        }

        @Override
        public StoredObject createCombinedObject(CombinedStoredObject combinedObject)
        {
            throw new UnsupportedOperationException("Not supported yet.");
        }
    }
}
