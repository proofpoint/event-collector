/*
 * Copyright 2011-2013 Proofpoint, Inc.
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
import com.proofpoint.event.collector.combiner.CombinedStoredObject;
import com.proofpoint.event.collector.combiner.StorageSystem;
import com.proofpoint.event.collector.combiner.StoredObject;
import com.proofpoint.event.collector.stats.CollectorStats;
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

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

public class TestS3Uploader
{
    private File tempStageDir;
    private S3Uploader uploader;
    private DummyStorageSystem storageSystem;
    private ServerConfig serverConfig;
    private SerialScheduledExecutorService executor;
    private CollectorStats stats;

    @BeforeMethod
    public void setup()
    {
        storageSystem = new DummyStorageSystem();

        tempStageDir = Files.createTempDir();
        tempStageDir.deleteOnExit();

        serverConfig = new ServerConfig()
                .setLocalStagingDirectory(tempStageDir)
                .setS3StagingLocation("s3://fake-location")
                .setRetryPeriod(new Duration(3, TimeUnit.MINUTES))
                .setRetryDelay(new Duration(1, TimeUnit.MINUTES));

        executor = new SerialScheduledExecutorService();
        stats = new CollectorStats();
    }

    @Test
    public void testSuccessOnFailedDirExists()
    {
        new File(tempStageDir.getPath(), "failed").mkdir();
        uploader = new S3Uploader(storageSystem, serverConfig, new EventPartitioner(), executor, executor, stats);

        checkStats(stats, 0, 0, 0);
    }

    @Test(expectedExceptions = IllegalArgumentException.class)
    public void testFailureOnFailedFileExists() throws IOException
    {
        new File(tempStageDir.getPath(), "failed").createNewFile();
        uploader = new S3Uploader(storageSystem, serverConfig, new EventPartitioner(), executor, executor, stats);

        checkStats(stats, 0, 0, 0);
    }

    @Test
    public void testSuccessOnRetryDirExists()
    {
        new File(tempStageDir.getPath(), "retry").mkdir();
        uploader = new S3Uploader(storageSystem, serverConfig, new EventPartitioner(), executor, executor, stats);

        checkStats(stats, 0, 0, 0);
    }

    @Test(expectedExceptions = IllegalArgumentException.class)
    public void testFailureOnRetryFileExists() throws IOException
    {
        new File(tempStageDir.getPath(), "retry").createNewFile();
        uploader = new S3Uploader(storageSystem, serverConfig, new EventPartitioner(), executor, executor, stats);

        checkStats(stats, 0, 0, 0);
    }

    @Test
    public void testSuccessOnDirectoriesInStaging() throws IOException
    {
        new File(tempStageDir.getPath(), "directory").mkdir();
        uploader = new S3Uploader(storageSystem, serverConfig, new EventPartitioner(), executor, executor, stats);
        uploader.start();

        checkStats(stats, 0, 0, 0);
    }

    @Test
    public void testUploadPendingFiles() throws Exception
    {
        File pendingFile = new File(tempStageDir, "pending.json.snappy");
        Files.copy(new File(Resources.getResource("pending.json.snappy").toURI()), pendingFile);

        assertTrue(pendingFile.exists());
        uploader = new S3Uploader(storageSystem, serverConfig, new EventPartitioner(), executor, executor, stats);
        uploader.start();
        assertFalse(pendingFile.exists());
        assertTrue(storageSystem.hasReceivedFile(pendingFile));

        checkStats(stats, 0, 1, 0);
    }

    @Test
    public void testUploadPendingFilesFailure() throws Exception
    {
        File invalidJsonFile = new File(tempStageDir, "invalidjson.snappy");
        Files.copy(new File(Resources.getResource("invalidjson.snappy").toURI()), invalidJsonFile);

        assertTrue(invalidJsonFile.exists());
        uploader = new S3Uploader(storageSystem, serverConfig, new EventPartitioner(), executor, executor, stats);
        uploader.start();
        assertFalse(invalidJsonFile.exists());
        assertTrue(new File(tempStageDir.getPath() + "/failed", invalidJsonFile.getName()).exists());
        assertFalse(storageSystem.hasReceivedFile(invalidJsonFile));

        checkStats(stats, 0, 0, 1);
    }

    @Test
    public void testFailsOnVerifyFile() throws Exception
    {
        File invalidJsonFile = new File(tempStageDir, "invalidjson2.snappy");
        Files.copy(new File(Resources.getResource("invalidjson2.snappy").toURI()), invalidJsonFile);

        assertTrue(invalidJsonFile.exists());
        uploader = new S3Uploader(storageSystem, serverConfig, new EventPartitioner(), executor, executor, stats);
        uploader.start();
        assertFalse(invalidJsonFile.exists());
        assertTrue(new File(tempStageDir.getPath() + "/failed", invalidJsonFile.getName()).exists());
        assertFalse(storageSystem.hasReceivedFile(invalidJsonFile));

        checkStats(stats, 0, 0, 1);
    }

    @Test
    public void testRetryUntilSuccess() throws Exception
    {
        storageSystem.succeedOnAttempt(3);
        File pendingFile = new File(tempStageDir, "pending.json.snappy");
        Files.copy(new File(Resources.getResource("pending.json.snappy").toURI()), pendingFile);

        String retryDir = tempStageDir.getPath() + "/retry";

        assertEquals(storageSystem.getAttempts(pendingFile), 0);
        //attempt 1 to upload from staging directory fails and file is moved to retry directory
        assertTrue(pendingFile.exists());
        uploader = new S3Uploader(storageSystem, serverConfig, new EventPartitioner(), executor, executor, stats);
        uploader.start();
        assertFalse(pendingFile.exists());
        assertTrue(new File(retryDir, pendingFile.getName()).exists());
        assertFalse(storageSystem.hasReceivedFile(pendingFile));
        assertEquals(storageSystem.getAttempts(pendingFile), 1);
        checkStats(stats, 1, 0, 0);

        //attempt 2: file is moved to staging from retry directory, and fails and hence gets back to retry directory
        executor.elapseTime(1, TimeUnit.MINUTES);
        assertFalse(pendingFile.exists());
        assertTrue(new File(retryDir, pendingFile.getName()).exists());
        assertFalse(storageSystem.hasReceivedFile(pendingFile));
        assertEquals(storageSystem.getAttempts(pendingFile), 2);
        checkStats(stats, 2, 0, 0);

        //retryExecutor hasn't run again
        executor.elapseTime(1, TimeUnit.MINUTES);
        assertFalse(pendingFile.exists());
        assertTrue(new File(retryDir, pendingFile.getName()).exists());
        assertFalse(storageSystem.hasReceivedFile(pendingFile));
        assertEquals(storageSystem.getAttempts(pendingFile), 2);
        checkStats(stats, 2, 0, 0);

        //attempt 3: file is moved to staging from retry directory, succeeds and hence is deleted from local directories
        executor.elapseTime(2, TimeUnit.MINUTES);
        assertFalse(pendingFile.exists());
        assertFalse(new File(retryDir, pendingFile.getName()).exists());
        assertTrue(storageSystem.hasReceivedFile(pendingFile));
        assertEquals(storageSystem.getAttempts(pendingFile), 3);

        checkStats(stats, 2, 1, 0);
    }

    @Test
    public void testInvalidFilesInRetryDirectory() throws Exception
    {
        uploader = new S3Uploader(storageSystem, serverConfig, new EventPartitioner(), executor, executor, stats);
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

        checkStats(stats, 0, 0, 2);
    }

    private void checkStats(CollectorStats stats, int uploadFailed, int uploadSucceeded, int fileError)
    {
        assertEquals(stats.getUploadFailureStats().getTotalCount(), uploadFailed);
        assertEquals(stats.getUploadSuccessStats().getTotalCount(), uploadSucceeded);
        assertEquals(stats.getFileErrorStats().getTotalCount(), fileError);
    }

    private class DummyStorageSystem
            implements StorageSystem
    {
        private Set<String> receivedFiles = Sets.newHashSet();
        private Map<String, Integer> retryCounts = Maps.newHashMap();
        private int succeedOnAttempt = 1;

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
