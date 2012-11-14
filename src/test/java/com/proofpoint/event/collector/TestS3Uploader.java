/*
 * Copyright 2011 Proofpoint, Inc.
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

import com.google.common.collect.Sets;
import com.google.common.io.Files;
import com.google.common.io.Resources;
import com.proofpoint.event.collector.combiner.CombinedStoredObject;
import com.proofpoint.event.collector.combiner.StorageSystem;
import com.proofpoint.event.collector.combiner.StoredObject;
import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.util.List;
import java.util.Set;
import org.logicalshift.concurrent.SerialScheduledExecutorService;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

public class TestS3Uploader
{
    private File tempStageDir;
    private S3Uploader uploader;
    private DummyStorageSystem storageSystem;
    private ServerConfig serverConfig;
    private SerialScheduledExecutorService executor;

    @BeforeMethod
    public void setup()
    {
        storageSystem = new DummyStorageSystem();

        tempStageDir = Files.createTempDir();
        tempStageDir.deleteOnExit();

        serverConfig = new ServerConfig()
                    .setLocalStagingDirectory(tempStageDir)
                    .setS3StagingLocation("s3://fake-location");

        executor = new SerialScheduledExecutorService();
    }

    @Test
    public void testSuccessOnFailedDirExists()
    {
        new File(tempStageDir.getPath(), "failed").mkdir();
        uploader = new S3Uploader(storageSystem, serverConfig, new EventPartitioner(), executor, executor);
    }

    @Test (expectedExceptions = IllegalArgumentException.class)
    public void testFailureOnFailedFileExists() throws IOException
    {
        new File(tempStageDir.getPath(), "failed").createNewFile();
        uploader = new S3Uploader(storageSystem, serverConfig, new EventPartitioner(), executor, executor);
    }

    @Test
    public void testSuccessOnDirectoriesInStaging() throws IOException
    {
        new File(tempStageDir.getPath(), "directory").mkdir();
        uploader = new S3Uploader(storageSystem, serverConfig, new EventPartitioner(), executor, executor);
        uploader.start();
    }

    @Test
    public void testUploadPendingFiles() throws Exception
    {
        File pendingFile = new File(tempStageDir, "pending.json.snappy");
        Files.copy(new File(Resources.getResource("pending.json.snappy").toURI()), pendingFile);

        assertTrue(pendingFile.exists());
        uploader = new S3Uploader(storageSystem, serverConfig, new EventPartitioner(), executor, executor);
        uploader.start();
        assertFalse(pendingFile.exists());
        assertTrue(storageSystem.hasReceivedFile(pendingFile));
    }

    @Test
    public void testUploadPendingFilesFailure() throws Exception
    {
        File invalidJsonFile = new File(tempStageDir, "invalidjson.snappy");
        Files.copy(new File(Resources.getResource( "invalidjson.snappy").toURI()), invalidJsonFile);

        assertTrue(invalidJsonFile.exists());
        uploader = new S3Uploader(storageSystem, serverConfig, new EventPartitioner(), executor, executor);
        uploader.start();
        assertFalse(invalidJsonFile.exists());
        assertTrue(new File(tempStageDir.getPath() + "/failed" , invalidJsonFile.getName()).exists());
        assertFalse(storageSystem.hasReceivedFile(invalidJsonFile));
    }

    private class DummyStorageSystem implements StorageSystem
    {
        private Set<File> receivedFiles = Sets.newHashSet();

        public boolean hasReceivedFile(File file)
        {
            return receivedFiles.contains(file);
        }

        @Override
        public StoredObject putObject(URI location, File source)
        {
            receivedFiles.add(source);
            return new StoredObject(URI.create("s3://dummyUri/bucket/day/hour"));
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
