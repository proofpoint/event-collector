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

import com.google.common.io.Files;
import com.google.common.io.Resources;
import com.proofpoint.event.collector.combiner.CombinedStoredObject;
import com.proofpoint.event.collector.combiner.StorageSystem;
import com.proofpoint.event.collector.combiner.StoredObject;
import java.io.File;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.List;
import org.logicalshift.concurrent.SerialScheduledExecutorService;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

public class TestS3Uploader
{
    private S3Uploader uploader;
    private File pendingFile;
    DummyStorageSystem storageSystem;

    @BeforeMethod
    public void setup() throws Exception
    {
        storageSystem = new DummyStorageSystem();

        File tempStageDir = Files.createTempDir();
        tempStageDir.deleteOnExit();

        pendingFile = new File(tempStageDir, "pending.json.snappy");
        Files.copy(new File(Resources.getResource("pending.json.snappy").toURI()), pendingFile);

        ServerConfig serverConfig = new ServerConfig()
                    .setLocalStagingDirectory(tempStageDir)
                    .setS3StagingLocation("s3://fake-location");

        SerialScheduledExecutorService executor = new SerialScheduledExecutorService();
        uploader = new S3Uploader(storageSystem, serverConfig, new EventPartitioner(), executor, executor);
    }

    @Test
    public void testEnquePendingFiles() throws URISyntaxException
    {
        assertTrue(pendingFile.exists());
        uploader.start();
        assertFalse(pendingFile.exists());
    }

    private class DummyStorageSystem implements StorageSystem
    {
        @Override
        public StoredObject putObject(URI location, File source)
        {
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
