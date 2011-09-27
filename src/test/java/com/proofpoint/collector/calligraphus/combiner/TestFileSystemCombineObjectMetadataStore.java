package com.proofpoint.collector.calligraphus.combiner;

import com.google.common.collect.ImmutableList;
import com.google.common.io.Files;
import org.testng.annotations.Test;

import java.io.File;
import java.net.URI;
import java.util.List;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;

public class TestFileSystemCombineObjectMetadataStore
{
    @Test
    public void testInitialState()
            throws Exception
    {
        FileSystemCombineObjectMetadataStore metadataStore = new FileSystemCombineObjectMetadataStore("test-node", new File("unused"));
        URI location = S3StorageHelper.buildS3Location("s3://bucket/event/time/file.json");

        CombinedGroup combinedGroup = metadataStore.getCombinedGroupManifest(location);
        assertNotNull(combinedGroup);
        assertEquals(combinedGroup.getVersion(), 0);
        assertEquals(combinedGroup.getLocationPrefix(), location);
        assertEquals(combinedGroup.getCreator(), "test-node");
        assertTrue(combinedGroup.getCombinedObjects().isEmpty());
    }

    @Test
    public void test()
            throws Exception
    {

        File tempDir = Files.createTempDir();
        try {
            FileSystemCombineObjectMetadataStore metadataStore = new FileSystemCombineObjectMetadataStore("test-node", tempDir);
            URI location = S3StorageHelper.buildS3Location("s3://bucket/event/time/file.json");

            CombinedGroup initialCombinedGroup = metadataStore.getCombinedGroupManifest(location);
            ImmutableList<StoredObject> storedObjects = ImmutableList.of(
                    new StoredObject(S3StorageHelper.buildS3Location("s3://bucket/stage/event/time/a.json")),
                    new StoredObject(S3StorageHelper.buildS3Location("s3://bucket/stage/event/time/b.json")),
                    new StoredObject(S3StorageHelper.buildS3Location("s3://bucket/stage/event/time/c.json"))
            );
            CombinedGroup newCombinedStoredObject = initialCombinedGroup.addNewCombinedObject("node2", location, storedObjects);
            assertTrue(metadataStore.replaceCombinedGroupManifest(initialCombinedGroup, newCombinedStoredObject));

            CombinedGroup persistentCombinedGroup = metadataStore.getCombinedGroupManifest(location);
            assertNotNull(persistentCombinedGroup);
            assertEquals(persistentCombinedGroup.getVersion(), 1);
            assertEquals(persistentCombinedGroup.getLocationPrefix(), location);
            assertEquals(persistentCombinedGroup.getCreator(), "node2");

            List<CombinedStoredObject> combinedObjects = persistentCombinedGroup.getCombinedObjects();
            assertEquals(combinedObjects.size(), 1);
            assertEquals(combinedObjects.get(0).getSourceParts(), storedObjects);
        }
        finally {
            // todo delete temp dir
        }
    }
}
