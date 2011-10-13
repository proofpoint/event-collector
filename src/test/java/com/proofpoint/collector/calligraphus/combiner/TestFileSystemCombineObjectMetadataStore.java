package com.proofpoint.collector.calligraphus.combiner;

import com.google.common.collect.ImmutableList;
import com.google.common.io.Files;
import com.proofpoint.collector.calligraphus.EventPartition;
import org.testng.annotations.Test;

import java.io.File;
import java.net.URI;
import java.util.List;

import static com.proofpoint.collector.calligraphus.combiner.CombinedGroup.createInitialCombinedGroup;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;

public class TestFileSystemCombineObjectMetadataStore
{
    @Test
    public void testInitialState()
            throws Exception
    {
        EventPartition eventPartition = new EventPartition("event", "day", "hour");
        String sizeName = "small";

        FileSystemCombineObjectMetadataStore metadataStore = new FileSystemCombineObjectMetadataStore(new File("unused"));
        assertNull(metadataStore.getCombinedGroupManifest(eventPartition, sizeName));
    }

    @Test
    public void test()
            throws Exception
    {
        EventPartition eventPartition = new EventPartition("event", "day", "hour");
        String sizeName = "small";

        File tempDir = Files.createTempDir();
        try {
            FileSystemCombineObjectMetadataStore metadataStore = new FileSystemCombineObjectMetadataStore(tempDir);
            URI location = S3StorageHelper.buildS3Location("s3://bucket/event/time/file.json");

            CombinedGroup initialCombinedGroup = createInitialCombinedGroup(location, "test");
            ImmutableList<StoredObject> storedObjects = ImmutableList.of(
                    new StoredObject(S3StorageHelper.buildS3Location("s3://bucket/stage/event/time/a.json")),
                    new StoredObject(S3StorageHelper.buildS3Location("s3://bucket/stage/event/time/b.json")),
                    new StoredObject(S3StorageHelper.buildS3Location("s3://bucket/stage/event/time/c.json"))
            );
            CombinedGroup newCombinedStoredObject = initialCombinedGroup.addNewCombinedObject("node2", storedObjects);
            assertTrue(metadataStore.replaceCombinedGroupManifest(eventPartition, sizeName, initialCombinedGroup, newCombinedStoredObject));

            CombinedGroup persistentCombinedGroup = metadataStore.getCombinedGroupManifest(eventPartition, sizeName);
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
