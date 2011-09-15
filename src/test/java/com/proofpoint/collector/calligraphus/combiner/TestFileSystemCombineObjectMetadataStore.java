package com.proofpoint.collector.calligraphus.combiner;

import com.google.common.collect.ImmutableList;
import com.google.common.io.Files;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.io.File;
import java.net.URI;

public class TestFileSystemCombineObjectMetadataStore
{
    @Test
    public void testInitialState()
            throws Exception
    {
        FileSystemCombineObjectMetadataStore metadataStore = new FileSystemCombineObjectMetadataStore("test-node", new File("unused"));
        URI location = S3StorageHelper.buildS3Location("s3://bucket/event/time/file.json");

        CombinedStoredObject initialCombinedStoredObject = metadataStore.getCombinedObjectManifest(location);
        Assert.assertNotNull(initialCombinedStoredObject);
        Assert.assertEquals(initialCombinedStoredObject.getVersion(), 0);
        Assert.assertEquals(initialCombinedStoredObject.getLocation(), location);
        Assert.assertEquals(initialCombinedStoredObject.getCreator(), "test-node");
        Assert.assertTrue(initialCombinedStoredObject.getSourceParts().isEmpty());
    }

    @Test
    public void test()
            throws Exception
    {

        File tempDir = Files.createTempDir();
        try {
            FileSystemCombineObjectMetadataStore metadataStore = new FileSystemCombineObjectMetadataStore("test-node", tempDir);
            URI location = S3StorageHelper.buildS3Location("s3://bucket/event/time/file.json");

            CombinedStoredObject initialCombinedStoredObject = metadataStore.getCombinedObjectManifest(location);
            ImmutableList<StoredObject> storedObjects = ImmutableList.of(
                    new StoredObject(S3StorageHelper.buildS3Location("s3://bucket/stage/event/time/a.json")),
                    new StoredObject(S3StorageHelper.buildS3Location("s3://bucket/stage/event/time/b.json")),
                    new StoredObject(S3StorageHelper.buildS3Location("s3://bucket/stage/event/time/c.json"))
            );
            CombinedStoredObject newCombinedStoredObject = initialCombinedStoredObject.update("node2", storedObjects);
            Assert.assertTrue(metadataStore.replaceCombinedObjectManifest(initialCombinedStoredObject, newCombinedStoredObject));

            CombinedStoredObject persistentCombinedStoredObject = metadataStore.getCombinedObjectManifest(location);
            Assert.assertNotNull(persistentCombinedStoredObject);
            Assert.assertEquals(persistentCombinedStoredObject.getVersion(), 1);
            Assert.assertEquals(persistentCombinedStoredObject.getLocation(), location);
            Assert.assertEquals(persistentCombinedStoredObject.getCreator(), "node2");
            Assert.assertEquals(persistentCombinedStoredObject.getSourceParts(), storedObjects);
        }
        finally {
            // todo delete temp dir
        }
    }
}
