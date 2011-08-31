package com.proofpoint.collector.calligraphus.combiner;

import com.google.common.collect.ImmutableList;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.net.URI;
import java.util.UUID;

public class TestStoredObjectCombiner
{
    @Test
    public void test()
            throws Exception
    {
        URI stagingArea = URI.create("s3://bucket/staging");
        URI targetArea = URI.create("s3://bucket/target");

        TestingStorageSystem storageSystem = new TestingStorageSystem();
        StoredObject a = new StoredObject("a", stagingArea, UUID.randomUUID().toString(), 1000, 0);
        storageSystem.addObject(stagingArea, a);
        StoredObject b = new StoredObject("b", stagingArea, UUID.randomUUID().toString(), 1000, 0);
        storageSystem.addObject(stagingArea, b);

        TestingCombineObjectMetadataStore metadataStore = new TestingCombineObjectMetadataStore("test");
        StoredObjectCombiner objectCombiner = new StoredObjectCombiner("nodeId", metadataStore, storageSystem);
        objectCombiner.combineObjects(stagingArea, targetArea);

        CombinedStoredObject combinedObjectManifest = metadataStore.getCombinedObjectManifest(stagingArea, targetArea);
        Assert.assertEquals(combinedObjectManifest.getSourceParts(), ImmutableList.<Object>of(a, b));
    }
}
