package com.proofpoint.collector.calligraphus.combiner;

import com.google.common.collect.ImmutableList;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.net.URI;
import java.util.UUID;

import static com.proofpoint.collector.calligraphus.combiner.S3StorageHelper.buildS3Location;

public class TestStoredObjectCombiner
{
    @Test
    public void test()
            throws Exception
    {
        URI stagingArea = URI.create("s3://bucket/staging");
        URI targetArea = URI.create("s3://bucket/target");

        TestingStorageSystem storageSystem = new TestingStorageSystem();
        StoredObject a = new StoredObject(buildS3Location(stagingArea, "a"), UUID.randomUUID().toString(), 1000, 0);
        storageSystem.addObject(stagingArea, a);
        StoredObject b = new StoredObject(buildS3Location(stagingArea, "b"), UUID.randomUUID().toString(), 1000, 0);
        storageSystem.addObject(stagingArea, b);

        TestingCombineObjectMetadataStore metadataStore = new TestingCombineObjectMetadataStore("test");
        StoredObjectCombiner objectCombiner = new StoredObjectCombiner("nodeId", metadataStore, storageSystem, stagingArea, targetArea);
        objectCombiner.combineObjects(targetArea, ImmutableList.<StoredObject>of(a, b));

        CombinedStoredObject combinedObjectManifest = metadataStore.getCombinedObjectManifest(targetArea);
        Assert.assertEquals(combinedObjectManifest.getSourceParts(), ImmutableList.<Object>of(a, b));
    }
}
