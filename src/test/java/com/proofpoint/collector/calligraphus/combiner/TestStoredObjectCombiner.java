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
        URI stagingArea = URI.create("s3://bucket/staging/");
        URI targetArea = URI.create("s3://bucket/target/");

        TestingStorageSystem storageSystem = new TestingStorageSystem();
        StoredObject a = new StoredObject(buildS3Location(stagingArea, "event", "day", "hour", "a"), UUID.randomUUID().toString(), 1000, 0);
        storageSystem.addObject(stagingArea, a);
        StoredObject b = new StoredObject(buildS3Location(stagingArea, "event", "day", "hour", "b"), UUID.randomUUID().toString(), 1000, 0);
        storageSystem.addObject(stagingArea, b);

        TestingCombineObjectMetadataStore metadataStore = new TestingCombineObjectMetadataStore("nodeId");
        StoredObjectCombiner objectCombiner = new StoredObjectCombiner("nodeId", metadataStore, storageSystem, stagingArea, targetArea, true);
        objectCombiner.combineObjects(buildS3Location(stagingArea, "event", "day", "hour"), ImmutableList.<StoredObject>of(a, b));

        URI targetUri = buildS3Location(stagingArea, "event", "day", "hour.small.json.snappy");
        CombinedStoredObject combinedObjectManifest = metadataStore.getCombinedObjectManifest(targetUri);
        Assert.assertEquals(combinedObjectManifest.getSourceParts(), ImmutableList.<Object>of(a, b));
    }
}
