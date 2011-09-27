package com.proofpoint.collector.calligraphus.combiner;

import com.google.common.collect.ImmutableList;
import com.proofpoint.experimental.units.DataSize;
import org.testng.annotations.Test;

import java.net.URI;
import java.util.List;
import java.util.UUID;

import static com.proofpoint.collector.calligraphus.combiner.S3StorageHelper.appendSuffix;
import static com.proofpoint.collector.calligraphus.combiner.S3StorageHelper.buildS3Location;
import static com.proofpoint.testing.Assertions.assertGreaterThan;
import static org.testng.Assert.assertEquals;

public class TestStoredObjectCombiner
{
    public static final URI stagingArea = URI.create("s3://bucket/staging/");
    public static final URI targetArea = URI.create("s3://bucket/target/");

    @Test
    public void testSmall()
            throws Exception
    {
        TestingStorageSystem storageSystem = new TestingStorageSystem();
        URI hourLocation = buildS3Location(stagingArea, "event", "day", "hour");

        StoredObject a = new StoredObject(buildS3Location(hourLocation, "a"), UUID.randomUUID().toString(), 1000, 0);
        StoredObject b = new StoredObject(buildS3Location(hourLocation, "b"), UUID.randomUUID().toString(), 1000, 0);

        ImmutableList<StoredObject> smallGroup = ImmutableList.of(a, b);
        for (StoredObject object : smallGroup) {
            storageSystem.addObject(stagingArea, object);
        }

        TestingCombineObjectMetadataStore metadataStore = new TestingCombineObjectMetadataStore("nodeId");
        DataSize targetFileSize = new DataSize(512, DataSize.Unit.MEGABYTE);
        StoredObjectCombiner combiner = new StoredObjectCombiner("nodeId", metadataStore, storageSystem, stagingArea, targetArea, targetFileSize, true);

        combiner.combineObjects(hourLocation, smallGroup);

        URI groupPrefix = appendSuffix(hourLocation, "small");
        CombinedGroup combinedGroup = metadataStore.getCombinedGroupManifest(groupPrefix);
        assertGreaterThan(combinedGroup.getVersion(), 0L);

        List<CombinedStoredObject> combinedObjects = combinedGroup.getCombinedObjects();
        assertEquals(combinedObjects.size(), 1);
        assertEquals(combinedObjects.get(0).getSourceParts(), smallGroup);
    }

    @Test
    public void testSmallLarge()
            throws Exception
    {
        TestingStorageSystem storageSystem = new TestingStorageSystem();
        URI dayLocation = buildS3Location(stagingArea, "event", "day");
        URI hourLocation = buildS3Location(dayLocation, "hour");

        StoredObject objectA = new StoredObject(buildS3Location(hourLocation, "a"), randomUUID(), megabytes(400), 0);
        StoredObject objectB = new StoredObject(buildS3Location(hourLocation, "b"), randomUUID(), megabytes(200), 0);
        StoredObject objectC = new StoredObject(buildS3Location(hourLocation, "c"), randomUUID(), megabytes(200), 0);
        StoredObject objectD = new StoredObject(buildS3Location(hourLocation, "d"), randomUUID(), megabytes(200), 0);
        StoredObject objectE = new StoredObject(buildS3Location(hourLocation, "e"), randomUUID(), megabytes(300), 0);
        StoredObject objectF = new StoredObject(buildS3Location(hourLocation, "f"), randomUUID(), megabytes(100), 0);

        List<StoredObject> group1 = ImmutableList.of(objectA, objectB);
        List<StoredObject> group2 = ImmutableList.of(objectC, objectD, objectE);
        List<StoredObject> group3 = ImmutableList.of(objectF);

        List<StoredObject> storedObjects = ImmutableList.of(objectA, objectB, objectC, objectD, objectE, objectF);
        for (StoredObject object : storedObjects) {
            storageSystem.addObject(stagingArea, object);
        }

        TestingCombineObjectMetadataStore metadataStore = new TestingCombineObjectMetadataStore("nodeId");
        DataSize targetFileSize = new DataSize(512, DataSize.Unit.MEGABYTE);
        StoredObjectCombiner combiner = new StoredObjectCombiner("nodeId", metadataStore, storageSystem, stagingArea, targetArea, targetFileSize, true);

        combiner.combineObjects(hourLocation, storedObjects);

        URI groupPrefix = appendSuffix(hourLocation, "large");
        CombinedGroup combinedGroup = metadataStore.getCombinedGroupManifest(groupPrefix);
        assertGreaterThan(combinedGroup.getVersion(), 0L);

        List<CombinedStoredObject> combinedObjects = combinedGroup.getCombinedObjects();
        assertEquals(combinedObjects.size(), 3);
        assertEquals(combinedObjects.get(0).getSourceParts(), group1);
        assertEquals(combinedObjects.get(1).getSourceParts(), group2);
        assertEquals(combinedObjects.get(2).getSourceParts(), group3);
    }

    private static String randomUUID()
    {
        return UUID.randomUUID().toString();
    }

    private static long megabytes(int megabytes)
    {
        return new DataSize(megabytes, DataSize.Unit.MEGABYTE).toBytes();
    }
}
