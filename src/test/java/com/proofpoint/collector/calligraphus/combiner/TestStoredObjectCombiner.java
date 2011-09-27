package com.proofpoint.collector.calligraphus.combiner;

import com.proofpoint.experimental.units.DataSize;
import org.testng.annotations.Test;

import java.net.URI;
import java.util.List;
import java.util.UUID;

import static com.google.common.collect.Lists.newArrayList;
import static com.proofpoint.collector.calligraphus.combiner.S3StorageHelper.appendSuffix;
import static com.proofpoint.collector.calligraphus.combiner.S3StorageHelper.buildS3Location;
import static com.proofpoint.testing.Assertions.assertGreaterThan;
import static java.util.Arrays.asList;
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

        TestingCombineObjectMetadataStore metadataStore = new TestingCombineObjectMetadataStore("nodeId");
        DataSize targetFileSize = new DataSize(512, DataSize.Unit.MEGABYTE);
        StoredObjectCombiner combiner = new StoredObjectCombiner("nodeId", metadataStore, storageSystem, stagingArea, targetArea, targetFileSize, true);

        // create initial set of objects
        StoredObject objectA = new StoredObject(buildS3Location(hourLocation, "a"), UUID.randomUUID().toString(), 1000, 0);
        StoredObject objectB = new StoredObject(buildS3Location(hourLocation, "b"), UUID.randomUUID().toString(), 1000, 0);
        List<StoredObject> smallGroup = newArrayList(objectA, objectB);
        storageSystem.addObjects(stagingArea, smallGroup);

        // combine initial set
        combiner.combineObjects(hourLocation, smallGroup);

        // validate manifest
        URI groupPrefix = appendSuffix(hourLocation, "small");
        CombinedGroup combinedGroup = metadataStore.getCombinedGroupManifest(groupPrefix);
        assertGreaterThan(combinedGroup.getVersion(), 0L);

        // validate objects
        List<CombinedStoredObject> combinedObjects = combinedGroup.getCombinedObjects();
        assertEquals(combinedObjects.size(), 1);
        assertEquals(combinedObjects.get(0).getSourceParts(), smallGroup);

        // add more objects
        StoredObject objectC = new StoredObject(buildS3Location(hourLocation, "c"), UUID.randomUUID().toString(), 1000, 0);
        StoredObject objectD = new StoredObject(buildS3Location(hourLocation, "d"), UUID.randomUUID().toString(), 1000, 0);
        smallGroup.addAll(asList(objectC, objectD));
        storageSystem.addObjects(stagingArea, smallGroup);

        // combine updated set
        combiner.combineObjects(hourLocation, smallGroup);

        // validate manifest
        CombinedGroup updatedCombinedGroup = metadataStore.getCombinedGroupManifest(groupPrefix);
        assertGreaterThan(updatedCombinedGroup.getVersion(), combinedGroup.getVersion());

        // validate objects
        combinedObjects = updatedCombinedGroup.getCombinedObjects();
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

        TestingCombineObjectMetadataStore metadataStore = new TestingCombineObjectMetadataStore("nodeId");
        DataSize targetFileSize = new DataSize(512, DataSize.Unit.MEGABYTE);
        StoredObjectCombiner combiner = new StoredObjectCombiner("nodeId", metadataStore, storageSystem, stagingArea, targetArea, targetFileSize, true);

        // create initial set of objects
        StoredObject objectA = new StoredObject(buildS3Location(hourLocation, "a"), randomUUID(), megabytes(400), 0);
        StoredObject objectB = new StoredObject(buildS3Location(hourLocation, "b"), randomUUID(), megabytes(200), 0);
        StoredObject objectC = new StoredObject(buildS3Location(hourLocation, "c"), randomUUID(), megabytes(200), 0);
        StoredObject objectD = new StoredObject(buildS3Location(hourLocation, "d"), randomUUID(), megabytes(200), 0);
        StoredObject objectE = new StoredObject(buildS3Location(hourLocation, "e"), randomUUID(), megabytes(300), 0);
        StoredObject objectF = new StoredObject(buildS3Location(hourLocation, "f"), randomUUID(), megabytes(100), 0);

        // create test groups based on object size
        List<StoredObject> group1 = newArrayList(objectA, objectB);
        List<StoredObject> group2 = newArrayList(objectC, objectD, objectE);
        List<StoredObject> group3 = newArrayList(objectF);

        List<StoredObject> storedObjects = newArrayList(objectA, objectB, objectC, objectD, objectE, objectF);
        storageSystem.addObjects(stagingArea, storedObjects);

        // combine initial set
        combiner.combineObjects(hourLocation, storedObjects);

        // validate manifest
        URI groupPrefix = appendSuffix(hourLocation, "large");
        CombinedGroup combinedGroup = metadataStore.getCombinedGroupManifest(groupPrefix);
        assertGreaterThan(combinedGroup.getVersion(), 0L);

        // validate groups
        List<CombinedStoredObject> combinedObjects = combinedGroup.getCombinedObjects();
        assertEquals(combinedObjects.size(), 3);
        assertEquals(combinedObjects.get(0).getSourceParts(), group1);
        assertEquals(combinedObjects.get(1).getSourceParts(), group2);
        assertEquals(combinedObjects.get(2).getSourceParts(), group3);

        // add more objects
        StoredObject objectG = new StoredObject(buildS3Location(hourLocation, "g"), randomUUID(), megabytes(500), 0);
        StoredObject objectH = new StoredObject(buildS3Location(hourLocation, "h"), randomUUID(), megabytes(200), 0);

        // update groups
        group3.add(objectG);
        List<StoredObject> group4 = newArrayList(objectH);

        storedObjects.addAll(asList(objectG, objectH));
        storageSystem.addObjects(stagingArea, storedObjects);

        // combine updated set
        combiner.combineObjects(hourLocation, storedObjects);

        // validate manifest
        CombinedGroup updatedCombinedGroup = metadataStore.getCombinedGroupManifest(groupPrefix);
        assertGreaterThan(updatedCombinedGroup.getVersion(), combinedGroup.getVersion());

        // validate groups
        combinedObjects = updatedCombinedGroup.getCombinedObjects();
        assertEquals(combinedObjects.size(), 4);
        assertEquals(combinedObjects.get(0).getSourceParts(), group1);
        assertEquals(combinedObjects.get(1).getSourceParts(), group2);
        assertEquals(combinedObjects.get(2).getSourceParts(), group3);
        assertEquals(combinedObjects.get(3).getSourceParts(), group4);
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
