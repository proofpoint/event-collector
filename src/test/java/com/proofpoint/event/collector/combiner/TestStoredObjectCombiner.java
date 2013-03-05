/*
 * Copyright 2011-2013 Proofpoint, Inc.
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
package com.proofpoint.event.collector.combiner;

import com.google.common.collect.ImmutableList;
import com.proofpoint.event.client.InMemoryEventClient;
import com.proofpoint.event.collector.EventPartition;
import com.proofpoint.experimental.units.DataSize;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.format.DateTimeFormatter;
import org.joda.time.format.ISODateTimeFormat;
import org.testng.annotations.Test;

import java.net.URI;
import java.util.List;
import java.util.UUID;

import static com.google.common.collect.Lists.newArrayList;
import static com.proofpoint.event.collector.combiner.S3StorageHelper.buildS3Location;
import static com.proofpoint.testing.Assertions.assertGreaterThan;
import static java.util.Arrays.asList;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;

public class TestStoredObjectCombiner
{
    public static final URI stagingArea = URI.create("s3://bucket/staging/");
    public static final URI targetArea = URI.create("s3://bucket/target/");
    private static final DateTimeFormatter DATE_FORMAT = ISODateTimeFormat.date().withZone(DateTimeZone.UTC);
    private static final DateTimeFormatter HOUR_FORMAT = ISODateTimeFormat.hour().withZone(DateTimeZone.UTC);
    private static final int combineDaysAgoStart = 14;
    private static final int combineDaysAgoEnd = -1;

    @Test
    public void testSmall()
            throws Exception
    {
        InMemoryEventClient eventClient = new InMemoryEventClient();
        EventPartition eventPartition = new EventPartition("event", "day", "hour");
        TestingStorageSystem storageSystem = new TestingStorageSystem();
        DateTime date = new DateTime();
        URI hourLocation = buildS3Location(stagingArea, "event", DATE_FORMAT.print(date), HOUR_FORMAT.print(date));

        TestingCombineObjectMetadataStore metadataStore = new TestingCombineObjectMetadataStore();
        DataSize targetFileSize = new DataSize(512, DataSize.Unit.MEGABYTE);
        StoredObjectCombiner combiner = new StoredObjectCombiner("nodeId", metadataStore, storageSystem, eventClient, stagingArea, targetArea, targetFileSize, combineDaysAgoStart, combineDaysAgoEnd);

        // create initial set of objects
        StoredObject objectA = new StoredObject(buildS3Location(hourLocation, "a"), UUID.randomUUID().toString(), 1000, 0);
        StoredObject objectB = new StoredObject(buildS3Location(hourLocation, "b"), UUID.randomUUID().toString(), 1000, 0);

        // create single test group
        List<StoredObject> smallGroup = newArrayList(objectA, objectB);
        storageSystem.addObjects(smallGroup);

        // combine initial set
        combiner.combineObjects(eventPartition, hourLocation, smallGroup);

        // validate manifest
        CombinedGroup combinedGroup = metadataStore.getCombinedGroupManifest(eventPartition, "small");
        assertGreaterThan(combinedGroup.getVersion(), 0L);

        // validate objects
        List<CombinedStoredObject> combinedObjects = combinedGroup.getCombinedObjects();
        assertEquals(combinedObjects.size(), 1);
        assertEquals(combinedObjects.get(0).getSourceParts(), smallGroup);

        // add more objects
        StoredObject objectC = new StoredObject(buildS3Location(hourLocation, "c"), UUID.randomUUID().toString(), 1000, 0);
        StoredObject objectD = new StoredObject(buildS3Location(hourLocation, "d"), UUID.randomUUID().toString(), 1000, 0);
        smallGroup.addAll(asList(objectC, objectD));
        storageSystem.addObjects(smallGroup);

        // combine updated set
        combiner.combineObjects(eventPartition, hourLocation, smallGroup);

        // validate manifest
        CombinedGroup updatedCombinedGroup = metadataStore.getCombinedGroupManifest(eventPartition, "small");
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
        InMemoryEventClient eventClient = new InMemoryEventClient();
        EventPartition eventPartition = new EventPartition("event", "day", "hour");
        TestingStorageSystem storageSystem = new TestingStorageSystem();
        DateTime date = new DateTime();
        URI hourLocation = buildS3Location(stagingArea, "event", DATE_FORMAT.print(date), HOUR_FORMAT.print(date));

        TestingCombineObjectMetadataStore metadataStore = new TestingCombineObjectMetadataStore();
        DataSize targetFileSize = new DataSize(512, DataSize.Unit.MEGABYTE);
        StoredObjectCombiner combiner = new StoredObjectCombiner("nodeId", metadataStore, storageSystem, eventClient, stagingArea, targetArea, targetFileSize, combineDaysAgoStart, combineDaysAgoEnd);

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
        storageSystem.addObjects(storedObjects);

        // combine initial set
        combiner.combineObjects(eventPartition, hourLocation, storedObjects);

        // validate manifest
        CombinedGroup combinedGroup = metadataStore.getCombinedGroupManifest(eventPartition, "large");
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
        storageSystem.addObjects(storedObjects);

        // combine updated set
        combiner.combineObjects(eventPartition, hourLocation, storedObjects);

        // validate manifest
        CombinedGroup updatedCombinedGroup = metadataStore.getCombinedGroupManifest(eventPartition, "large");
        assertGreaterThan(updatedCombinedGroup.getVersion(), combinedGroup.getVersion());

        // validate groups
        combinedObjects = updatedCombinedGroup.getCombinedObjects();
        assertEquals(combinedObjects.size(), 4);
        assertEquals(combinedObjects.get(0).getSourceParts(), group1);
        assertEquals(combinedObjects.get(1).getSourceParts(), group2);
        assertEquals(combinedObjects.get(2).getSourceParts(), group3);
        assertEquals(combinedObjects.get(3).getSourceParts(), group4);
    }

    @Test
    public void testMissingSourceFiles()
    {
        InMemoryEventClient eventClient = new InMemoryEventClient();
        EventPartition eventPartition = new EventPartition("event", "day", "hour");
        TestingStorageSystem storageSystem = new TestingStorageSystem();
        DateTime date = new DateTime();
        URI hourLocation = buildS3Location(stagingArea, "event", DATE_FORMAT.print(date), HOUR_FORMAT.print(date));
        URI targetLocation = buildS3Location(targetArea, "event", DATE_FORMAT.print(date), HOUR_FORMAT.print(date));

        TestingCombineObjectMetadataStore metadataStore = new TestingCombineObjectMetadataStore();
        DataSize targetFileSize = new DataSize(512, DataSize.Unit.MEGABYTE);
        StoredObjectCombiner combiner = new StoredObjectCombiner("nodeId", metadataStore, storageSystem, eventClient, stagingArea, targetArea, targetFileSize, combineDaysAgoStart, combineDaysAgoEnd);

        // create initial set of objects
        StoredObject objectA = new StoredObject(buildS3Location(hourLocation, "a"), UUID.randomUUID().toString(), 1000, 0);
        StoredObject objectB = new StoredObject(buildS3Location(hourLocation, "b"), UUID.randomUUID().toString(), 1000, 0);
        List<StoredObject> smallGroup = newArrayList(objectA, objectB);
        storageSystem.addObjects(smallGroup);

        // combine initial set
        combiner.combineObjects(eventPartition, targetLocation, smallGroup);

        // validate manifest
        CombinedGroup combinedGroup = metadataStore.getCombinedGroupManifest(eventPartition, "small");
        assertGreaterThan(combinedGroup.getVersion(), 0L);

        // remove one of the source files
        assertTrue(storageSystem.removeObject(objectA.getLocation()));

        // remove target combined object to force recombine
        StoredObject combinedObject = new StoredObject(combinedGroup.getCombinedObjects().get(0).getLocation());
        assertTrue(storageSystem.removeObject(combinedObject.getLocation()));

        // combine again
        combiner.combineObjects(eventPartition, targetLocation, storageSystem.listObjects(targetLocation));
    }

    @Test
    public void testComineDaysAgoStart()
            throws Exception
    {
        InMemoryEventClient eventClient = new InMemoryEventClient();
        TestingStorageSystem storageSystem = new TestingStorageSystem();

        TestingCombineObjectMetadataStore metadataStore = new TestingCombineObjectMetadataStore();
        DataSize targetFileSize = new DataSize(512, DataSize.Unit.MEGABYTE);

        // create set of objects
        DateTime allowedDate = new DateTime().minusDays(2);
        URI allowedHourLocation = buildS3Location(stagingArea, "event", DATE_FORMAT.print(allowedDate), HOUR_FORMAT.print(allowedDate));
        StoredObject objectA = new StoredObject(buildS3Location(allowedHourLocation, "a"), randomUUID(), 1000, 0);
        StoredObject objectB = new StoredObject(buildS3Location(allowedHourLocation, "b"), randomUUID(), 1000, 0);
        EventPartition allowedEventPartition = new EventPartition("event", DATE_FORMAT.print(allowedDate), HOUR_FORMAT.print(allowedDate));

        DateTime olderDate = new DateTime().minusDays(combineDaysAgoStart + 2);
        URI olderHourLocation = buildS3Location(stagingArea, "event", DATE_FORMAT.print(olderDate), HOUR_FORMAT.print(olderDate));
        StoredObject objectC = new StoredObject(buildS3Location(olderHourLocation, "c"), randomUUID(), 1000, 0);
        StoredObject objectD = new StoredObject(buildS3Location(olderHourLocation, "d"), randomUUID(), 1000, 0);
        EventPartition olderEventPartition = new EventPartition("event", DATE_FORMAT.print(olderDate), HOUR_FORMAT.print(olderDate));

        // create single test group
        List<StoredObject> group = newArrayList(objectA, objectB, objectC, objectD);
        storageSystem.addObjects(group);

        // combine
        StoredObjectCombiner combiner = new StoredObjectCombiner("nodeId", metadataStore, storageSystem, eventClient, stagingArea, targetArea, targetFileSize, combineDaysAgoStart, combineDaysAgoEnd);
        combiner.combineAllObjects();

        // validate manifest
        CombinedGroup combinedGroup = metadataStore.getCombinedGroupManifest(allowedEventPartition, "small");
        assertGreaterThan(combinedGroup.getVersion(), 0L);

        // validate only objects newer than combineDaysAgoStart get processed
        List<CombinedStoredObject> combinedObjects = combinedGroup.getCombinedObjects();
        assertEquals(combinedObjects.size(), 1);
        assertEquals(combinedObjects.get(0).getSourceParts(), newArrayList(objectA, objectB));

        assertNull(metadataStore.getCombinedGroupManifest(olderEventPartition, "small"));
    }

    public void testCombineDaysAgoEnd()
            throws Exception
    {
        InMemoryEventClient eventClient = new InMemoryEventClient();
        TestingStorageSystem storageSystem = new TestingStorageSystem();

        TestingCombineObjectMetadataStore metadataStore = new TestingCombineObjectMetadataStore();
        DataSize targetFileSize = new DataSize(512, DataSize.Unit.MEGABYTE);

        // create set of objects
        DateTime allowedDate = new DateTime().minusDays(2);
        URI allowedHourLocation = buildS3Location(stagingArea, "event", DATE_FORMAT.print(allowedDate), HOUR_FORMAT.print(allowedDate));
        StoredObject objectA = new StoredObject(buildS3Location(allowedHourLocation, "a"), randomUUID(), 1000, 0);
        StoredObject objectB = new StoredObject(buildS3Location(allowedHourLocation, "b"), randomUUID(), 1000, 0);
        EventPartition allowedEventPartition = new EventPartition("event", DATE_FORMAT.print(allowedDate), HOUR_FORMAT.print(allowedDate));

        DateTime olderDate = new DateTime().plusDays(combineDaysAgoEnd + 1);
        URI olderHourLocation = buildS3Location(stagingArea, "event", DATE_FORMAT.print(olderDate), HOUR_FORMAT.print(olderDate));
        StoredObject objectC = new StoredObject(buildS3Location(olderHourLocation, "c"), randomUUID(), 1000, 0);
        StoredObject objectD = new StoredObject(buildS3Location(olderHourLocation, "d"), randomUUID(), 1000, 0);
        EventPartition olderEventPartition = new EventPartition("event", DATE_FORMAT.print(olderDate), HOUR_FORMAT.print(olderDate));

        // create single test group
        List<StoredObject> group = newArrayList(objectA, objectB, objectC, objectD);
        storageSystem.addObjects(group);

        // combine
        StoredObjectCombiner combiner = new StoredObjectCombiner("nodeId", metadataStore, storageSystem, eventClient, stagingArea, targetArea, targetFileSize, combineDaysAgoStart, combineDaysAgoEnd);
        combiner.combineAllObjects();

        // validate manifest
        CombinedGroup combinedGroup = metadataStore.getCombinedGroupManifest(allowedEventPartition, "small");
        assertGreaterThan(combinedGroup.getVersion(), 0L);

        // validate only objects older than combineDaysAgoEnd get processed
        List<CombinedStoredObject> combinedObjects = combinedGroup.getCombinedObjects();
        assertEquals(combinedObjects.size(), 1);
        assertEquals(combinedObjects.get(0).getSourceParts(), newArrayList(objectA, objectB));

        assertNull(metadataStore.getCombinedGroupManifest(olderEventPartition, "small"));
    }

    @Test(expectedExceptions = IllegalArgumentException.class)
    public void testBadDateRanges()
    {
        InMemoryEventClient eventClient = new InMemoryEventClient();
        TestingStorageSystem storageSystem = new TestingStorageSystem();

        DateTime allowedDate = new DateTime().minusDays(2);
        URI allowedHourLocation = buildS3Location(stagingArea, "event", DATE_FORMAT.print(allowedDate), HOUR_FORMAT.print(allowedDate));
        StoredObject objectA = new StoredObject(buildS3Location(allowedHourLocation, "a"), randomUUID(), 1000, 0);
        storageSystem.addObjects(ImmutableList.of(objectA));

        TestingCombineObjectMetadataStore metadataStore = new TestingCombineObjectMetadataStore();
        DataSize targetFileSize = new DataSize(512, DataSize.Unit.MEGABYTE);

        StoredObjectCombiner combiner = new StoredObjectCombiner("nodeId", metadataStore, storageSystem, eventClient, stagingArea, targetArea, targetFileSize, 1, 2);
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
