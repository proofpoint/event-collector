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
import com.proofpoint.units.DataSize;
import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormatter;
import org.joda.time.format.ISODateTimeFormat;
import org.testng.annotations.Test;

import java.net.URI;
import java.util.List;
import java.util.UUID;

import static com.google.common.collect.Lists.newArrayList;
import static com.proofpoint.event.collector.combiner.S3StorageHelper.buildS3Location;
import static com.proofpoint.testing.Assertions.assertGreaterThan;
import static java.lang.String.format;
import static java.util.Arrays.asList;
import static org.joda.time.DateTimeZone.UTC;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;

public class TestStoredObjectCombiner
{
    public static final URI stagingArea = URI.create("s3://bucket/staging/");
    public static final URI targetArea = URI.create("s3://bucket/target/");
    private static final DateTimeFormatter DATE_FORMAT = ISODateTimeFormat.date().withZone(UTC);
    private static final DateTimeFormatter HOUR_FORMAT = ISODateTimeFormat.hour().withZone(UTC);
    private static final int combineDaysAgoStart = 14;
    private static final int combineDaysAgoEnd = -1;

    @Test
    public void testSmall()
            throws Exception
    {
        InMemoryEventClient eventClient = new InMemoryEventClient();
        EventPartition eventPartition = new EventPartition("event", "day", "hour");
        TestingStorageSystem storageSystem = new TestingStorageSystem();
        DateTime date = DateTime.now(UTC);
        URI hourLocation = buildS3Location(stagingArea, "event", DATE_FORMAT.print(date), HOUR_FORMAT.print(date));

        TestingCombineObjectMetadataStore metadataStore = new TestingCombineObjectMetadataStore();
        DataSize targetFileSize = new DataSize(512, DataSize.Unit.MEGABYTE);
        StoredObjectCombiner combiner = new StoredObjectCombiner("nodeId", metadataStore, storageSystem, eventClient, stagingArea, targetArea, targetFileSize, combineDaysAgoStart, combineDaysAgoEnd, "testGroup");

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
        DateTime date = DateTime.now(UTC);
        URI hourLocation = buildS3Location(stagingArea, "event", DATE_FORMAT.print(date), HOUR_FORMAT.print(date));

        TestingCombineObjectMetadataStore metadataStore = new TestingCombineObjectMetadataStore();
        DataSize targetFileSize = new DataSize(512, DataSize.Unit.MEGABYTE);
        StoredObjectCombiner combiner = new StoredObjectCombiner("nodeId", metadataStore, storageSystem, eventClient, stagingArea, targetArea, targetFileSize, combineDaysAgoStart, combineDaysAgoEnd, "testGroup");

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
        DateTime date = DateTime.now(UTC);
        URI hourLocation = buildS3Location(stagingArea, "event", DATE_FORMAT.print(date), HOUR_FORMAT.print(date));
        URI targetLocation = buildS3Location(targetArea, "event", DATE_FORMAT.print(date), HOUR_FORMAT.print(date));

        TestingCombineObjectMetadataStore metadataStore = new TestingCombineObjectMetadataStore();
        DataSize targetFileSize = new DataSize(512, DataSize.Unit.MEGABYTE);
        StoredObjectCombiner combiner = new StoredObjectCombiner("nodeId", metadataStore, storageSystem, eventClient, stagingArea, targetArea, targetFileSize, combineDaysAgoStart, combineDaysAgoEnd, "testGroup");

        // create initial set of objects
        StoredObject objectA = new StoredObject(buildS3Location(hourLocation, "a"), UUID.randomUUID().toString(), 1000, 0);
        StoredObject objectB = new StoredObject(buildS3Location(hourLocation, "b"), UUID.randomUUID().toString(), 1000, 0);
        List<StoredObject> smallGroup = ImmutableList.of(objectA, objectB);
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
    public void testCombineDaysAgoStart()
            throws Exception
    {
        InMemoryEventClient eventClient = new InMemoryEventClient();
        TestingStorageSystem storageSystem = new TestingStorageSystem();

        TestingCombineObjectMetadataStore metadataStore = new TestingCombineObjectMetadataStore();
        DataSize targetFileSize = new DataSize(512, DataSize.Unit.MEGABYTE);

        // create set of objects
        DateTime allowedDate = DateTime.now(UTC).minusDays(combineDaysAgoStart);
        String allowedDatePartition = DATE_FORMAT.print(allowedDate);
        String allowedHourPartition = HOUR_FORMAT.print(allowedDate);
        URI allowedEventAHourLocation = buildS3Location(stagingArea, "eventA", allowedDatePartition, allowedHourPartition);
        StoredObject allowedEventA1 = new StoredObject(buildS3Location(allowedEventAHourLocation, "allowedA1"), randomUUID(), 1000, 0);
        StoredObject allowedEventA2 = new StoredObject(buildS3Location(allowedEventAHourLocation, "allowedA2"), randomUUID(), 1000, 0);
        EventPartition allowedEventAPartition = new EventPartition("eventA", allowedDatePartition, allowedHourPartition);

        URI allowedEventBHourLocation = buildS3Location(stagingArea, "eventB", allowedDatePartition, allowedHourPartition);
        StoredObject allowedEventB1 = new StoredObject(buildS3Location(allowedEventBHourLocation, "allowedB1"), randomUUID(), 1000, 0);
        StoredObject allowedEventB2 = new StoredObject(buildS3Location(allowedEventBHourLocation, "allowedB2"), randomUUID(), 1000, 0);
        EventPartition allowedEventBPartition = new EventPartition("eventB", allowedDatePartition, allowedHourPartition);

        DateTime olderDate = DateTime.now(UTC).minusDays(combineDaysAgoStart + 2);
        URI olderEventAHourLocation = buildS3Location(stagingArea, "eventA", DATE_FORMAT.print(olderDate), HOUR_FORMAT.print(olderDate));
        StoredObject olderEventA1 = new StoredObject(buildS3Location(olderEventAHourLocation, "olderA"), randomUUID(), 1000, 0);
        StoredObject olderEventA2 = new StoredObject(buildS3Location(olderEventAHourLocation, "olderB"), randomUUID(), 1000, 0);
        EventPartition olderEventAPartition = new EventPartition("eventA", DATE_FORMAT.print(olderDate), HOUR_FORMAT.print(olderDate));

        // create single test group
        storageSystem.addObjects(ImmutableList.of(allowedEventA1, allowedEventA2, allowedEventB1, allowedEventB2, olderEventA1, olderEventA2));

        // combine
        StoredObjectCombiner combiner = new StoredObjectCombiner("nodeId", metadataStore, storageSystem, eventClient, stagingArea, targetArea, targetFileSize, combineDaysAgoStart, combineDaysAgoEnd, "testGroup");
        combiner.combineAllObjects();

        checkCombinedEventGroup(metadataStore, allowedEventAPartition, "small", allowedEventA1, allowedEventA2);
        checkCombinedEventGroup(metadataStore, allowedEventBPartition, "small", allowedEventB1, allowedEventB2);
        checkCombinedEventGroup(metadataStore, olderEventAPartition, "small");

        checkEventsGenerated(eventClient, ImmutableList.of(new CombineCompleted("testGroup", "eventA"), new CombineCompleted("testGroup", "eventB")));
    }

    @Test
    public void testCombineDaysAgoEnd()
            throws Exception
    {
        InMemoryEventClient eventClient = new InMemoryEventClient();
        TestingStorageSystem storageSystem = new TestingStorageSystem();

        TestingCombineObjectMetadataStore metadataStore = new TestingCombineObjectMetadataStore();
        DataSize targetFileSize = new DataSize(512, DataSize.Unit.MEGABYTE);

        // create set of objects
        DateTime allowedDate = DateTime.now(UTC).minusDays(combineDaysAgoStart);
        String allowedDatePartition = DATE_FORMAT.print(allowedDate);
        String allowedHourPartition = HOUR_FORMAT.print(allowedDate);
        URI allowedEventAHourLocation = buildS3Location(stagingArea, "eventA", allowedDatePartition, allowedHourPartition);
        StoredObject allowedEventA1 = new StoredObject(buildS3Location(allowedEventAHourLocation, "eventA1"), randomUUID(), 1000, 0);
        StoredObject allowedEventA2 = new StoredObject(buildS3Location(allowedEventAHourLocation, "eventA2"), randomUUID(), 1000, 0);
        EventPartition allowedEventAPartition = new EventPartition("eventA", DATE_FORMAT.print(allowedDate), HOUR_FORMAT.print(allowedDate));

        URI allowedEventBHourLocation = buildS3Location(stagingArea, "eventB", allowedDatePartition, allowedHourPartition);
        StoredObject allowedEventB1 = new StoredObject(buildS3Location(allowedEventBHourLocation, "allowedB1"), randomUUID(), 1000, 0);
        StoredObject allowedEventB2 = new StoredObject(buildS3Location(allowedEventBHourLocation, "allowedB2"), randomUUID(), 1000, 0);
        EventPartition allowedEventBPartition = new EventPartition("eventB", allowedDatePartition, allowedHourPartition);

        DateTime newerDate = DateTime.now(UTC).minusDays(combineDaysAgoEnd).plusDays(1);
        URI newerEventAHourLocation = buildS3Location(stagingArea, "eventA", DATE_FORMAT.print(newerDate), HOUR_FORMAT.print(newerDate));
        StoredObject newerEventA1 = new StoredObject(buildS3Location(newerEventAHourLocation, "c"), randomUUID(), 1000, 0);
        StoredObject newerEventA2 = new StoredObject(buildS3Location(newerEventAHourLocation, "d"), randomUUID(), 1000, 0);
        EventPartition newerEventAPartition = new EventPartition("eventA", DATE_FORMAT.print(newerDate), HOUR_FORMAT.print(newerDate));

        // create single test group
        storageSystem.addObjects(ImmutableList.of(allowedEventA1, allowedEventA2, allowedEventB1, allowedEventB2, newerEventA1, newerEventA2));

        // combine
        StoredObjectCombiner combiner = new StoredObjectCombiner("nodeId", metadataStore, storageSystem, eventClient, stagingArea, targetArea, targetFileSize, combineDaysAgoStart, combineDaysAgoEnd, "testGroup");
        combiner.combineAllObjects();

        checkCombinedEventGroup(metadataStore, allowedEventAPartition, "small", allowedEventA1, allowedEventA2);
        checkCombinedEventGroup(metadataStore, allowedEventBPartition, "small", allowedEventB1, allowedEventB2);
        checkCombinedEventGroup(metadataStore, newerEventAPartition, "small");

        checkEventsGenerated(eventClient, ImmutableList.of(new CombineCompleted("testGroup", "eventA"), new CombineCompleted("testGroup", "eventB")));
    }

    @Test(expectedExceptions = IllegalArgumentException.class, expectedExceptionsMessageRegExp = "startDaysAgo must be greater than endDaysAgo")
    public void testBadDateRanges()
    {
        InMemoryEventClient eventClient = new InMemoryEventClient();
        TestingStorageSystem storageSystem = new TestingStorageSystem();

        DateTime allowedDate = DateTime.now(UTC).minusDays(2);
        URI allowedHourLocation = buildS3Location(stagingArea, "event", DATE_FORMAT.print(allowedDate), HOUR_FORMAT.print(allowedDate));
        StoredObject objectA = new StoredObject(buildS3Location(allowedHourLocation, "a"), randomUUID(), 1000, 0);
        storageSystem.addObjects(ImmutableList.of(objectA));

        TestingCombineObjectMetadataStore metadataStore = new TestingCombineObjectMetadataStore();
        DataSize targetFileSize = new DataSize(512, DataSize.Unit.MEGABYTE);

        new StoredObjectCombiner("nodeId", metadataStore, storageSystem, eventClient, stagingArea, targetArea, targetFileSize, 1, 2, "testGroup");
    }

    @Test
    public void testCombineObjectsByEventType()
    {
        InMemoryEventClient eventClient = new InMemoryEventClient();
        TestingStorageSystem storageSystem = new TestingStorageSystem();
        TestingCombineObjectMetadataStore metadataStore = new TestingCombineObjectMetadataStore();
        DataSize targetFileSize = new DataSize(512, DataSize.Unit.MEGABYTE);

        DateTime allowedDate = DateTime.now(UTC).minusDays(combineDaysAgoStart);
        String allowedDatePartition = DATE_FORMAT.print(allowedDate);
        String allowedHourPartition = HOUR_FORMAT.print(allowedDate);
        URI allowedEventAHourLocation = buildS3Location(stagingArea, "eventA", allowedDatePartition, allowedHourPartition);
        StoredObject allowedEventA1 = new StoredObject(buildS3Location(allowedEventAHourLocation, "eventA1"), randomUUID(), 1000, 0);
        StoredObject allowedEventA2 = new StoredObject(buildS3Location(allowedEventAHourLocation, "eventA2"), randomUUID(), 1000, 0);
        EventPartition allowedEventAPartition = new EventPartition("eventA", DATE_FORMAT.print(allowedDate), HOUR_FORMAT.print(allowedDate));

        storageSystem.addObjects(ImmutableList.of(allowedEventA1, allowedEventA2));
        StoredObjectCombiner combiner = new StoredObjectCombiner("nodeId", metadataStore, storageSystem, eventClient, stagingArea, targetArea, targetFileSize, combineDaysAgoStart, combineDaysAgoEnd, "testGroup");
        combiner.combineObjects("eventA");

        checkCombinedEventGroup(metadataStore, allowedEventAPartition, "small", allowedEventA1, allowedEventA2);
        checkEventsGenerated(eventClient, ImmutableList.of(new CombineCompleted("testGroup", "eventA")));
    }

    private void checkCombinedEventGroup(CombineObjectMetadataStore metadataStore, EventPartition eventPartition, String size, StoredObject... events)
    {
        if (events.length == 0) {
            assertNull(metadataStore.getCombinedGroupManifest(eventPartition, size));
        }
        else {
            // validate manifest
            CombinedGroup combinedGroup = metadataStore.getCombinedGroupManifest(eventPartition, size);
            assertNotNull(combinedGroup);
            assertGreaterThan(combinedGroup.getVersion(), 0L);

            // validate only objects newer than combineDaysAgoStart get processed
            List<CombinedStoredObject> combinedObjects = combinedGroup.getCombinedObjects();
            assertEquals(combinedObjects.size(), 1);
            assertEquals(combinedObjects.get(0).getSourceParts(), ImmutableList.copyOf(events));
        }
    }

    private void checkEventsGenerated(InMemoryEventClient eventClient, List<CombineCompleted> expectedEvents)
    {
        List<?> actualEvents = eventClient.getEvents();

        assertEquals(actualEvents.size(), expectedEvents.size());
        for (int i = 0; i < actualEvents.size(); ++i) {
            CombineCompleted actualEvent = (CombineCompleted) actualEvents.get(i);
            CombineCompleted expectedEvent = expectedEvents.get(i);

            assertEquals(actualEvent.getGroupId(), expectedEvent.getGroupId(), format("element %d", i));
            assertEquals(actualEvent.getEventType(), expectedEvent.getEventType(), format("element %d", i));
        }
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
