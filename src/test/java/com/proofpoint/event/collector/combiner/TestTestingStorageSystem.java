/*
 * Copyright 2011-2014 Proofpoint, Inc.
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
import org.testng.annotations.Test;

import java.net.URI;
import java.util.UUID;

import static com.proofpoint.event.collector.combiner.S3StorageHelper.buildS3Location;
import static org.testng.Assert.assertEquals;

public class TestTestingStorageSystem
{
    private static final URI stagingArea = URI.create("s3://bucket/staging/");
    private static final URI eventLocation = URI.create(stagingArea + "event/");
    private static final URI eventDay1Location = URI.create(eventLocation + "2011-10-09/");
    private static final URI eventDay1Hour1Location = URI.create(eventDay1Location + "00/");
    private static final URI eventDay1Hour2Location = URI.create(eventDay1Location + "23/");
    private static final URI eventDay2Location = URI.create(eventLocation + "2012-11-10/");
    private static final URI eventDay2Hour1Location = URI.create(eventDay2Location + "11/");
    private static final URI eventDay2Hour2Location = URI.create(eventDay2Location + "12/");
    private static final StoredObject eventDay1Hour1ObjectA = createStoredObject(eventDay1Hour1Location, "A");
    private static final StoredObject eventDay1Hour1ObjectB = createStoredObject(eventDay1Hour1Location, "B");
    private static final StoredObject eventDay1Hour2ObjectA = createStoredObject(eventDay1Hour2Location, "A");
    private static final StoredObject eventDay1Hour2ObjectB = createStoredObject(eventDay1Hour2Location, "B");
    private static final StoredObject eventDay2Hour1ObjectA = createStoredObject(eventDay2Hour1Location, "A");
    private static final StoredObject eventDay2Hour1ObjectB = createStoredObject(eventDay2Hour1Location, "B");
    private static final StoredObject eventDay2Hour2ObjectA = createStoredObject(eventDay2Hour2Location, "A");
    private static final StoredObject eventDay2Hour2ObjectB = createStoredObject(eventDay2Hour2Location, "B");
    private static final StoredObject eventObjectA = createStoredObject(eventLocation, "A");
    private static final StoredObject[] allObjects = new StoredObject[]{
            eventDay1Hour1ObjectA, eventDay1Hour1ObjectB, eventDay1Hour2ObjectA, eventDay1Hour2ObjectB,
            eventDay2Hour1ObjectA, eventDay2Hour1ObjectB, eventDay2Hour2ObjectA, eventDay2Hour2ObjectB,
            eventObjectA};

    @Test
    public void testListDirectories()
    {
        StorageSystem storageSystem = createStorageSystem(allObjects);
        checkDirectoryListing(storageSystem, stagingArea, eventLocation);
        checkDirectoryListing(storageSystem, eventLocation, eventDay1Location, eventDay2Location);
        checkDirectoryListing(storageSystem, eventDay1Location, eventDay1Hour1Location, eventDay1Hour2Location);
        checkDirectoryListing(storageSystem, eventDay2Location, eventDay2Hour1Location, eventDay2Hour2Location);
        checkDirectoryListing(storageSystem, eventDay1Hour1Location);
        checkDirectoryListing(storageSystem, eventDay1Hour2Location);
        checkDirectoryListing(storageSystem, eventDay1Hour1Location);
        checkDirectoryListing(storageSystem, eventDay2Hour2Location);
    }

    @Test
    public void testListDirectoriesNoTrailingSlash()
    {
        StorageSystem storageSystem = createStorageSystem(eventDay1Hour1ObjectA);
        checkDirectoryListing(storageSystem, eventLocation, eventDay1Location);
        checkDirectoryListing(storageSystem, stripTrailingSlashFromUri(eventLocation));
    }

    @Test
    public void testListObjects()
    {
        StorageSystem storageSystem = createStorageSystem(allObjects);
        checkObjectListing(storageSystem, stagingArea);
        checkObjectListing(storageSystem, eventLocation, eventObjectA);
        checkObjectListing(storageSystem, eventDay1Location);
        checkObjectListing(storageSystem, eventDay2Location);
        checkObjectListing(storageSystem, eventDay1Hour1Location, eventDay1Hour1ObjectA, eventDay1Hour1ObjectB);
        checkObjectListing(storageSystem, eventDay1Hour2Location, eventDay1Hour2ObjectA, eventDay1Hour2ObjectB);
        checkObjectListing(storageSystem, eventDay2Hour1Location, eventDay2Hour1ObjectA, eventDay2Hour1ObjectB);
        checkObjectListing(storageSystem, eventDay2Hour2Location, eventDay2Hour2ObjectA, eventDay2Hour2ObjectB);
    }

    @Test
    public void testListObjectsNoTrailingSlash()
    {
        StorageSystem storageSystem = createStorageSystem(eventObjectA);
        checkObjectListing(storageSystem, eventLocation, eventObjectA);
        checkObjectListing(storageSystem, stripTrailingSlashFromUri(eventLocation));
    }

    private static void checkDirectoryListing(StorageSystem storageSystem, URI directory, URI... subdirs)
    {
        assertEquals(storageSystem.listDirectories(directory), ImmutableList.copyOf(subdirs));
    }

    private static void checkObjectListing(StorageSystem storageSystem, URI directory, StoredObject... children)
    {
        assertEquals(storageSystem.listObjects(directory), ImmutableList.copyOf(children));
    }

    private static StorageSystem createStorageSystem(StoredObject... storedObjects)
    {
        TestingStorageSystem result = new TestingStorageSystem();
        result.addObjects(ImmutableList.copyOf(storedObjects));
        return result;
    }

    private static StoredObject createStoredObject(URI eventLocation, String... parts)
    {
        return new StoredObject(buildS3Location(eventLocation, parts), createEtag(), 1000, 0);
    }

    private static String createEtag()
    {
        return UUID.randomUUID().toString();
    }

    private static URI stripTrailingSlashFromUri(URI uri)
    {
        String uriAsString = uri.toString();
        while (uriAsString.endsWith("/")) {
            uriAsString = uriAsString.substring(0, uriAsString.length() - 1);
        }
        return URI.create(uriAsString);
    }
}
