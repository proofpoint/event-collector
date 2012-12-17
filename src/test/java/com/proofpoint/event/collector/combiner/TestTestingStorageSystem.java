/*
 * Copyright 2011-2012 Proofpoint, Inc.
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

import java.util.List;
import java.util.UUID;
import java.net.URI;
import org.testng.annotations.Test;

import static com.google.common.collect.Lists.newArrayList;
import static com.proofpoint.event.collector.combiner.S3StorageHelper.buildS3Location;
import static org.testng.Assert.assertEquals;

public class TestTestingStorageSystem
{
    private static final URI stagingArea = URI.create("s3://bucket/staging/");

    @Test
    public void testListDirectories()
    {
        TestingStorageSystem storageSystem = new TestingStorageSystem();
        URI location = buildS3Location(stagingArea, "event", "day", "hour");
        StoredObject objectA = new StoredObject(buildS3Location(location, "a"), UUID.randomUUID().toString(), 1000, 0);
        StoredObject objectB = new StoredObject(buildS3Location(location, "b"), UUID.randomUUID().toString(), 1000, 0);

        // create single test group
        List<StoredObject> smallGroup = newArrayList(objectA, objectB);
        storageSystem.addObjects(smallGroup);

        List<URI> directories = storageSystem.listDirectories(stagingArea);
        assertEquals(directories, newArrayList(URI.create(stagingArea + "event")));

        directories = storageSystem.listDirectories(directories.get(0));
        assertEquals(directories, newArrayList(URI.create(stagingArea + "event/day")));

        directories = storageSystem.listDirectories(directories.get(0));
        assertEquals(directories, newArrayList(URI.create(stagingArea + "event/day/hour")));
    }
}
