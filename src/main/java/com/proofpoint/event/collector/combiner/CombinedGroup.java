/*
 * Copyright 2011 Proofpoint, Inc.
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
import com.google.common.collect.Lists;
import org.codehaus.jackson.annotate.JsonCreator;
import org.codehaus.jackson.annotate.JsonProperty;

import java.net.URI;
import java.util.List;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.proofpoint.event.collector.combiner.S3StorageHelper.appendSuffix;
import static java.lang.String.format;
import static java.lang.System.currentTimeMillis;

public class CombinedGroup
{
    public static CombinedGroup createInitialCombinedGroup(URI locationPrefix, String creator)
    {
        checkNotNull(locationPrefix, "locationPrefix is null");
        checkNotNull(creator, "creator is null");
        return new CombinedGroup(locationPrefix, 0, creator, currentTimeMillis(), ImmutableList.<CombinedStoredObject>of());
    }

    private final URI locationPrefix;
    private final long version;
    private final String creator;
    private final long updatedTimestamp;
    private final List<CombinedStoredObject> combinedObjects;

    @JsonCreator
    public CombinedGroup(
            @JsonProperty("locationPrefix") URI locationPrefix,
            @JsonProperty("version") long version,
            @JsonProperty("creator") String creator,
            @JsonProperty("updatedTimestamp") long updatedTimestamp,
            @JsonProperty("combinedObjects") List<CombinedStoredObject> combinedObjects)
    {
        this.locationPrefix = locationPrefix;
        this.version = version;
        this.creator = creator;
        this.updatedTimestamp = updatedTimestamp;
        this.combinedObjects = combinedObjects;
    }

    @JsonProperty
    public URI getLocationPrefix()
    {
        return locationPrefix;
    }

    @JsonProperty
    public long getVersion()
    {
        return version;
    }

    @JsonProperty
    public String getCreator()
    {
        return creator;
    }

    @JsonProperty
    public long getUpdatedTimestamp()
    {
        return updatedTimestamp;
    }

    @JsonProperty
    public List<CombinedStoredObject> getCombinedObjects()
    {
        return combinedObjects;
    }

    @Override
    public String toString()
    {
        return locationPrefix.toString();
    }

    public CombinedStoredObject getCombinedObject(URI location) {
        for (CombinedStoredObject combinedObject : combinedObjects) {
            if (location.equals(combinedObject.getLocation())) {
                return combinedObject;
            }
        }
        return null;
    }

    public CombinedGroup update(String creator, List<CombinedStoredObject> combinedObjects)
    {
        checkNotNull(creator, "creator is null");
        checkNotNull(combinedObjects, "combinedObjects is null");
        return new CombinedGroup(locationPrefix, version + 1, creator, currentTimeMillis(), combinedObjects);
    }

    public CombinedGroup updateCombinedObject(String creator, CombinedStoredObject combinedObject)
    {
        checkNotNull(creator, "creator is null");
        checkNotNull(combinedObject, "combinedObject is null");
        List<CombinedStoredObject> newList = Lists.newArrayList();
        boolean found = false;
        for (CombinedStoredObject object : combinedObjects) {
            if (combinedObject.getLocation().equals(object.getLocation())) {
                found = true;
                object = combinedObject;
            }
            newList.add(object);
        }
        if (!found) {
            throw new IllegalArgumentException("combinedObjects does not contain object to update");
        }
        return update(creator, newList);
    }

    public CombinedGroup addNewCombinedObject(String creator, List<StoredObject> parts)
    {
        checkNotNull(creator, "creator is null");
        checkNotNull(parts, "parts is null");
        URI location = appendSuffix(locationPrefix, format("%05d.json.snappy", combinedObjects.size()));
        CombinedStoredObject newObject = new CombinedStoredObject(location, currentTimeMillis(), parts);
        return update(creator, concat(combinedObjects, newObject));
    }

    private static <T> ImmutableList<T> concat(Iterable<T> base, T item)
    {
        return ImmutableList.<T>builder().addAll(base).add(item).build();
    }
}
