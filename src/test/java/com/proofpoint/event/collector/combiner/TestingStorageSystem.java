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

import com.google.common.base.Function;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.MapMaker;
import com.google.common.collect.Ordering;

import javax.annotation.Nullable;
import java.io.File;
import java.net.URI;
import java.util.Collection;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import static com.google.common.collect.Sets.newHashSet;
import static java.lang.System.currentTimeMillis;
import static java.util.Arrays.asList;

public class TestingStorageSystem
        implements StorageSystem
{
    private final Map<URI, Set<StoredObject>> objects = new MapMaker().makeComputingMap(new Function<URI, Set<StoredObject>>()
    {
        @Override
        public Set<StoredObject> apply(@Nullable URI input)
        {
            return newHashSet();
        }
    });

    public void addObjects(Collection<StoredObject> storedObjects)
    {
        for (StoredObject storedObject : storedObjects) {
            objects.get(directory(storedObject.getLocation())).add(storedObject);
        }
    }

    public boolean removeObject(URI location)
    {
        Iterator<StoredObject> iter = objects.get(directory(location)).iterator();
        while (iter.hasNext()) {
            StoredObject object = iter.next();
            if (object.getLocation().equals(location)) {
                iter.remove();
                return true;
            }
        }
        return false;
    }

    public boolean objectExists(URI location)
    {
        for (StoredObject storedObject : objects.get(directory(location))) {
            if (storedObject.getLocation().equals(location)) {
                return true;
            }
        }
        return false;
    }

    @Override
    public List<URI> listDirectories(URI storageArea)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public List<StoredObject> listObjects(URI storageArea)
    {
        // S3 always lists objects in sorted order
        return Ordering.from(new Comparator<StoredObject>()
        {
            @Override
            public int compare(StoredObject o1, StoredObject o2)
            {
                return o1.getLocation().compareTo(o2.getLocation());
            }
        }).immutableSortedCopy(objects.get(storageArea));
    }

    @Override
    public StoredObject createCombinedObject(CombinedStoredObject object)
    {
        for (StoredObject sourcePart : object.getSourceParts()) {
            if (!objectExists(sourcePart.getLocation())) {
                throw new AssertionError("source part does not exist: " + sourcePart);
            }
        }
        StoredObject newObject = new StoredObject(object.getLocation(), UUID.randomUUID().toString(), object.getSize(), currentTimeMillis());
        addObjects(asList(newObject));
        return newObject;
    }

    @Override
    public StoredObject putObject(URI location, File source)
    {
        throw new UnsupportedOperationException();
    }

    private static URI directory(URI uri)
    {
        String s = uri.toString();
        return URI.create(s.substring(0, s.lastIndexOf('/')));
    }
}
