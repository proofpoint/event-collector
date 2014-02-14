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

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSortedSet;
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

    private final LoadingCache<URI, Set<StoredObject>> objects = CacheBuilder.newBuilder()
            .build(new CacheLoader<URI, Set<StoredObject>>()
            {
                @Override
                public Set<StoredObject> load(@Nullable URI input)
                {
                    return newHashSet();
                }
            });

    public void addObjects(Collection<StoredObject> storedObjects)
    {
        for (StoredObject storedObject : storedObjects) {
            objects.getUnchecked(directory(storedObject.getLocation())).add(storedObject);
        }
    }

    public boolean removeObject(URI location)
    {
        Iterator<StoredObject> iter = objects.getUnchecked(directory(location)).iterator();
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
        for (StoredObject storedObject : objects.getUnchecked(directory(location))) {
            if (storedObject.getLocation().equals(location)) {
                return true;
            }
        }
        return false;
    }

    @Override
    public List<URI> listDirectories(URI storageAreaURI)
    {
        String storageArea = storageAreaURI.toString();
        ImmutableSortedSet.Builder<URI> builder = ImmutableSortedSet.naturalOrder();
        if (storageArea.endsWith("/")) {
            // Directories must end in a slash, otherwise S3 considers them as normal files.
            for (Map.Entry<URI, Set<StoredObject>> entry : objects.asMap().entrySet()) {
                String uri = entry.getKey().toString();
                if (uri.startsWith(storageArea) && !uri.equals(storageArea)) {
                    // All URIs are directories and MUST have another slash following the prefix
                    // being searched (except for the search directory). That trailing slash
                    // of the subdirectory must be included in the result.
                    builder.add(URI.create(uri.substring(0, uri.indexOf("/", storageArea.length()) + 1)));
                }
            }
        }
        return ImmutableList.copyOf(builder.build());
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
        }).immutableSortedCopy(objects.getUnchecked(storageArea));
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
        return URI.create(s.substring(0, s.lastIndexOf('/')) + "/");
    }
}
