package com.proofpoint.collector.calligraphus.combiner;

import com.google.common.base.Function;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.MapMaker;

import javax.annotation.Nullable;
import java.io.File;
import java.net.URI;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static com.google.common.collect.Lists.newArrayList;

public class TestingStorageSystem implements StorageSystem
{
    private final Map<URI, List<StoredObject>> objects = new MapMaker().makeComputingMap(new Function<URI, List<StoredObject>>()
    {
        @Override
        public List<StoredObject> apply(@Nullable URI input)
        {
            return newArrayList();
        }
    });

    public void addObject(URI storageArea, StoredObject storedObject)
    {
        objects.get(storageArea).add(storedObject);
    }

    public void removeObject(URI storageArea, StoredObject storedObject)
    {
        objects.get(storageArea).remove(storedObject);
    }

    @Override
    public List<URI> listDirectories(URI storageArea)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public List<StoredObject> listObjects(URI storageArea)
    {
        List<StoredObject> storedObjects = objects.get(storageArea);
        return ImmutableList.copyOf(storedObjects);
    }

    @Override
    public StoredObject createCombinedObject(URI location, List<StoredObject> newCombinedObjectParts)
    {
        long size = 0;
        for (StoredObject newCombinedObjectPart : newCombinedObjectParts) {
            size += newCombinedObjectPart.getSize();
        }
        return new StoredObject(location, UUID.randomUUID().toString(), size, System.currentTimeMillis());
    }

    @Override
    public StoredObject putObject(URI location, File source)
    {
        throw new UnsupportedOperationException();
    }
}
