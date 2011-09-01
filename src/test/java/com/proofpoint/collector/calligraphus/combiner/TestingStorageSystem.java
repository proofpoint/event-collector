package com.proofpoint.collector.calligraphus.combiner;

import com.google.common.base.Function;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.MapMaker;

import javax.annotation.Nullable;
import java.io.File;
import java.net.URI;
import java.util.List;
import java.util.Map;

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
    public List<StoredObject> listObjects(URI storageArea)
    {
        List<StoredObject> storedObjects = objects.get(storageArea);
        return ImmutableList.copyOf(storedObjects);
    }

    @Override
    public StoredObject createCombinedObject(StoredObject target, List<StoredObject> newCombinedObjectParts)
    {
        return target;
    }

    @Override
    public StoredObject putObject(StoredObject target, File source)
    {
        throw new UnsupportedOperationException();
    }
}
