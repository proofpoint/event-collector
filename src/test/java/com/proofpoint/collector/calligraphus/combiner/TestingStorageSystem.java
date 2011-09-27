package com.proofpoint.collector.calligraphus.combiner;

import com.google.common.base.Function;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.MapMaker;

import javax.annotation.Nullable;
import java.io.File;
import java.net.URI;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import static com.google.common.collect.Sets.newHashSet;

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

    public void addObjects(URI storageArea, Collection<StoredObject> storedObjects)
    {
        objects.get(storageArea).addAll(storedObjects);
    }

    @Override
    public List<URI> listDirectories(URI storageArea)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public List<StoredObject> listObjects(URI storageArea)
    {
        return ImmutableList.copyOf(objects.get(storageArea));
    }

    @Override
    public StoredObject createCombinedObject(CombinedStoredObject object)
    {
        return new StoredObject(object.getLocation(), UUID.randomUUID().toString(), object.getSize(), System.currentTimeMillis());
    }

    @Override
    public StoredObject putObject(URI location, File source)
    {
        throw new UnsupportedOperationException();
    }
}
