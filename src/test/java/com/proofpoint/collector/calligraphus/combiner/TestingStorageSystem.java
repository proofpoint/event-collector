package com.proofpoint.collector.calligraphus.combiner;

import com.google.common.base.Function;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.MapMaker;

import javax.annotation.Nullable;
import java.io.File;
import java.net.URI;
import java.util.Collection;
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
        return ImmutableList.copyOf(objects.get(storageArea));
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
