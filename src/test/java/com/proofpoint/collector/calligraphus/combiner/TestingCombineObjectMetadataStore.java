package com.proofpoint.collector.calligraphus.combiner;

import com.google.common.base.Preconditions;
import com.google.common.collect.Maps;

import java.net.URI;
import java.util.concurrent.ConcurrentMap;

public class TestingCombineObjectMetadataStore implements CombineObjectMetadataStore
{
    private final String nodeId;
    private final ConcurrentMap<URI, CombinedStoredObject> metadata = Maps.newConcurrentMap();

    TestingCombineObjectMetadataStore(String nodeId)
    {
        this.nodeId = nodeId;
    }

    @Override
    public CombinedStoredObject getCombinedObjectManifest(URI combinedObjectLocation)
    {
        Preconditions.checkNotNull(combinedObjectLocation, "combinedObjectLocation is null");

        CombinedStoredObject combinedStoredObject = metadata.get(combinedObjectLocation);
        if (combinedStoredObject == null) {
            combinedStoredObject = CombinedStoredObject.createInitialCombinedStoredObject(combinedObjectLocation, nodeId);
        }

        return combinedStoredObject;
    }

    @Override
    public boolean replaceCombinedObjectManifest(CombinedStoredObject currentCombinedObject, CombinedStoredObject newCombinedObject)
    {
        Preconditions.checkNotNull(currentCombinedObject, "currentCombinedObject is null");
        Preconditions.checkNotNull(newCombinedObject, "newCombinedObject is null");
        Preconditions.checkArgument(currentCombinedObject.getLocation().equals(newCombinedObject.getLocation()), "newCombinedObject location is different from currentCombineObject location ");

        CombinedStoredObject persistentCombinedStoredObject = metadata.get(newCombinedObject.getLocation());
        if (persistentCombinedStoredObject != null) {
            if (persistentCombinedStoredObject.getVersion() != currentCombinedObject.getVersion()) {
                return false;
            }
        }
        else if (currentCombinedObject.getVersion() != 0) {
            return false;
        }

        URI location = currentCombinedObject.getLocation();
        if (currentCombinedObject.getVersion() == 0) {
            return metadata.putIfAbsent(location, newCombinedObject) == null;
        }
        else {
            return metadata.replace(location, currentCombinedObject, newCombinedObject);
        }
    }
}
