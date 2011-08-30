package com.proofpoint.collector.calligraphus.combiner;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Maps;

import java.util.List;
import java.util.UUID;
import java.util.concurrent.ConcurrentMap;

public class TestingCombineObjectMetadataStore implements CombineObjectMetadataStore
{
    private final String nodeId;
    private final ConcurrentMap<StoredObject, CombinedStoredObject> metadata = Maps.newConcurrentMap();

    TestingCombineObjectMetadataStore(String nodeId)
    {
        this.nodeId = nodeId;
    }

    @Override
    public CombinedStoredObject getCombinedObjectManifest(StorageArea stagingArea, StorageArea targetArea)
    {
        CombinedStoredObject combinedStoredObject = metadata.get(new StoredObject(stagingArea.getName(), targetArea));
        if (combinedStoredObject == null) {
            combinedStoredObject = new CombinedStoredObject(stagingArea.getName(),
                    targetArea,
                    null,
                    0,
                    0,
                    nodeId,
                    0,
                    ImmutableList.<StoredObject>of()
            );
        }

        return combinedStoredObject;
    }

    @Override
    public boolean replaceCombinedObjectManifest(CombinedStoredObject currentCombinedObject, List<StoredObject> newCombinedObjectParts)
    {
        long totalSize = 0;
        for (StoredObject storedObject : newCombinedObjectParts) {
            totalSize += storedObject.getSize();
        }

        StoredObject currentStoredObject = currentCombinedObject.getStoredObject();
        CombinedStoredObject newCombinedObject = new CombinedStoredObject(
                currentStoredObject.getName(),
                currentStoredObject.getStorageArea(),
                UUID.randomUUID().toString(),
                totalSize,
                System.currentTimeMillis(),
                nodeId,
                System.currentTimeMillis(),
                newCombinedObjectParts
        );

        if (currentStoredObject.getETag() == null) {
            return metadata.putIfAbsent(currentStoredObject, newCombinedObject) == null;
        }
        else {
            return metadata.replace(currentStoredObject, currentCombinedObject, newCombinedObject);
        }
    }
}
