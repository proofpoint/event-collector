package com.proofpoint.collector.calligraphus.combiner;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Maps;

import java.net.URI;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ConcurrentMap;

import static com.proofpoint.collector.calligraphus.combiner.S3StorageHelper.getS3Name;

public class TestingCombineObjectMetadataStore implements CombineObjectMetadataStore
{
    private final String nodeId;
    private final ConcurrentMap<StoredObject, CombinedStoredObject> metadata = Maps.newConcurrentMap();

    TestingCombineObjectMetadataStore(String nodeId)
    {
        this.nodeId = nodeId;
    }

    @Override
    public CombinedStoredObject getCombinedObjectManifest(URI stagingArea, URI targetArea)
    {
        Preconditions.checkNotNull(stagingArea, "stagingArea is null");
        Preconditions.checkNotNull(targetArea, "targetArea is null");

        String s3Name = getS3Name(stagingArea);
        Preconditions.checkNotNull(s3Name, "s3Name is null");

        CombinedStoredObject combinedStoredObject = metadata.get(new StoredObject(s3Name, targetArea));
        if (combinedStoredObject == null) {
            combinedStoredObject = new CombinedStoredObject(s3Name,
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

        CombinedStoredObject newCombinedObject = new CombinedStoredObject(
                currentCombinedObject.getName(),
                currentCombinedObject.getStorageArea(),
                UUID.randomUUID().toString(),
                totalSize,
                System.currentTimeMillis(),
                nodeId,
                System.currentTimeMillis(),
                newCombinedObjectParts
        );

        if (currentCombinedObject.getETag() == null) {
            return metadata.putIfAbsent(currentCombinedObject.getStoredObject(), newCombinedObject) == null;
        }
        else {
            return metadata.replace(currentCombinedObject.getStoredObject(), currentCombinedObject, newCombinedObject);
        }
    }
}
