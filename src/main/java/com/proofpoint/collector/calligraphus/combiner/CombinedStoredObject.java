package com.proofpoint.collector.calligraphus.combiner;

import com.google.common.collect.ImmutableList;

import java.util.List;

public class CombinedStoredObject
{
    private final StoredObject storedObject;
    private final String creator;
    private final long manifestTimestamp;
    private final List<StoredObject> sourceParts;

    public CombinedStoredObject(StoredObject storedObject, String creator, long manifestTimestamp, List<StoredObject> sourceParts)
    {
        this.storedObject = storedObject;
        this.creator = creator;
        this.manifestTimestamp = manifestTimestamp;
        this.sourceParts = sourceParts;
    }

    public CombinedStoredObject(
            String name,
            StorageArea storageArea,
            String etag,
            long size,
            long lastModified,
            String creator,
            long manifestTimestamp,
            List<StoredObject> sourceParts)
    {
        this.storedObject = new StoredObject(name, storageArea, etag, size, lastModified);
        this.creator = creator;
        this.manifestTimestamp = manifestTimestamp;
        this.sourceParts = ImmutableList.copyOf(sourceParts);
    }

    public StoredObject getStoredObject()
    {
        return storedObject;
    }

    public String getCreator()
    {
        return creator;
    }

    public long getManifestTimestamp()
    {
        return manifestTimestamp;
    }

    public List<StoredObject> getSourceParts()
    {
        return sourceParts;
    }

}
