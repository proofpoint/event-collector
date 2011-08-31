package com.proofpoint.collector.calligraphus.combiner;

import com.google.common.collect.ImmutableList;
import org.codehaus.jackson.annotate.JsonCreator;
import org.codehaus.jackson.annotate.JsonProperty;

import java.net.URI;
import java.util.List;

public class CombinedStoredObject
{
    private final StoredObject storedObject;
    private final String creator;
    private final long createdTimestamp;
    private final List<StoredObject> sourceParts;

    public CombinedStoredObject(
            StoredObject storedObject,
            String creator,
            long createdTimestamp,
            List<StoredObject> sourceParts)
    {
        this.storedObject = storedObject;
        this.creator = creator;
        this.createdTimestamp = createdTimestamp;
        this.sourceParts = sourceParts;
    }

    @JsonCreator
    public CombinedStoredObject(
            @JsonProperty("name") String name,
            @JsonProperty("storageArea") URI storageArea,
            @JsonProperty("etag") String etag,
            @JsonProperty("size") long size,
            @JsonProperty("lastModified") long lastModified,
            @JsonProperty("creator") String creator,
            @JsonProperty("createdTimestamp") long createdTimestamp,
            @JsonProperty("sourceParts") List<StoredObject> sourceParts)
    {
        this.storedObject = new StoredObject(name, storageArea, etag, size, lastModified);
        this.creator = creator;
        this.createdTimestamp = createdTimestamp;
        this.sourceParts = ImmutableList.copyOf(sourceParts);
    }

    @JsonProperty
    public String getName()
    {
        return storedObject.getName();
    }

    @JsonProperty
    public URI getStorageArea()
    {
        return storedObject.getStorageArea();
    }

    @JsonProperty
    public String getETag()
    {
        return storedObject.getETag();
    }

    @JsonProperty
    public long getSize()
    {
        return storedObject.getSize();
    }

    @JsonProperty
    public long getLastModified()
    {
        return storedObject.getLastModified();
    }

    @JsonProperty
    public StoredObject getStoredObject()
    {
        return storedObject;
    }

    @JsonProperty
    public String getCreator()
    {
        return creator;
    }

    @JsonProperty
    public long getCreatedTimestamp()
    {
        return createdTimestamp;
    }

    @JsonProperty
    public List<StoredObject> getSourceParts()
    {
        return sourceParts;
    }
}
