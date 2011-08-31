package com.proofpoint.collector.calligraphus.combiner;

import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import org.codehaus.jackson.annotate.JsonCreator;
import org.codehaus.jackson.annotate.JsonProperty;

import java.net.URI;

public class StoredObject
{
    private final String name;
    private final URI storageArea;
    private final String etag;
    private final long size;
    private final long lastModified;

    public StoredObject(String name, URI storageArea)
    {
        Preconditions.checkNotNull(name, "name is null");
        Preconditions.checkNotNull(storageArea, "storageArea is null");

        this.name = name;
        this.storageArea = storageArea;
        this.etag = null;
        this.size = -1;
        this.lastModified = 0;
    }

    @JsonCreator
    public StoredObject(
            @JsonProperty("name") String name,
            @JsonProperty("storageArea") URI storageArea,
            @JsonProperty("etag") String etag,
            @JsonProperty("size") long size,
            @JsonProperty("lastModified") long lastModified)
    {
        Preconditions.checkNotNull(name, "name is null");
        Preconditions.checkNotNull(storageArea, "storageArea is null");

        this.name = name;
        this.storageArea = storageArea;
        this.etag = etag;
        this.size = size;
        this.lastModified = lastModified;
    }

    @JsonProperty
    public String getName()
    {
        return name;
    }

    @JsonProperty
    public URI getStorageArea()
    {
        return storageArea;
    }

    @JsonProperty
    public String getETag()
    {
        return etag;
    }

    @JsonProperty
    public long getSize()
    {
        return size;
    }

    @JsonProperty
    public long getLastModified()
    {
        return lastModified;
    }


    @Override
    public boolean equals(Object o)
    {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        StoredObject that = (StoredObject) o;

        if (etag != null ? !etag.equals(that.etag) : that.etag != null) {
            return false;
        }
        if (!name.equals(that.name)) {
            return false;
        }
        if (!storageArea.equals(that.storageArea)) {
            return false;
        }

        return true;
    }

    @Override
    public int hashCode()
    {
        int result = name.hashCode();
        result = 31 * result + storageArea.hashCode();
        result = 31 * result + (etag != null ? etag.hashCode() : 0);
        return result;
    }

    @Override
    public String toString()
    {
        final StringBuilder sb = new StringBuilder();
        sb.append("StoredObject");
        sb.append("{name='").append(name).append('\'');
        sb.append(", storageArea=").append(storageArea);
        sb.append(", etag='").append(etag).append('\'');
        sb.append(", size=").append(size);
        sb.append(", lastModified=").append(lastModified);
        sb.append('}');
        return sb.toString();
    }

    public static Function<StoredObject, String> GET_NAME_FUNCTION = new Function<StoredObject, String>()
    {
        @Override
        public String apply(StoredObject storedObject)
        {
            return storedObject.getName();
        }
    };
}
