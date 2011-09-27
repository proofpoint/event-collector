package com.proofpoint.collector.calligraphus.combiner;

import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import org.codehaus.jackson.annotate.JsonCreator;
import org.codehaus.jackson.annotate.JsonProperty;

import java.net.URI;

public class StoredObject
{
    private final URI location;
    private final String etag;
    private final long size;
    private final long lastModified;

    public StoredObject(URI location)
    {
        Preconditions.checkNotNull(location, "location is null");
        this.location = location;
        this.etag = null;
        this.size = 0;
        this.lastModified = 0;
    }

    @JsonCreator
    public StoredObject(
            @JsonProperty("location") URI location,
            @JsonProperty("etag") String etag,
            @JsonProperty("size") long size,
            @JsonProperty("lastModified") long lastModified)
    {
        Preconditions.checkNotNull(location, "location is null");
        Preconditions.checkArgument(size >= 0, "size is negative");
        Preconditions.checkArgument(lastModified >= 0, "lastModified is negative");

        this.location = location;
        this.etag = etag;
        this.size = size;
        this.lastModified = lastModified;
    }

    @JsonProperty
    public URI getLocation()
    {
        return location;
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

        if (size != that.size) {
            return false;
        }
        if (etag != null ? !etag.equals(that.etag) : that.etag != null) {
            return false;
        }
        if (!location.equals(that.location)) {
            return false;
        }

        return true;
    }

    @Override
    public int hashCode()
    {
        int result = location.hashCode();
        result = 31 * result + (etag != null ? etag.hashCode() : 0);
        result = 31 * result + (int) (size ^ (size >>> 32));
        return result;
    }

    @Override
    public String toString()
    {
        final StringBuilder sb = new StringBuilder();
        sb.append("StoredObject");
        sb.append("{location='").append(location).append('\'');
        sb.append(", etag='").append(etag).append('\'');
        sb.append(", size=").append(size);
        sb.append(", lastModified=").append(lastModified);
        sb.append('}');
        return sb.toString();
    }

    public static Function<StoredObject, URI> GET_LOCATION_FUNCTION = new Function<StoredObject, URI>()
    {
        @Override
        public URI apply(StoredObject storedObject)
        {
            return storedObject.getLocation();
        }
    };
}
