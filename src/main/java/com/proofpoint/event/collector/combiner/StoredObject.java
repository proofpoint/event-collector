/*
 * Copyright 2011-2013 Proofpoint, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.proofpoint.event.collector.combiner;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Function;
import com.google.common.base.Preconditions;

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

    public static final Function<StoredObject, URI> GET_LOCATION_FUNCTION = new Function<StoredObject, URI>()
    {
        @Override
        public URI apply(StoredObject storedObject)
        {
            return storedObject.getLocation();
        }
    };
}
