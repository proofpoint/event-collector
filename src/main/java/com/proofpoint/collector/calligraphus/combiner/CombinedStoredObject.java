package com.proofpoint.collector.calligraphus.combiner;

import com.google.common.collect.ImmutableList;
import org.codehaus.jackson.annotate.JsonCreator;
import org.codehaus.jackson.annotate.JsonProperty;

import java.net.URI;
import java.util.List;

import static com.google.common.base.Preconditions.checkNotNull;
import static java.lang.System.currentTimeMillis;

public class CombinedStoredObject
{
    private final URI location;
    private final long updatedTimestamp;
    private final List<StoredObject> sourceParts;

    @JsonCreator
    public CombinedStoredObject(
            @JsonProperty("location") URI location,
            @JsonProperty("updatedTimestamp") long updatedTimestamp,
            @JsonProperty("sourceParts") List<StoredObject> sourceParts)
    {
        this.location = location;
        this.updatedTimestamp = updatedTimestamp;
        this.sourceParts = ImmutableList.copyOf(sourceParts);
    }

    @JsonProperty
    public URI getLocation()
    {
        return location;
    }

    @JsonProperty
    public long getUpdatedTimestamp()
    {
        return updatedTimestamp;
    }

    @JsonProperty
    public List<StoredObject> getSourceParts()
    {
        return sourceParts;
    }

    public long getSize()
    {
        long size = 0;
        for (StoredObject part : sourceParts) {
            size += part.getSize();
        }
        return size;
    }

    public CombinedStoredObject update(List<StoredObject> sourceParts)
    {
        checkNotNull(sourceParts, "sourceParts is null");
        return new CombinedStoredObject(location, currentTimeMillis(), sourceParts);
    }

    public CombinedStoredObject addPart(StoredObject sourcePart)
    {
        return update(concat(sourceParts, sourcePart));
    }

    private static <T> ImmutableList<T> concat(Iterable<T> base, T item)
    {
        return ImmutableList.<T>builder().addAll(base).add(item).build();
    }
}
