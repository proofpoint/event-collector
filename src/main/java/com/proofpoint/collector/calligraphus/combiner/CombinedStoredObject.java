package com.proofpoint.collector.calligraphus.combiner;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import org.codehaus.jackson.annotate.JsonCreator;
import org.codehaus.jackson.annotate.JsonProperty;

import java.net.URI;
import java.util.List;

public class CombinedStoredObject
{
    public static CombinedStoredObject createInitialCombinedStoredObject(URI location, String creator)
    {
        Preconditions.checkNotNull(location, "location is null");
        Preconditions.checkNotNull(creator, "creator is null");
        return new CombinedStoredObject(location,
                0,
                creator,
                System.currentTimeMillis(),
                ImmutableList.<StoredObject>of());
    }

    private final URI location;
    private final long version;
    private final String creator;
    private final long createdTimestamp;

    private final List<StoredObject> sourceParts;

    @JsonCreator
    public CombinedStoredObject(
            @JsonProperty("location") URI location,
            @JsonProperty("version") long version,
            @JsonProperty("creator") String creator,
            @JsonProperty("createdTimestamp") long createdTimestamp,
            @JsonProperty("sourceParts") List<StoredObject> sourceParts)
    {
        this.location = location;
        this.version = version;
        this.creator = creator;
        this.createdTimestamp = createdTimestamp;
        this.sourceParts = ImmutableList.copyOf(sourceParts);
    }

    @JsonProperty
    public URI getLocation()
    {
        return location;
    }

    @JsonProperty
    public long getVersion()
    {
        return version;
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

    public CombinedStoredObject update(String creator, List<StoredObject> sourceParts)
    {
        Preconditions.checkNotNull(sourceParts, "sourceParts is null");
        return new CombinedStoredObject(location,
                version + 1,
                creator,
                System.currentTimeMillis(),
                sourceParts);
    }
}
