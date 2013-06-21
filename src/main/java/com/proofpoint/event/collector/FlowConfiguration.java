package com.proofpoint.event.collector;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import javax.validation.constraints.NotNull;
import java.util.Set;

public class FlowConfiguration
{
    private Set<String> propertiesToSerialize;

    @JsonCreator
    public FlowConfiguration(@JsonProperty Set<String> propertiesToSerialize)
    {
        this.propertiesToSerialize = propertiesToSerialize;
    }

    @NotNull(message = "is missing")
    @JsonProperty
    public Set<String> getPropertiesToSerialize()
    {
        return propertiesToSerialize;
    }
}
