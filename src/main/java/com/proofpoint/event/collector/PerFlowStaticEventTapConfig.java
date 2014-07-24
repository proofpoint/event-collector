/*
 * Copyright 2011-2014 Proofpoint, Inc.
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
package com.proofpoint.event.collector;

import com.google.common.base.Objects;
import com.google.common.base.Splitter;
import com.google.common.collect.ImmutableSet;
import com.proofpoint.configuration.Config;
import com.proofpoint.configuration.ConfigDescription;

import javax.validation.constraints.NotNull;
import javax.validation.constraints.Size;
import java.util.Set;

import static com.google.common.base.Strings.nullToEmpty;

public class PerFlowStaticEventTapConfig
{
    private static final Splitter COMMA_LIST_SPLITTER = Splitter.on(",").omitEmptyStrings().trimResults();

    private Set<String> eventTypes;
    private String flowId;
    private Set<String> uris;

    @Size(min = 1, message = "may not be empty")
    public Set<String> getEventTypes()
    {
        return eventTypes;
    }

    @Config("event-types")
    @ConfigDescription("Comma separated list of event types to send")
    public PerFlowStaticEventTapConfig setEventTypes(String eventTypes)
    {
        this.eventTypes = split(eventTypes);
        return this;
    }

    @NotNull
    @Size(min = 1, message = "may not be empty")
    public String getFlowId()
    {
        return flowId;
    }

    @Config("flow-id")
    @ConfigDescription("")
    public PerFlowStaticEventTapConfig setFlowId(String flowId)
    {
        this.flowId = flowId;
        return this;
    }

    @Size(min = 1, message = "may not be empty")
    public Set<String> getUris()
    {
        return uris;
    }

    @Config("uris")
    @ConfigDescription("Comma separated list of tap uris")
    public PerFlowStaticEventTapConfig setUris(String uris)
    {
        this.uris = split(uris);
        return this;
    }

    @Override
    public int hashCode()
    {
        return Objects.hashCode(eventTypes, flowId, uris);
    }

    @Override
    public boolean equals(Object obj)
    {
        if (this == obj) {
            return true;
        }
        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }
        final PerFlowStaticEventTapConfig other = (PerFlowStaticEventTapConfig) obj;
        return Objects.equal(this.eventTypes, other.eventTypes) && Objects.equal(this.flowId, other.flowId) && Objects.equal(this.uris, other.uris);
    }

    @Override
    public String toString()
    {
        return Objects.toStringHelper(this)
                .add("eventTypes", eventTypes)
                .add("flowId", flowId)
                .add("uris", uris)
                .toString();
    }

    private Set<String> split(String inputString)
    {
        return ImmutableSet.copyOf(COMMA_LIST_SPLITTER.split(nullToEmpty(inputString)));
    }
}
