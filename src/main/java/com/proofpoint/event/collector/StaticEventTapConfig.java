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
import com.google.common.collect.ImmutableMap;
import com.proofpoint.configuration.Config;
import com.proofpoint.configuration.ConfigDescription;

import javax.validation.Valid;
import javax.validation.constraints.NotNull;
import java.util.List;
import java.util.Map;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Strings.nullToEmpty;

public class StaticEventTapConfig
{
    private static final Splitter FLOW_KEY_SPLITTER = Splitter.on('@')
            .trimResults();

    private Map<FlowKey, PerFlowStaticEventTapConfig> staticTaps = ImmutableMap.of();

    @Valid
    @NotNull
    public Map<FlowKey, PerFlowStaticEventTapConfig> getStaticTaps()
    {
        return staticTaps;
    }

    @Config("static-taps")
    @ConfigDescription("Event tap specification for static taps that are added to the dynamic taps found through discovery.")
    public StaticEventTapConfig setStaticTaps(Map<FlowKey, PerFlowStaticEventTapConfig> staticTaps)
    {
        this.staticTaps = staticTaps;
        return this;
    }

    public static final class FlowKey
    {
        private final String eventType;
        private final String flowId;

        public FlowKey(String eventType, String flowId)
        {
            this.eventType = checkNotNull(eventType, "eventType is null");
            this.flowId = checkNotNull(flowId, "flowId is null");
        }

        public static FlowKey valueOf(String rawValue)
        {
            List<String> tokens = FLOW_KEY_SPLITTER.splitToList(nullToEmpty(rawValue));
            checkArgument(tokens.size() == 2 , "Invalid flow key: %s; Flow key must have a single @ character", rawValue);
            checkArgument(!tokens.get(0).isEmpty() && !tokens.get(1).isEmpty() , "Invalid flow key: %s; Elements separate by @ cannot be empty", rawValue);
            return new FlowKey(tokens.get(0), tokens.get(1));
        }

        public String getEventType()
        {
            return eventType;
        }

        public String getFlowId()
        {
            return flowId;
        }

        @Override
        public int hashCode()
        {
            return Objects.hashCode(eventType, flowId);
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
            final FlowKey other = (FlowKey) obj;
            return Objects.equal(this.eventType, other.eventType) && Objects.equal(this.flowId, other.flowId);
        }

        @Override
        public String toString()
        {
            return Objects.toStringHelper(this)
                    .add("eventType", eventType)
                    .add("flowId", flowId)
                    .toString();
        }
    }
}
