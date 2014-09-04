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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Objects;
import com.google.common.base.Splitter;
import com.google.common.collect.ImmutableSet;
import com.proofpoint.configuration.Config;
import com.proofpoint.configuration.ConfigDescription;
import com.proofpoint.event.collector.validation.ValidUri;

import javax.validation.constraints.NotNull;
import javax.validation.constraints.Size;
import java.util.Set;

import static com.google.common.base.Preconditions.*;
import static com.google.common.base.Strings.nullToEmpty;
import static com.proofpoint.event.collector.QosDelivery.BEST_EFFORT;

public class PerFlowStaticEventTapConfig
{
    private static final Splitter COMMA_LIST_SPLITTER = Splitter.on(',')
            .trimResults()
            .omitEmptyStrings();

    private QosDelivery qosDelivery = BEST_EFFORT;
    private Set<String> uris = ImmutableSet.of();

    @ValidUri(schemes = {"http", "https"})
    @Size(min = 1, message = "may not be empty")
    public Set<String> getUris()
    {
        return uris;
    }

    @Config("uris")
    @ConfigDescription("Comma separated list of tap uris")
    public PerFlowStaticEventTapConfig setUris(String urisString)
    {
        Iterable<String> uriTokens = COMMA_LIST_SPLITTER.split(nullToEmpty(urisString));
        this.uris = ImmutableSet.copyOf(uriTokens);
        return this;
    }

    @VisibleForTesting
    PerFlowStaticEventTapConfig setUris(Set<String> uris)
    {
        this.uris = ImmutableSet.copyOf(checkNotNull(uris, "uris is null"));
        return this;
    }

    @NotNull
    public QosDelivery getQosDelivery()
    {
        return qosDelivery;
    }

    @Config("qos-delivery")
    @ConfigDescription("The Quality of Service the collector will provide when delivering events to this flow.")
    public PerFlowStaticEventTapConfig setQosDelivery(QosDelivery qosDelivery)
    {
        this.qosDelivery = qosDelivery;
        return this;
    }

    @Override
    public int hashCode()
    {
        return Objects.hashCode(qosDelivery, uris);
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
        return Objects.equal(this.qosDelivery, other.qosDelivery) && Objects.equal(this.uris, other.uris);
    }

    @Override
    public String toString()
    {
        return Objects.toStringHelper(this)
                .add("qosDelivery", qosDelivery)
                .add("uris", uris)
                .toString();
    }

}
