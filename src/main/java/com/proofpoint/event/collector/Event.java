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

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.joda.time.DateTime;

import javax.annotation.concurrent.Immutable;
import javax.validation.constraints.NotNull;
import javax.validation.constraints.Pattern;
import java.util.Map;
import java.util.Objects;

@Immutable
public class Event
{
    private final String type;
    private final String uuid;
    private final String host;
    private final DateTime timestamp;
    private final Map<String, ?> data;

    public Event(String type, String uuid, String host, DateTime timestamp, Map<String, ?> data)
    {
        this.type = type;
        this.uuid = uuid;
        this.host = host;
        this.timestamp = timestamp;
        this.data = data;
    }

    @JsonCreator
    private static Event fromJson(@JsonProperty("type") String type,
            @JsonProperty("uuid") String uuid,
            @JsonProperty("host") String host,
            @JsonProperty("timestamp") String timestamp,
            @JsonProperty("data") Map<String, ?> data)
    {
        return new Event(type, uuid, host, DateTime.parse(timestamp), data);
    }

    @JsonProperty
    @NotNull(message = "is missing")
    @Pattern(regexp = "[A-Za-z][A-Za-z0-9]*", message = "must be alphanumeric")
    public String getType()
    {
        return type;
    }

    @JsonProperty
    @NotNull(message = "is missing")
    public String getUuid()
    {
        return uuid;
    }

    @JsonProperty
    @NotNull(message = "is missing")
    public String getHost()
    {
        return host;
    }

    @JsonProperty
    @NotNull(message = "is missing")
    public DateTime getTimestamp()
    {
        return timestamp;
    }

    @JsonProperty
    @NotNull(message = "is missing")
    public Map<String, ?> getData()
    {
        return data;
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(type, uuid, host, timestamp, data);
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
        final Event other = (Event) obj;

        //this part is not auto generated since we have to use the ridiculous isEqual method to compare timestamps
        if (timestamp != null ? !timestamp.isEqual(other.timestamp) : other.timestamp != null) {
            return false;
        }

        return Objects.equals(this.type, other.type) && Objects.equals(this.uuid, other.uuid) && Objects.equals(this.host, other.host) && Objects.equals(this.data, other.data);
    }
}
