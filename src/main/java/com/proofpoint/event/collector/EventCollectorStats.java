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

import com.proofpoint.reporting.Key;
import com.proofpoint.stats.CounterStat;

import java.net.URI;

public interface EventCollectorStats
{
    // EventCollector.IncomingEvents.Count (Tags: eventType=blah, eventStatus=valid)
    CounterStat incomingEvents(@Key("eventType") String eventType, @Key("eventStatus") EventStatus eventStatus);

    CounterStat outboundEvents(@Key("eventType") String eventType, @Key("flowId") String flowId, @Key("outboundStatus") Status status);

    CounterStat outboundEvents(@Key("eventType") String eventType, @Key("flowId") String flowId, @Key("uri") URI uri, @Key("outboundStatus") Status status);

    public enum Status
    {
        DROPPED,              // events dropped due to queue overflow
        DELIVERED,            // events successfully delivered to the consumer
        LOST,                 // events couldn't be delivered because all taps returned 5XX (server) error
        REJECTED;             // events couldn't be delivered because a tap rejected with 4XX (client) error

        @Override
        public String toString()
        {
            return name().toLowerCase();
        }
    }

    public enum EventStatus
    {
        VALID, UNSUPPORTED;

        @Override
        public String toString()
        {
            return name().toLowerCase();
        }
    }
}