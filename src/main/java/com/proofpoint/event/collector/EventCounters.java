/*
 * Copyright 2011-2012 Proofpoint, Inc.
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

import com.google.common.annotations.Beta;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.MapMaker;
import org.codehaus.jackson.annotate.JsonProperty;

import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicLong;

import static com.google.common.base.Objects.firstNonNull;

@Beta
public class EventCounters<K>
{
    private final ConcurrentMap<String, Counter> counters = new MapMaker().makeMap();

    public static class Counter
    {
        // TODO: These should be changed to decay counters, once this is on a more recent version of platform.
        private final AtomicLong received = new AtomicLong();
        private final AtomicLong lost = new AtomicLong();

        public void recordReceived(long count)
        {
            received.addAndGet(count);
        }

        public void recordLost(long count)
        {
            lost.addAndGet(count);
        }

        public CounterState getState()
        {
            return new CounterState(received.get(), lost.get());
        }
    }

    public static class CounterState
    {
        private final long received;
        private final long lost;

        public CounterState(long received, long lost)
        {
            this.received = received;
            this.lost = lost;
        }

        @JsonProperty
        public long getReceived()
        {
            return received;
        }

        @JsonProperty
        public long getLost()
        {
            return lost;
        }
    }

    public void recordReceived(K key, long count)
    {
        getCounter(key).recordReceived(count);
    }

    public void recordLost(K key, long count)
    {
        getCounter(key).recordLost(count);
    }

    public void resetCounts()
    {
        counters.clear();
    }

    public Map<String, CounterState> getCounts()
    {
        ImmutableMap.Builder<String, CounterState> builder = ImmutableMap.builder();

        for(Entry<String, Counter> entry : counters.entrySet()) {
            builder.put(entry.getKey(), entry.getValue().getState());
        }

        return builder.build();
    }

    private Counter getCounter(K key)
    {
        String stringKey = key.toString();
        Counter counter = counters.get(stringKey);
        if (counter != null) {
            return counter;
        }

        counter = new Counter();
        return firstNonNull(counters.putIfAbsent(stringKey, counter), counter);
    }
}
