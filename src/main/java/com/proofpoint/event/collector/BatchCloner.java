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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.proofpoint.event.collector.BatchProcessor.BatchHandler;

import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;

import static com.google.common.base.Preconditions.checkNotNull;

public class BatchCloner<T>
    implements BatchHandler<T>
{
    private final AtomicReference<Set<BatchHandler<T>>> destinations = new AtomicReference<Set<BatchHandler<T>>>(
            ImmutableSet.<BatchHandler<T>>of());

    @Override
    public void processBatch(List<T> entries)
    {
        List<T> copyOfEntries = ImmutableList.<T>copyOf(entries);
        for (BatchHandler<T> handler: destinations.get()) {
            handler.processBatch(copyOfEntries);
        }
    }

    public void clear()
    {
        destinations.set(ImmutableSet.<BatchHandler<T>>of());
    }

    public void setDestinations(Set<? extends BatchHandler<T>> newDestinations)
    {
        checkNotNull(newDestinations, "newDestinations is null");
        destinations.set(ImmutableSet.<BatchHandler<T>>copyOf(newDestinations));
    }
}
