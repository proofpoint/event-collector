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
package com.proofpoint.event.collector.combiner;

import com.google.common.collect.ImmutableSet;
import com.proofpoint.event.collector.ServerConfig;
import com.proofpoint.log.Logger;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import javax.inject.Inject;
import javax.inject.Named;
import java.io.IOException;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.ScheduledExecutorService;

import static com.google.common.base.Objects.firstNonNull;
import static com.google.common.base.Preconditions.checkNotNull;
import static java.lang.String.format;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.MINUTES;
import static java.util.concurrent.TimeUnit.SECONDS;

public class ScheduledCombiner
{
    private static final Logger log = Logger.get(ScheduledCombiner.class);
    private static final long PER_TYPE_CHECK_DELAY_MILLIS = SECONDS.toMillis(10);
    private static final long CHECK_DELAY_MILLIS = MINUTES.toMillis(10);

    private final Set<String> highPriorityEventTypes;
    private final Set<String> lowPriorityEventTypes;
    private final StoredObjectCombiner objectCombiner;
    private final ScheduledExecutorService executor;
    private final ScheduledExecutorService highPriorityExecutor;
    private final ScheduledExecutorService lowPriorityExecutor;
    private final Set<String> eventTypes;
    private final boolean enabled;

    @Inject
    public ScheduledCombiner(StoredObjectCombiner objectCombiner,
            @Named("ScheduledCombinerExecutor") ScheduledExecutorService executorService,
            @Named("ScheduledCombinerHighPriorityExecutor") ScheduledExecutorService highPriorityExecutorService,
            @Named("ScheduledCombinerLowPriorityExecutor") ScheduledExecutorService lowPriorityExecutorService,
            ServerConfig config)
    {
        checkNotNull(config, "config is null");
        this.highPriorityEventTypes = emptySetFromNull(config.getCombinerHighPriorityEventTypes());
        this.lowPriorityEventTypes = emptySetFromNull(config.getCombinerLowPriorityEventTypes());
        this.objectCombiner = checkNotNull(objectCombiner, "objectCombiner is null");
        this.executor = checkNotNull(executorService, "executorService is null");
        this.highPriorityExecutor = checkPriorityExecutorService(highPriorityEventTypes, highPriorityExecutorService, "highPriorityExecutorService");
        this.lowPriorityExecutor = checkPriorityExecutorService(lowPriorityEventTypes, lowPriorityExecutorService, "lowPriorityExecutorService");

        this.enabled = config.isCombinerEnabled();

        this.eventTypes = new HashSet<String>();
    }

    @PostConstruct
    public void start()
    {
        if (!enabled) {
            return;
        }

        executor.scheduleAtFixedRate(new Runnable()
        {
            @Override
            public void run()
            {
                try {
                    scheduleNewTypes();
                }
                catch (Exception e) {
                    log.error(e, "checking for new types failed");
                }
            }
        }, 0, CHECK_DELAY_MILLIS, MILLISECONDS);
    }

    @PreDestroy
    public void destroy()
            throws IOException
    {
        executor.shutdown();
    }

    private static ImmutableSet<String> emptySetFromNull(Set<String> set)
    {
        return ImmutableSet.copyOf(firstNonNull(set, ImmutableSet.<String>of()));
    }

    private static ScheduledExecutorService checkPriorityExecutorService(Set<String> priorityEventTypes, ScheduledExecutorService priorityExecutorService, String priorityExecutorServiceName)
    {
        return priorityEventTypes.isEmpty() ? null : checkNotNull(priorityExecutorService, format("%s is null", priorityExecutorServiceName));
    }

    private void scheduleNewTypes()
    {
        for (final String eventType : objectCombiner.listEventTypes()) {
            if (eventTypes.contains(eventType)) {
                continue;
            }

            selectExecutorForEventType(eventType).scheduleWithFixedDelay(
                    new Runnable()
                    {
                        @Override
                        public void run()
                        {
                            try {
                                objectCombiner.combineObjects(eventType);
                            }
                            catch (Exception e) {
                                log.error(e, "combining objects of type %s failed", eventType);
                            }
                        }
                    }, 0, PER_TYPE_CHECK_DELAY_MILLIS, MILLISECONDS);
            eventTypes.add(eventType);
        }
    }

    private ScheduledExecutorService selectExecutorForEventType(String eventType)
    {
        if (highPriorityEventTypes.contains(eventType)) {
            return checkNotNull(highPriorityExecutor, "highPriorityExecutor is null");
        }
        else if (lowPriorityEventTypes.contains(eventType)) {
            return checkNotNull(lowPriorityExecutor, "lowPriorityExecutor is null");
        }
        else {
            return executor;
        }
    }
}
