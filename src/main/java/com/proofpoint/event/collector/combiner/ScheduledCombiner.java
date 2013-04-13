/*
 * Copyright 2011-2013 Proofpoint, Inc.
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

import static com.google.common.base.Preconditions.checkNotNull;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.MINUTES;
import static java.util.concurrent.TimeUnit.SECONDS;

public class ScheduledCombiner
{
    private static final Logger log = Logger.get(ScheduledCombiner.class);
    private static final long PER_TYPE_CHECK_DELAY_MILLIS = SECONDS.toMillis(10);
    private static final long CHECK_DELAY_MILLIS = MINUTES.toMillis(10);

    private final StoredObjectCombiner objectCombiner;
    private final ScheduledExecutorService executor;
    private final Set<String> eventTypes;
    private final boolean enabled;

    @Inject
    public ScheduledCombiner(StoredObjectCombiner objectCombiner, @Named("ScheduledCombinerExecutor") ScheduledExecutorService executorService, ServerConfig config)
    {
        this.objectCombiner = checkNotNull(objectCombiner, "objectCombiner is null");
        this.executor = checkNotNull(executorService, "executorService is null");
        this.enabled = checkNotNull(config, "config is null").isCombinerEnabled();

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

    private void scheduleNewTypes()
    {
        for (final String eventType : objectCombiner.listEventTypes()) {
            if (eventTypes.contains(eventType)) {
                continue;
            }

            executor.scheduleWithFixedDelay(
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
}
