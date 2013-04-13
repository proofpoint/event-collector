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
import com.proofpoint.units.Duration;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import javax.inject.Inject;
import javax.inject.Named;
import java.io.IOException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import static com.google.common.base.Preconditions.checkNotNull;

public class ScheduledCombiner
{
    private static final Logger log = Logger.get(ScheduledCombiner.class);

    private static final Duration CHECK_DELAY = new Duration(10, TimeUnit.SECONDS);

    private final StoredObjectCombiner objectCombiner;
    private final ScheduledExecutorService executor;
    private final boolean enabled;

    @Inject
    public ScheduledCombiner(StoredObjectCombiner objectCombiner, @Named("ScheduledCombinerExecutor") ScheduledExecutorService executorService, ServerConfig config)
    {
        this.objectCombiner = checkNotNull(objectCombiner, "objectCombiner is null");
        this.executor = checkNotNull(executorService, "executorService is null");
        this.enabled = checkNotNull(config, "config is null").isCombinerEnabled();
    }

    @PostConstruct
    public void start()
    {
        if (!enabled) {
            return;
        }
        Runnable combiner = new Runnable()
        {
            @Override
            public void run()
            {
                try {
                    objectCombiner.combineAllObjects();
                }
                catch (Exception e) {
                    log.error(e, "combine failed");
                }
            }
        };
        executor.scheduleAtFixedRate(combiner, 0, (long) CHECK_DELAY.toMillis(), TimeUnit.MILLISECONDS);
    }

    @PreDestroy
    public void destroy()
            throws IOException
    {
        executor.shutdown();
    }
}
