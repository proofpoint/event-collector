/*
 * Copyright 2011 Proofpoint, Inc.
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
package com.proofpoint.event.combiner;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.proofpoint.log.Logger;
import com.proofpoint.units.Duration;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import javax.inject.Inject;
import java.io.IOException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import static java.util.concurrent.Executors.newSingleThreadScheduledExecutor;

public class ScheduledCombiner
{
    private static final Logger log = Logger.get(ScheduledCombiner.class);

    private static final Duration CHECK_DELAY = new Duration(10, TimeUnit.SECONDS);

    private final ScheduledExecutorService executor = newSingleThreadScheduledExecutor(
            new ThreadFactoryBuilder().setNameFormat("StoredObjectCombiner-%s").setDaemon(true).build());

    private final StoredObjectCombiner objectCombiner;

    @Inject
    public ScheduledCombiner(StoredObjectCombiner objectCombiner)
    {
        this.objectCombiner = objectCombiner;
    }

    @PostConstruct
    public void start()
    {
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
