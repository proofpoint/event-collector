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
package com.proofpoint.event.collector.queue;

import com.google.inject.Inject;
import com.proofpoint.event.collector.BatchProcessorConfig;
import com.proofpoint.event.collector.Event;
import com.proofpoint.json.JsonCodec;
import com.proofpoint.reporting.ReportExporter;
import org.weakref.jmx.ObjectNameBuilder;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import static com.proofpoint.json.JsonCodec.jsonCodec;
import static org.weakref.jmx.internal.guava.base.Preconditions.checkArgument;
import static org.weakref.jmx.internal.guava.base.Preconditions.checkNotNull;

public class QueueFactory
{
    public static final JsonCodec<Event> EVENT_CODEC = jsonCodec(Event.class);

    private final BatchProcessorConfig config;
    private final ReportExporter reportExporter;
    private final Map<String, Queue<Event>> map;
    private final Object lock = new Object();

    @Inject
    public QueueFactory(BatchProcessorConfig config, ReportExporter reportExporter)
    {
        this.config = checkNotNull(config, "config is null");
        this.reportExporter = checkNotNull(reportExporter, "reportExporter is null");

        map = new HashMap<>();
    }

    public Queue<Event> create(String name)
            throws IOException
    {
        checkArgument(name != null && !name.isEmpty(), "name is null or empty");

        Queue<Event> queue;
        synchronized (lock) {
            queue = map.get(name);
            if (queue == null) {
                queue = new FileBackedQueue<>(name, config.getDataDirectory(), EVENT_CODEC, config.getQueueSize());
                setupQueueMetric(name, queue);
                map.put(name, queue);
            }
        }

        return queue;
    }

    private void setupQueueMetric(String flowId, Queue<Event> queue)
    {
        String metricName = new ObjectNameBuilder("com.proofpoint.event.colllector")
                .withProperty("type", "EventCollector")
                .withProperty("name", "Queue")
                .withProperty("flowId", flowId)
                .build();

        reportExporter.export(metricName, queue);
    }
}
