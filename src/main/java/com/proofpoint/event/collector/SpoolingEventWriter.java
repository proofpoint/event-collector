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

import com.fasterxml.jackson.core.JsonEncoding;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.util.MinimalPrettyPrinter;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.io.CountingOutputStream;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.proofpoint.event.collector.EventCounters.CounterState;
import com.proofpoint.log.Logger;
import com.proofpoint.units.DataSize;
import com.proofpoint.units.Duration;
import org.iq80.snappy.SnappyOutputStream;

import javax.annotation.Nullable;
import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import javax.inject.Inject;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public class SpoolingEventWriter
        implements EventWriter
{
    private static final Duration CHECK_DELAY = new Duration(5, TimeUnit.SECONDS);
    private static final Logger log = Logger.get(SpoolingEventWriter.class);

    private final ScheduledExecutorService executor = Executors.newScheduledThreadPool(1,
            new ThreadFactoryBuilder().setNameFormat("SpoolingEventWriter-%s").build());

    private final Uploader uploader;
    private final EventPartitioner partitioner;
    private final ObjectMapper objectMapper;
    private final Duration maxBufferTime;
    private final DataSize targetFileSize;
    private final EventCounters<String> counters = new EventCounters<>();

    private final LoadingCache<EventPartition, OutputPartition> outputFiles = CacheBuilder.newBuilder()
            .build(new CacheLoader<EventPartition, OutputPartition>()
            {
                @Override
                public OutputPartition load(@Nullable EventPartition partition)
                {
                    return new OutputPartition(partition, uploader, objectMapper, targetFileSize, maxBufferTime);
                }
            });

    @Inject
    public SpoolingEventWriter(Uploader uploader, EventPartitioner partitioner, ObjectMapper objectMapper, ServerConfig config)
    {
        this.uploader = uploader;
        this.partitioner = partitioner;
        this.objectMapper = objectMapper;
        this.maxBufferTime = config.getMaxBufferTime();
        this.targetFileSize = config.getTargetFileSize();
    }

    @PostConstruct
    public void start()
    {
        // roll files that have been open too long
        Runnable closer = new Runnable()
        {
            @Override
            public void run()
            {
                for (OutputPartition partition : outputFiles.asMap().values()) {
                    try {
                        if (partition.isAtMaxAge()) {
                            partition.close();
                        }
                    }
                    catch (IOException e) {
                        log.error(e, "close output partition failed");
                    }
                }
            }
        };
        executor.scheduleAtFixedRate(closer, 0, (long) CHECK_DELAY.toMillis(), TimeUnit.MILLISECONDS);
    }

    @PreDestroy
    public void destroy()
            throws IOException
    {
        executor.shutdown();
        for (OutputPartition partition : outputFiles.asMap().values()) {
            partition.close();
        }
    }

    @Override
    public void write(Event event)
            throws IOException
    {
        EventPartition partition = partitioner.getPartition(event);

        outputFiles.getUnchecked(partition).write(event);
        counters.recordReceived(event.getType(), 1);
    }

    @Override
    public void distribute(Event event)
            throws IOException
    {
        // do nothing
    }

    public Map<String, CounterState> getCounts()
    {
        return counters.getCounts();
    }

    public void clearCounts()
    {
        counters.resetCounts();
    }

    private static class OutputPartition
    {
        private final EventPartition eventPartition;
        private final Uploader uploader;
        private final ObjectMapper objectMapper;
        private final DataSize targetFileSize;
        private final Duration maxBufferTime;

        private File file;
        private CountingOutputStream output;
        private JsonGenerator generator;
        private long createdTime;
        private boolean hasContent;

        public OutputPartition(EventPartition eventPartition,
                Uploader uploader,
                ObjectMapper objectMapper,
                DataSize targetFileSize,
                Duration maxBufferTime)
        {
            this.eventPartition = eventPartition;
            this.uploader = uploader;
            this.objectMapper = objectMapper;
            this.targetFileSize = targetFileSize;
            this.maxBufferTime = maxBufferTime;
        }

        private synchronized void open()
                throws IOException
        {
            if (generator != null) {
                return;
            }

            file = uploader.generateNextFilename();
            output = new CountingOutputStream(new FileOutputStream(file));
            OutputStream snappyOut = new SnappyOutputStream(output);

            generator = objectMapper.getFactory().createGenerator(snappyOut, JsonEncoding.UTF8);
            generator.disable(JsonGenerator.Feature.FLUSH_PASSED_TO_STREAM);
            generator.setPrettyPrinter(new MinimalPrettyPrinter("\n"));

            createdTime = System.nanoTime();
            hasContent = false;
        }

        public synchronized void write(Event event)
                throws IOException
        {
            open();

            generator.writeObject(event);

            hasContent = true;

            // roll file if it is over the target size or max age
            if ((output.getCount() >= targetFileSize.toBytes()) || isAtMaxAge()) {
                close();
            }
        }

        public synchronized void close()
                throws IOException
        {
            if (generator == null) {
                return;
            }

            // add final newline
            if (hasContent) {
                generator.writeRaw('\n');
            }

            generator.close();

            uploader.enqueueUpload(eventPartition, file);

            file = null;
            output = null;
            generator = null;
        }

        public synchronized boolean isAtMaxAge()
        {
            return (generator != null) && (Duration.nanosSince(createdTime).compareTo(maxBufferTime) > 0);
        }
    }
}
