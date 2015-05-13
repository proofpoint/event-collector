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

import com.proofpoint.event.collector.BatchProcessor.BatchHandler;
import com.proofpoint.event.collector.queue.QueueFactory;

import javax.inject.Inject;
import java.io.IOException;

import static com.google.common.base.Preconditions.checkNotNull;

public class BatchProcessorFactoryImpl implements BatchProcessorFactory
{
    private final BatchProcessorConfig config;
    private QueueFactory<Event> queueFactory;

    @Inject
    public BatchProcessorFactoryImpl(BatchProcessorConfig config, QueueFactory<Event> queueFactory)
    {
        this.config = checkNotNull(config, "config is null");
        this.queueFactory = checkNotNull(queueFactory, "queueFactory is null");
    }

    @Override
    public BatchProcessor<Event> createBatchProcessor(String name, BatchHandler<Event> batchHandler)
            throws IOException
    {
        return new AsyncBatchProcessor<>(name, batchHandler, config, queueFactory);
    }
}
