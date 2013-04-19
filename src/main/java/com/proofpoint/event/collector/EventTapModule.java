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
package com.proofpoint.event.collector;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.google.inject.Binder;
import com.google.inject.Key;
import com.google.inject.Module;
import com.google.inject.Provides;
import org.weakref.jmx.guice.MBeanModule;

import java.util.concurrent.ScheduledExecutorService;

import static com.google.inject.Scopes.SINGLETON;
import static com.google.inject.multibindings.Multibinder.newSetBinder;
import static com.proofpoint.configuration.ConfigurationModule.bindConfig;
import static com.proofpoint.discovery.client.DiscoveryBinder.discoveryBinder;
import static com.proofpoint.http.client.HttpClientBinder.httpClientBinder;
import static com.proofpoint.json.JsonCodecBinder.jsonCodecBinder;
import static java.util.concurrent.Executors.newSingleThreadScheduledExecutor;

public class EventTapModule implements Module
{
    @Override
    public void configure(Binder binder)
    {
        discoveryBinder(binder).bindSelector("eventTap");
        bindConfig(binder).to(EventTapConfig.class);
        jsonCodecBinder(binder).bindListJsonCodec(Event.class);

        bindConfig(binder).to(BatchProcessorConfig.class);
        binder.bind(BatchProcessorFactory.class).to(BatchProcessorFactoryImpl.class);
        binder.bind(BatchProcessorFactoryImpl.class).in(SINGLETON);

        binder.bind(EventTapFlowFactory.class).to(HttpEventTapFlowFactory.class);
        binder.bind(HttpEventTapFlowFactory.class).in(SINGLETON);
        binder.bind(EventTapWriter.class).in(SINGLETON);
        binder.bind(EventTapStats.class).to(EventTapWriter.class);

        httpClientBinder(binder).bindHttpClient("EventTap", EventTap.class);

        MBeanModule.newExporter(binder).export(EventTapWriter.class).withGeneratedName();
        newSetBinder(binder, EventWriter.class).addBinding().to(Key.get(EventTapWriter.class)).in(SINGLETON);

        binder.bind(EventTapStatsResource.class).in(SINGLETON);
    }

    @Provides
    @EventTap
    public ScheduledExecutorService createExecutor()
    {
        return newSingleThreadScheduledExecutor(new ThreadFactoryBuilder().setNameFormat("event-tap-%s").build());
    }
}
