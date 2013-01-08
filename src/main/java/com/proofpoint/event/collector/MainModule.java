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

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.google.inject.Binder;
import com.google.inject.Key;
import com.google.inject.Module;
import com.google.inject.Provides;
import com.google.inject.Scopes;
import com.proofpoint.event.client.EventClient;
import com.proofpoint.event.collector.combiner.CombineCompleted;
import com.proofpoint.event.collector.combiner.CombineObjectMetadataStore;
import com.proofpoint.event.collector.combiner.S3CombineObjectMetadataStore;
import com.proofpoint.event.collector.combiner.S3StorageSystem;
import com.proofpoint.event.collector.combiner.ScheduledCombiner;
import com.proofpoint.event.collector.combiner.StorageSystem;
import com.proofpoint.event.collector.combiner.StoredObjectCombiner;
import com.proofpoint.event.collector.stats.CollectorStats;
import java.util.concurrent.ExecutorService;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import javax.inject.Singleton;

import static com.google.inject.multibindings.Multibinder.newSetBinder;
import static com.proofpoint.configuration.ConfigurationModule.bindConfig;
import static com.proofpoint.discovery.client.DiscoveryBinder.discoveryBinder;
import static com.proofpoint.event.client.EventBinder.eventBinder;
import static com.proofpoint.event.collector.ProcessStats.HourlyEventCount;
import static org.weakref.jmx.guice.MBeanModule.newExporter;

public class MainModule
        implements Module
{
    public void configure(Binder binder)
    {
        binder.requireExplicitBindings();
        binder.disableCircularProxies();

        binder.bind(EventClient.class).to(LocalEventClient.class).in(Scopes.SINGLETON);

        binder.bind(EventPartitioner.class).in(Scopes.SINGLETON);
        binder.bind(Uploader.class).to(S3Uploader.class).in(Scopes.SINGLETON);

        binder.bind(StorageSystem.class).to(S3StorageSystem.class).in(Scopes.SINGLETON);

        binder.bind(SpoolingEventWriter.class).in(Scopes.SINGLETON);
        newExporter(binder).export(SpoolingEventWriter.class).withGeneratedName();
        newSetBinder(binder, EventWriter.class).addBinding().to(Key.get(SpoolingEventWriter.class)).in(Scopes.SINGLETON);

        binder.bind(EventResource.class).in(Scopes.SINGLETON);

        binder.bind(StoredObjectCombiner.class).in(Scopes.SINGLETON);
        newExporter(binder).export(StoredObjectCombiner.class).withGeneratedName();

        binder.bind(ScheduledCombiner.class).in(Scopes.SINGLETON);

        binder.bind(StorageSystem.class).to(S3StorageSystem.class).in(Scopes.SINGLETON);
        binder.bind(CombineObjectMetadataStore.class).to(S3CombineObjectMetadataStore.class).in(Scopes.SINGLETON);

        bindConfig(binder).to(ServerConfig.class);

        eventBinder(binder).bindEventClient(CombineCompleted.class);

        binder.bind(EventWriterStatsResource.class).in(Scopes.SINGLETON);

        eventBinder(binder).bindEventClient(HourlyEventCount.class);

        binder.bind(CollectorStats.class).in(Scopes.SINGLETON);
        newExporter(binder).export(CollectorStats.class).withGeneratedName();

        discoveryBinder(binder).bindHttpAnnouncement("collector");
    }

    @Provides
    @Singleton
    private AmazonS3 provideAmazonS3(AWSCredentials credentials)
    {
        return new AmazonS3Client(credentials);
    }

    @Provides
    @Singleton
    private AWSCredentials provideProviderCredentials(ServerConfig config)
    {
        return new BasicAWSCredentials(config.getAwsAccessKey(), config.getAwsSecretKey());
    }

    @Provides
    @Singleton
    private ScheduledExecutorService createScheduledExecutor()
    {
        return Executors.newSingleThreadScheduledExecutor();
    }

    @Provides
    @Singleton
    @UploaderExecutorService
    private ExecutorService createUploaderExecutor(ServerConfig config)
    {
        return Executors.newFixedThreadPool(config.getMaxUploadThreads(),
                new ThreadFactoryBuilder().setNameFormat("S3Uploader-%s").build());
    }

    @Provides
    @Singleton
    @PendingFileExecutorService
     private ScheduledExecutorService createPendingFileExecutor()
    {
        return Executors.newSingleThreadScheduledExecutor();
    }
}
