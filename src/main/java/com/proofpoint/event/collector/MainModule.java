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

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.transfer.TransferManager;
import com.google.common.base.Ticker;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.google.inject.*;
import com.proofpoint.discovery.client.announce.AnnouncementHttpServerInfo;
import com.proofpoint.discovery.client.announce.ServiceAnnouncement;
import com.proofpoint.discovery.client.announce.ServiceAnnouncement.ServiceAnnouncementBuilder;
import com.proofpoint.event.client.EventClient;
import com.proofpoint.event.collector.combiner.CombineCompleted;
import com.proofpoint.event.collector.combiner.CombineObjectMetadataStore;
import com.proofpoint.event.collector.combiner.S3CombineObjectMetadataStore;
import com.proofpoint.event.collector.combiner.S3StorageSystem;
import com.proofpoint.event.collector.combiner.ScheduledCombiner;
import com.proofpoint.event.collector.combiner.StorageSystem;
import com.proofpoint.event.collector.combiner.StoredObjectCombiner;
import org.weakref.jmx.ObjectNameBuilder;

import javax.inject.Named;
import javax.inject.Singleton;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;

import static com.google.inject.multibindings.Multibinder.newSetBinder;
import static com.proofpoint.configuration.ConfigurationModule.bindConfig;
import static com.proofpoint.discovery.client.DiscoveryBinder.discoveryBinder;
import static com.proofpoint.event.client.EventBinder.eventBinder;
import static com.proofpoint.jaxrs.JaxrsBinder.jaxrsBinder;
import static com.proofpoint.reporting.ReportBinder.reportBinder;
import static java.util.concurrent.Executors.newScheduledThreadPool;
import static java.util.concurrent.Executors.newSingleThreadScheduledExecutor;
import static org.weakref.jmx.guice.ExportBinder.newExporter;

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

        binder.bind(StoredObjectCombiner.class).in(Scopes.SINGLETON);
        newExporter(binder).export(StoredObjectCombiner.class).withGeneratedName();

        binder.bind(ScheduledCombiner.class).in(Scopes.SINGLETON);

        binder.bind(StorageSystem.class).to(S3StorageSystem.class).in(Scopes.SINGLETON);
        binder.bind(CombineObjectMetadataStore.class).to(S3CombineObjectMetadataStore.class).in(Scopes.SINGLETON);

        bindConfig(binder).to(ServerConfig.class);
        bindConfig(binder).to(StaticEventTapConfig.class);

        eventBinder(binder).bindEventClient(CombineCompleted.class);

        jaxrsBinder(binder).bind(EventResource.class);
        jaxrsBinder(binder).bind(EventWriterStatsResource.class);

        reportBinder(binder).bindReportCollection(EventCollectorStats.class).as(new ObjectNameBuilder(EventCollectorStats.class.getPackage().getName()).withProperty("type", "EventCollector").build());

        reportBinder(binder).bindReportCollection(S3UploaderStats.class).as(new ObjectNameBuilder(S3UploaderStats.class.getPackage().getName()).withProperty("type", "S3Uploader").build());

        discoveryBinder(binder).bindServiceAnnouncement(CollectorHttpAnnouncementProvider.class);
    }

    @Provides
    @Singleton
    private AmazonS3 provideAmazonS3(AWSCredentials credentials)
    {
        return new AmazonS3Client(credentials);
    }

    @Provides
    @Singleton
    private TransferManager provideAmazonS3TransferManager(AWSCredentials credentials)
    {
        return new TransferManager(credentials);
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

    @Provides
    @Singleton
    @Named("ScheduledCombinerExecutor")
    private ScheduledExecutorService createScheduledCombinerExecutor(ServerConfig config)
    {
        return newScheduledThreadPool(
                config.getCombinerThreadCount(),
                new ThreadFactoryBuilder()
                        .setNameFormat("StoredObjectCombiner-%s")
                        .setDaemon(true)
                        .build());
    }

    @Provides
    @Singleton
    @Named("ScheduledCombinerHighPriorityExecutor")
    private ScheduledExecutorService createScheduledCombinerHighPriorityExecutor(ServerConfig config)
    {
        int highPriorityTypeCount = config.getCombinerHighPriorityEventTypes().size();
        ThreadFactoryBuilder threadFactoryBuilder = new ThreadFactoryBuilder().setDaemon(true);
        if (highPriorityTypeCount == 0) {
            return newSingleThreadScheduledExecutor(threadFactoryBuilder.setNameFormat("StoredObjectCombiner-High-Disabled-Should-Not-Be-Used").build());
        }
        else {
            return newScheduledThreadPool(highPriorityTypeCount, threadFactoryBuilder.setNameFormat("StoredObjectCombiner-High-%s").build());
        }
    }

    @Provides
    @Singleton
    @Named("ScheduledCombinerLowPriorityExecutor")
    private ScheduledExecutorService createScheduledCombinerLowPriorityExecutor(ServerConfig config)
    {
        ThreadFactoryBuilder threadFactoryBuilder = new ThreadFactoryBuilder().setDaemon(true);
        if (config.getCombinerLowPriorityEventTypes().size() <= 0) {
            threadFactoryBuilder.setNameFormat("StoredObjectCombiner-Low-Disabled-Should-Not-Be-Used");
        }
        else {
            threadFactoryBuilder.setNameFormat("StoredObjectCombiner-Low-%s");
        }

        return newSingleThreadScheduledExecutor(threadFactoryBuilder.build());
    }

    @Provides
    @Singleton
    private Ticker providesTicker()
    {
        return Ticker.systemTicker();
    }


    static class CollectorHttpAnnouncementProvider implements Provider<ServiceAnnouncement>
    {
        private final String serviceType;
        private AnnouncementHttpServerInfo httpServerInfo;

        @Inject
        CollectorHttpAnnouncementProvider(ServerConfig config)
        {
            this.serviceType = config.getServiceType();
        }

        @Inject
        public void setAnnouncementHttpServerInfo(AnnouncementHttpServerInfo httpServerInfo)
        {
            this.httpServerInfo = httpServerInfo;
        }

        @Override
        public ServiceAnnouncement get()
        {
            ServiceAnnouncementBuilder builder = ServiceAnnouncement.serviceAnnouncement(serviceType);

            if (httpServerInfo.getHttpUri() != null) {
                builder.addProperty("http", httpServerInfo.getHttpUri().toString());
                builder.addProperty("http-external", httpServerInfo.getHttpExternalUri().toString());
            }
            if (httpServerInfo.getHttpsUri() != null) {
                builder.addProperty("https", httpServerInfo.getHttpsUri().toString());
            }
            return builder.build();
        }
    }
}
