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
package com.proofpoint.event.collector;

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
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
import org.weakref.jmx.guice.MBeanModule;

import javax.inject.Singleton;

import static com.google.inject.multibindings.Multibinder.newSetBinder;
import static com.proofpoint.configuration.ConfigurationModule.bindConfig;
import static com.proofpoint.discovery.client.DiscoveryBinder.discoveryBinder;
import static com.proofpoint.event.client.EventBinder.eventBinder;

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
        MBeanModule.newExporter(binder).export(SpoolingEventWriter.class).withGeneratedName();
        newSetBinder(binder, EventWriter.class).addBinding().to(Key.get(SpoolingEventWriter.class)).in(Scopes.SINGLETON);

        binder.bind(EventResource.class).in(Scopes.SINGLETON);

        binder.bind(StoredObjectCombiner.class).in(Scopes.SINGLETON);
        MBeanModule.newExporter(binder).export(StoredObjectCombiner.class).withGeneratedName();

        binder.bind(ScheduledCombiner.class).in(Scopes.SINGLETON);

        binder.bind(StorageSystem.class).to(S3StorageSystem.class).in(Scopes.SINGLETON);
        binder.bind(CombineObjectMetadataStore.class).to(S3CombineObjectMetadataStore.class).in(Scopes.SINGLETON);

        bindConfig(binder).to(ServerConfig.class);

        eventBinder(binder).bindEventClient(CombineCompleted.class);

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
}
