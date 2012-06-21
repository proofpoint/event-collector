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

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.google.inject.Binder;
import com.google.inject.Module;
import com.google.inject.Provides;
import com.google.inject.Scopes;
import org.weakref.jmx.guice.MBeanModule;

import javax.inject.Singleton;

import static com.proofpoint.configuration.ConfigurationModule.bindConfig;
import static com.proofpoint.event.client.EventBinder.eventBinder;

public class CombinerMainModule
        implements Module
{
    public void configure(Binder binder)
    {
        binder.requireExplicitBindings();
        binder.disableCircularProxies();

        binder.bind(StorageSystem.class).to(S3StorageSystem.class).in(Scopes.SINGLETON);

        binder.bind(StoredObjectCombiner.class).in(Scopes.SINGLETON);
        MBeanModule.newExporter(binder).export(StoredObjectCombiner.class).withGeneratedName();

        binder.bind(ScheduledCombiner.class).in(Scopes.SINGLETON);

        binder.bind(StorageSystem.class).to(S3StorageSystem.class).in(Scopes.SINGLETON);
        binder.bind(CombineObjectMetadataStore.class).to(S3CombineObjectMetadataStore.class).in(Scopes.SINGLETON);

        bindConfig(binder).to(CombinerConfig.class);

        eventBinder(binder).bindEventClient(CombineCompleted.class);
    }

    @Provides
    @Singleton
    private AmazonS3 provideAmazonS3(AWSCredentials credentials)
    {
        return new AmazonS3Client(credentials);
    }

    @Provides
    @Singleton
    private AWSCredentials provideProviderCredentials(CombinerConfig config)
    {
        return new BasicAWSCredentials(config.getAwsAccessKey(), config.getAwsSecretKey());
    }
}
