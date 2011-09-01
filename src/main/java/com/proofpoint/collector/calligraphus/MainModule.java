/*
 * Copyright 2010 Proofpoint, Inc.
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
package com.proofpoint.collector.calligraphus;

import com.google.inject.Binder;
import com.google.inject.Module;
import com.google.inject.Provides;
import com.google.inject.Scopes;
import com.proofpoint.collector.calligraphus.combiner.ExtendedRestS3Service;
import com.proofpoint.collector.calligraphus.combiner.S3StorageSystem;
import com.proofpoint.collector.calligraphus.combiner.StorageSystem;
import org.jets3t.service.S3ServiceException;
import org.jets3t.service.security.AWSCredentials;
import org.jets3t.service.security.ProviderCredentials;
import org.weakref.jmx.guice.MBeanModule;

import javax.inject.Singleton;

import static com.proofpoint.configuration.ConfigurationModule.bindConfig;
import static com.proofpoint.discovery.client.DiscoveryBinder.discoveryBinder;

public class MainModule
        implements Module
{
    public void configure(Binder binder)
    {
        binder.requireExplicitBindings();
        binder.disableCircularProxies();

        binder.bind(EventPartitioner.class).in(Scopes.SINGLETON);
        binder.bind(S3Uploader.class).in(Scopes.SINGLETON);

        binder.bind(StorageSystem.class).to(S3StorageSystem.class).in(Scopes.SINGLETON);

        binder.bind(EventWriter.class).to(S3EventWriter.class).in(Scopes.SINGLETON);
        MBeanModule.newExporter(binder).export(S3EventWriter.class).withGeneratedName();

        binder.bind(EventResource.class).in(Scopes.SINGLETON);

        bindConfig(binder).to(ServerConfig.class);

        discoveryBinder(binder).bindHttpAnnouncement("collector");
    }

    @Provides
    @Singleton
    private ExtendedRestS3Service provideExtendedRestS3Service(ProviderCredentials credentials)
            throws S3ServiceException
    {
        return new ExtendedRestS3Service(credentials);
    }

    @Provides
    @Singleton
    private ProviderCredentials provideProviderCredentials(ServerConfig config)
    {
        return new AWSCredentials(config.getAwsAccessKey(), config.getAwsSecretKey());
    }
}
