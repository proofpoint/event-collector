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

import com.google.common.base.Charsets;
import com.google.common.collect.ImmutableMap;
import com.google.common.io.Files;
import com.google.common.io.Resources;
import com.google.inject.Injector;
import com.proofpoint.bootstrap.Bootstrap;
import com.proofpoint.bootstrap.LifeCycleManager;
import com.proofpoint.discovery.client.testing.TestingDiscoveryModule;
import com.proofpoint.event.client.JsonEventModule;
import com.proofpoint.http.client.HttpClient;
import com.proofpoint.http.client.StatusResponseHandler.StatusResponse;
import com.proofpoint.http.client.StringResponseHandler.StringResponse;
import com.proofpoint.http.client.jetty.JettyHttpClient;
import com.proofpoint.http.server.testing.TestingHttpServer;
import com.proofpoint.http.server.testing.TestingHttpServerModule;
import com.proofpoint.jaxrs.JaxrsModule;
import com.proofpoint.jmx.testing.TestingJmxModule;
import com.proofpoint.json.JsonCodec;
import com.proofpoint.json.JsonModule;
import com.proofpoint.node.testing.TestingNodeModule;
import com.proofpoint.reporting.ReportingModule;
import com.proofpoint.testing.FileUtils;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;
import org.weakref.jmx.guice.MBeanModule;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.util.concurrent.ExecutionException;

import static com.proofpoint.http.client.Request.Builder.prepareDelete;
import static com.proofpoint.http.client.Request.Builder.prepareGet;
import static com.proofpoint.http.client.Request.Builder.preparePost;
import static com.proofpoint.http.client.StaticBodyGenerator.createStaticBodyGenerator;
import static com.proofpoint.http.client.StatusResponseHandler.createStatusResponseHandler;
import static com.proofpoint.http.client.StringResponseHandler.createStringResponseHandler;
import static javax.ws.rs.core.HttpHeaders.CONTENT_TYPE;
import static javax.ws.rs.core.MediaType.APPLICATION_JSON;
import static javax.ws.rs.core.Response.Status;
import static org.testng.Assert.assertEquals;

public class TestServer
{
    private JsonCodec<Object> OBJECT_CODEC = JsonCodec.jsonCodec(Object.class);
    private HttpClient client;
    private TestingHttpServer server;
    private File tempStageDir;
    private EventTapWriter eventTapWriter;
    private LifeCycleManager lifeCycleManager;

    @BeforeMethod
    public void setup()
            throws Exception
    {
        tempStageDir = Files.createTempDir();

        ImmutableMap<String, String> config = ImmutableMap.<String, String>builder()
                .put("collector.accepted-event-types", "Test")
                .put("collector.local-staging-directory", tempStageDir.getAbsolutePath())
                .put("collector.aws-access-key", "fake-aws-access-key")
                .put("collector.aws-secret-key", "fake-aws-secret-key")
                .put("collector.s3-staging-location", "s3://test-staging/")
                .put("collector.s3-data-location", "s3://test-data/")
                .put("collector.s3-metadata-location", "s3://test-metadata/")
                .put("collector.event-tap.data-directory", "target")
                .build();

        Bootstrap app = Bootstrap.bootstrapApplication("event-collector")
                .doNotInitializeLogging()
                .withModules(
                        new TestingNodeModule(),
                        new TestingHttpServerModule(),
                        new TestingDiscoveryModule(),
                        new TestingJmxModule(),
                        new JsonModule(),
                        new JaxrsModule(),
                        new JsonEventModule(),
                        new EventTapModule(),
                        new ReportingModule(),
                        new MBeanModule(),
                        new MainModule());

        Injector injector = app
                .setRequiredConfigurationProperties(config)
                .initialize();

        lifeCycleManager = injector.getInstance(LifeCycleManager.class);

        server = injector.getInstance(TestingHttpServer.class);
        eventTapWriter = injector.getInstance(EventTapWriter.class);
        eventTapWriter.start();
        client = new JettyHttpClient();

    }

    @AfterMethod
    public void teardown()
            throws Exception
    {
        if (eventTapWriter != null) {
            eventTapWriter.stop();
        }

        if (lifeCycleManager != null) {
            lifeCycleManager.stop();
        }

        if (client != null) {
            client.close();
        }

        FileUtils.deleteRecursively(tempStageDir);
    }

    @Test
    public void testPostSingle()
            throws IOException, ExecutionException, InterruptedException
    {
        String json = Resources.toString(Resources.getResource("single.json"), Charsets.UTF_8);
        StatusResponse response = client.execute(preparePost()
                .setUri(urlFor("/v2/event"))
                .setHeader("Content-Type", APPLICATION_JSON)
                        .setBodySource(createStaticBodyGenerator(json, Charsets.UTF_8))
                .build(),
                createStatusResponseHandler());

        assertEquals(response.getStatusCode(), Status.ACCEPTED.getStatusCode());
    }

    @Test
    public void testPostMultiple()
            throws IOException, ExecutionException, InterruptedException
    {
        String json = Resources.toString(Resources.getResource("multiple.json"), Charsets.UTF_8);
        StatusResponse response = client.execute(
                preparePost().setUri(urlFor("/v2/event"))
                        .setHeader("Content-Type", APPLICATION_JSON)
                        .setBodySource(createStaticBodyGenerator(json.getBytes(Charsets.UTF_8)))
                        .build(),
                createStatusResponseHandler());

        assertEquals(response.getStatusCode(), Status.ACCEPTED.getStatusCode());
    }

    @Test
    public void testGetSpoolCounts()
            throws Exception
    {
        StringResponse response = client.execute(prepareGet()
                .setUri(urlFor("/v1/spool/stats")).build(), createStringResponseHandler());

        assertEquals(response.getStatusCode(), Status.OK.getStatusCode());
        assertEquals(response.getHeader(CONTENT_TYPE), APPLICATION_JSON);

        Object actual = OBJECT_CODEC.fromJson(response.getBody());
        Object expected = OBJECT_CODEC.fromJson("{}");
        assertEquals(actual, expected);
    }

    @Test
    public void testClearSpoolCounts()
            throws Exception
    {
        StatusResponse response = client.execute(prepareDelete()
                .setUri(urlFor("/v1/spool/stats")).build(),
                createStatusResponseHandler());

        assertEquals(response.getStatusCode(), Status.NO_CONTENT.getStatusCode());
    }

    @Test
    public void testDistributeSingle()
            throws IOException, ExecutionException, InterruptedException
    {
        String json = Resources.toString(Resources.getResource("single.json"), Charsets.UTF_8);
        StatusResponse response = client.execute(preparePost()
                .setUri(urlFor("/v2/event/distribute"))
                .setHeader("Content-Type", APPLICATION_JSON)
                        .setBodySource(createStaticBodyGenerator(json, Charsets.UTF_8))
                .build(),
                createStatusResponseHandler());

        assertEquals(response.getStatusCode(), Status.ACCEPTED.getStatusCode());
    }

    private URI urlFor(String path)
    {
        return server.getBaseUrl().resolve(path);
    }

}
