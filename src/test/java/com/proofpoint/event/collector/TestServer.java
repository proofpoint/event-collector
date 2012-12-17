/*
 * Copyright 2011-2012 Proofpoint, Inc.
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
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.ning.http.client.AsyncHttpClient;
import com.ning.http.client.Response;
import com.proofpoint.configuration.ConfigurationFactory;
import com.proofpoint.configuration.ConfigurationModule;
import com.proofpoint.discovery.client.DiscoveryModule;
import com.proofpoint.event.client.JsonEventModule;
import com.proofpoint.http.client.HttpClientModule;
import com.proofpoint.http.server.testing.TestingHttpServer;
import com.proofpoint.http.server.testing.TestingHttpServerModule;
import com.proofpoint.jaxrs.JaxrsModule;
import com.proofpoint.json.JsonCodec;
import com.proofpoint.json.JsonModule;
import com.proofpoint.node.testing.TestingNodeModule;
import com.proofpoint.testing.FileUtils;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.io.File;
import java.io.IOException;
import java.util.concurrent.ExecutionException;

import static javax.ws.rs.core.HttpHeaders.CONTENT_TYPE;
import static javax.ws.rs.core.MediaType.APPLICATION_JSON;
import static javax.ws.rs.core.Response.Status;
import static org.testng.Assert.assertEquals;

public class TestServer
{
    private JsonCodec<Object> OBJECT_CODEC = JsonCodec.jsonCodec(Object.class);
    private AsyncHttpClient client;
    private TestingHttpServer server;
    private File tempStageDir;
    private EventTapWriter eventTapWriter;

    @BeforeMethod
    public void setup()
            throws Exception
    {
        tempStageDir = Files.createTempDir();

        // TODO: wrap all this stuff in a TestBootstrap class
        ImmutableMap<String, String> config = ImmutableMap.<String, String>builder()
                .put("collector.accepted-event-types", "Test")
                .put("collector.local-staging-directory", tempStageDir.getAbsolutePath())
                .put("collector.aws-access-key", "fake-aws-access-key")
                .put("collector.aws-secret-key", "fake-aws-secret-key")
                .put("collector.s3-staging-location", "s3://test-staging/")
                .put("collector.s3-data-location", "s3://test-data/")
                .put("collector.s3-metadata-location", "s3://test-metadata/")
                .build();
        Injector injector = Guice.createInjector(
                new TestingNodeModule(),
                new TestingHttpServerModule(),
                new DiscoveryModule(),
                new JsonModule(),
                new JaxrsModule(),
                new JsonEventModule(),
                new EventTapModule(),
                new MainModule(),
                new HttpClientModule(EventTap.class),
                new ConfigurationModule(new ConfigurationFactory(config)));

        server = injector.getInstance(TestingHttpServer.class);
        eventTapWriter = injector.getInstance(EventTapWriter.class);

        server.start();
        eventTapWriter.start();
        client = new AsyncHttpClient();
    }

    @AfterMethod
    public void teardown()
            throws Exception
    {
        if (eventTapWriter != null) {
            eventTapWriter.stop();
        }

        if (server != null) {
            server.stop();
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
        Response response = client.preparePost(urlFor("/v2/event"))
                .setHeader("Content-Type", APPLICATION_JSON)
                .setBody(json)
                .execute()
                .get();

        assertEquals(response.getStatusCode(), Status.ACCEPTED.getStatusCode());
    }

    @Test
    public void testPostMultiple()
            throws IOException, ExecutionException, InterruptedException
    {
        String json = Resources.toString(Resources.getResource("multiple.json"), Charsets.UTF_8);
        Response response = client.preparePost(urlFor("/v2/event"))
                .setHeader("Content-Type", APPLICATION_JSON)
                .setBody(json)
                .execute()
                .get();

        assertEquals(response.getStatusCode(), Status.ACCEPTED.getStatusCode());
    }

    @Test
    public void testGetTapCounts()
            throws Exception
    {
        Response response = client.prepareGet(urlFor("/v1/tap/stats")).execute().get();

        assertEquals(response.getStatusCode(), Status.OK.getStatusCode());
        assertEquals(response.getHeader(CONTENT_TYPE), APPLICATION_JSON);

        Object actual = OBJECT_CODEC.fromJson(response.getResponseBody());
        Object expected = OBJECT_CODEC.fromJson("{\"queue\":{},\"flows\":{}}");
        assertEquals(actual, expected);
    }

    @Test
    public void testClearTapCounts()
            throws Exception
    {
        Response response = client.prepareDelete(urlFor("/v1/tap/stats")).execute().get();

        assertEquals(response.getStatusCode(), Status.NO_CONTENT.getStatusCode());
    }


    @Test
    public void testGetSpoolCounts()
            throws Exception
    {
        Response response = client.prepareGet(urlFor("/v1/spool/stats")).execute().get();

        assertEquals(response.getStatusCode(), Status.OK.getStatusCode());
        assertEquals(response.getHeader(CONTENT_TYPE), APPLICATION_JSON);

        Object actual = OBJECT_CODEC.fromJson(response.getResponseBody());
        Object expected = OBJECT_CODEC.fromJson("{}");
        assertEquals(actual, expected);
    }

    @Test
    public void testClearSpoolCounts()
            throws Exception
    {
        Response response = client.prepareDelete(urlFor("/v1/spool/stats")).execute().get();

        assertEquals(response.getStatusCode(), Status.NO_CONTENT.getStatusCode());
    }

    private String urlFor(String path)
    {
        return server.getBaseUrl().resolve(path).toString();
    }
}
