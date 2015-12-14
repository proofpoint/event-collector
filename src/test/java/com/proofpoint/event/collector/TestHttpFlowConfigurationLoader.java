package com.proofpoint.event.collector;

import com.google.common.collect.ImmutableSet;
import com.proofpoint.http.client.HttpClient;
import com.proofpoint.http.client.Request;
import com.proofpoint.http.client.ResponseHandler;
import org.testng.annotations.Test;

import javax.ws.rs.core.HttpHeaders;
import javax.ws.rs.core.MediaType;
import java.net.URI;

import static java.lang.String.format;
import static org.mockito.Matchers.anyObject;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertSame;
import static org.testng.Assert.fail;

public class TestHttpFlowConfigurationLoader
{
    FlowConfiguration remoteConfig = new FlowConfiguration(ImmutableSet.of(""));

    @Test
    public void testBasicRemoteConfigurationLoad()
            throws Exception
    {
        HttpClient client = mock(HttpClient.class);
        URI configLocation = URI.create("http://www.remoteconfiglocation.com/");
        Request.Builder requestBuilder = Request.builder().prepareGet().setUri(configLocation).setHeader(HttpHeaders.ACCEPT, MediaType.APPLICATION_JSON);
        when(client.execute(eq(requestBuilder.build()), (ResponseHandler) anyObject())).thenReturn(remoteConfig);

        FlowConfigurationLoader loader = new HttpFlowConfigurationLoader(client);
        assertEquals(loader.loadConfiguration(configLocation), remoteConfig);
    }

    @Test(expectedExceptions = NullPointerException.class)
    public void testNoRemoteConfigurationLocationLoad()
            throws Exception
    {
        FlowConfigurationLoader loader = new HttpFlowConfigurationLoader(mock(HttpClient.class));
        loader.loadConfiguration(null);
        fail("Should have thrown", new NullPointerException("Config location was null"));
    }

    @Test()
    public void testConfigLoadingExceptionMessage()
            throws Exception
    {
        String configLocation = "http://www.remoteconfiglocation.com/foo.json";
        HttpClient client = mock(HttpClient.class);
        Exception thrownException = new Exception("Bad!");
        when(client.execute((Request) anyObject(), (ResponseHandler) anyObject())).thenThrow(thrownException);

        FlowConfigurationLoader loader = new HttpFlowConfigurationLoader(client);
        try {
            loader.loadConfiguration(URI.create(configLocation));
            fail("expected ConfigLoadingException");
        }
        catch (ConfigLoadingException ex) {
            assertEquals(ex.getMessage(), format("Config could not be loaded from location http://www.remoteconfiglocation.com/foo.json", configLocation));
            assertSame(ex.getCause(), thrownException);
        }
    }
}
