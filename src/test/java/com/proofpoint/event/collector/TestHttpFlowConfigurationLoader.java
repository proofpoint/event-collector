package com.proofpoint.event.collector;

import com.google.common.collect.ImmutableSet;
import com.proofpoint.http.client.FullJsonResponseHandler.JsonResponse;
import com.proofpoint.http.client.HttpClient;
import com.proofpoint.http.client.Request;
import com.proofpoint.http.client.ResponseHandler;
import org.testng.annotations.Test;

import java.net.URI;

import static java.lang.String.format;
import static org.mockito.Matchers.anyObject;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;

public class TestHttpFlowConfigurationLoader
{
    FlowConfiguration defaultConfig = new FlowConfiguration(ImmutableSet.of("default"));
    FlowConfiguration remoteConfig = new FlowConfiguration(ImmutableSet.of(""));

    @Test
    public void testBasicRemoteConfigurationLoad()
            throws Exception
    {
        HttpClient client = mock(HttpClient.class);
        JsonResponse jsonResponse = mock(JsonResponse.class);
        when(client.execute((Request) anyObject(), (ResponseHandler) anyObject())).thenReturn(jsonResponse);
        when(jsonResponse.getValue()).thenReturn(remoteConfig);

        FlowConfigurationLoader loader = new HttpFlowConfigurationLoader(client, defaultConfig);
        assertEquals(loader.loadConfiguration(URI.create("http://www.remoteconfiglocation.com/")), remoteConfig);
    }

    @Test
    public void testNoRemoteConfigurationLocationLoad()
            throws Exception
    {
        FlowConfigurationLoader loader = new HttpFlowConfigurationLoader(mock(HttpClient.class), defaultConfig);
        assertEquals(loader.loadConfiguration(null), defaultConfig);
    }

    @Test(expectedExceptions = ConfigLoadingException.class)
    public void testClientExceptionThrowsConfigLoadingException()
            throws Exception
    {
        HttpClient client = mock(HttpClient.class);
        when(client.execute((Request) anyObject(), (ResponseHandler) anyObject())).thenThrow(new Exception("Bad!"));

        FlowConfigurationLoader loader = new HttpFlowConfigurationLoader(client, defaultConfig);
        loader.loadConfiguration(URI.create("http://www.remoteconfiglocation.com/"));
    }

    @Test()
    public void testConfigLoadingExceptionMessage()
            throws Exception
    {
        String configLocation = "http://www.remoteconfiglocation.com/foo.json";
        HttpClient client = mock(HttpClient.class);
        when(client.execute((Request) anyObject(), (ResponseHandler) anyObject())).thenThrow(new Exception("Bad!"));

        FlowConfigurationLoader loader = new HttpFlowConfigurationLoader(client, defaultConfig);
        try {
            loader.loadConfiguration(URI.create(configLocation));
        }
        catch (ConfigLoadingException ex) {
            assertEquals(ex.getMessage(), format(HttpFlowConfigurationLoader.LOADING_EXCEPTION_MESSAGE, configLocation));
        }
    }
}
