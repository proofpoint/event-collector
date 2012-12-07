package com.proofpoint.event.collector;

import com.google.common.collect.ImmutableSet;
import com.proofpoint.discovery.client.ServiceDescriptor;
import com.proofpoint.discovery.client.ServiceSelector;
import com.proofpoint.discovery.client.testing.StaticServiceSelector;
import com.proofpoint.http.client.HttpClient;
import com.proofpoint.json.JsonCodec;
import org.logicalshift.concurrent.SerialScheduledExecutorService;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.List;
import java.util.concurrent.ScheduledExecutorService;

import static org.mockito.Mockito.mock;

public class TestEventTapWriter
{
    private static final JsonCodec<List<Event>> OBJECT_CODEC = JsonCodec.listJsonCodec(Event.class);
    private ServiceSelector serviceSelector;
    private HttpClient httpClient;
    private ScheduledExecutorService executorService;

    @BeforeMethod
    public void setup()
    {
        serviceSelector = new StaticServiceSelector(ImmutableSet.<ServiceDescriptor>of());
        httpClient = mock(HttpClient.class);
        executorService = new SerialScheduledExecutorService();
    }

    @Test(expectedExceptions = NullPointerException.class, expectedExceptionsMessageRegExp = "selector is null")
    public void testConstructorNullSelector()
    {
        new EventTapWriter(null, httpClient, OBJECT_CODEC, executorService, new EventTapConfig());
    }

    @Test(expectedExceptions = NullPointerException.class, expectedExceptionsMessageRegExp = "httpClient is null")
    public void testConstructorNullHttpClient()
    {
        new EventTapWriter(serviceSelector, null, OBJECT_CODEC, executorService, new EventTapConfig());
    }

    @Test(expectedExceptions = NullPointerException.class, expectedExceptionsMessageRegExp = "eventCodec is null")
    public void testConstructorNullEventCodec()
    {
        new EventTapWriter(serviceSelector, httpClient, null, executorService, new EventTapConfig());
    }

    @Test(expectedExceptions = NullPointerException.class, expectedExceptionsMessageRegExp = "executorService is null")
    public void testConstructorNullExecutorService()
    {
        new EventTapWriter(serviceSelector, httpClient, OBJECT_CODEC, null, new EventTapConfig());
    }

    @Test(expectedExceptions = NullPointerException.class, expectedExceptionsMessageRegExp = "config is null")
    public void testConstructorNullConfig()
    {
        new EventTapWriter(serviceSelector, httpClient, OBJECT_CODEC, executorService, null);
    }
}
