package com.proofpoint.event.collector;

import com.proofpoint.http.client.HttpClient;
import com.proofpoint.http.client.Request;
import com.proofpoint.json.JsonCodec;

import javax.inject.Inject;
import javax.ws.rs.core.HttpHeaders;
import javax.ws.rs.core.MediaType;
import java.net.URI;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.proofpoint.http.client.JsonResponseHandler.createJsonResponseHandler;
import static java.lang.String.format;


public class HttpFlowConfigurationLoader
    implements FlowConfigurationLoader
{
    private static final String LOADING_EXCEPTION_MESSAGE = "Config could not be loaded from location %s";
    private static final JsonCodec<FlowConfiguration> FLOW_CONFIGURATION_CODEC = JsonCodec.jsonCodec(FlowConfiguration.class);
    private final HttpClient httpClient;

    @Inject
    public HttpFlowConfigurationLoader(@ForFlowConfigurationLoader HttpClient httpClient)
    {
        this.httpClient = checkNotNull(httpClient, "httpClient is null");
    }

    @Override
    public FlowConfiguration loadConfiguration(URI configurationLocation)
            throws ConfigLoadingException
    {
        checkNotNull(configurationLocation, "configurationLocation is null");
        try {
            Request.Builder requestBuilder = Request.builder().prepareGet().setUri(configurationLocation).setHeader(HttpHeaders.ACCEPT, MediaType.APPLICATION_JSON);
            return httpClient.execute(requestBuilder.build(), createJsonResponseHandler(FLOW_CONFIGURATION_CODEC));
        }
        catch (Exception ex) {
            throw new ConfigLoadingException(format(LOADING_EXCEPTION_MESSAGE, configurationLocation), ex);
        }
    }
}
