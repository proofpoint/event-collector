package com.proofpoint.event.collector;

import com.google.common.annotations.VisibleForTesting;
import com.proofpoint.http.client.HttpClient;
import com.proofpoint.http.client.Request;
import com.proofpoint.json.JsonCodec;

import javax.inject.Inject;
import javax.ws.rs.core.HttpHeaders;
import javax.ws.rs.core.MediaType;
import java.net.URI;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.proofpoint.http.client.FullJsonResponseHandler.createFullJsonResponseHandler;
import static java.lang.String.format;


public class HttpFlowConfigurationLoader
    implements FlowConfigurationLoader
{
    @VisibleForTesting
    static final String LOADING_EXCEPTION_MESSAGE = "Config could not be loaded from location %s";
    private static final JsonCodec<FlowConfiguration> flowConfigurationCodec = JsonCodec.jsonCodec(FlowConfiguration.class);
    private final HttpClient httpClient;
    private final FlowConfiguration defaultConfig;

    @Inject
    public HttpFlowConfigurationLoader(HttpClient httpClient, FlowConfiguration defaultConfig)
    {
        this.httpClient = checkNotNull(httpClient, "httpClient is null");
        this.defaultConfig = defaultConfig;
    }

    @Override
    public FlowConfiguration loadConfiguration(URI configurationLocation)
            throws ConfigLoadingException
    {
        if (configurationLocation == null) {
            return defaultConfig;
        }

        try {
            Request.Builder requestBuilder = Request.builder().prepareGet()
                .setUri(configurationLocation)
                .setHeader(HttpHeaders.CONTENT_TYPE, MediaType.APPLICATION_JSON);

            return httpClient.execute(requestBuilder.build(), createFullJsonResponseHandler(flowConfigurationCodec)).getValue();
        }
        catch (Exception ex) {
            throw new ConfigLoadingException(format(LOADING_EXCEPTION_MESSAGE, configurationLocation), ex);
        }
    }
}
