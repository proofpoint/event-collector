package com.proofpoint.event.collector;

import java.net.URI;

public interface FlowConfigurationLoader
{
    public FlowConfiguration loadConfiguration(URI configurationLocation)
            throws ConfigLoadingException;
}
