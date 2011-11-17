package com.proofpoint.event.collector;

import java.io.IOException;

public interface EventWriter
{
    void write(Event event)
            throws IOException;
}
