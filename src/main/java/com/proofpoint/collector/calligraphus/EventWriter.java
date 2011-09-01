package com.proofpoint.collector.calligraphus;

import java.io.IOException;

public interface EventWriter
{
    void write(Event event)
            throws IOException;
}
