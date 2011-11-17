package com.proofpoint.event.collector;

import org.joda.time.DateTimeZone;
import org.joda.time.format.DateTimeFormatter;
import org.joda.time.format.ISODateTimeFormat;

public class EventPartitioner
{
    private static final DateTimeFormatter DATE_FORMAT = ISODateTimeFormat.date().withZone(DateTimeZone.UTC);
    private static final DateTimeFormatter HOUR_FORMAT = ISODateTimeFormat.hour().withZone(DateTimeZone.UTC);

    public EventPartition getPartition(Event event)
    {
        return new EventPartition(event.getType(),
                DATE_FORMAT.print(event.getTimestamp()),
                HOUR_FORMAT.print(event.getTimestamp()));
    }
}
