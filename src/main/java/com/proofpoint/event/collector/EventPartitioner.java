/*
 * Copyright 2011-2013 Proofpoint, Inc.
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
