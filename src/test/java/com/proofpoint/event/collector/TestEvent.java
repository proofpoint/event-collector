/*
 * Copyright 2011-2012 Proofpoint, Inc.
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

import org.joda.time.DateTime;
import org.testng.annotations.Test;

import javax.validation.constraints.NotNull;
import javax.validation.constraints.Pattern;
import java.util.Collections;
import java.util.Map;
import java.util.UUID;

import static com.proofpoint.experimental.testing.ValidationAssertions.assertFailsValidation;
import static com.proofpoint.experimental.testing.ValidationAssertions.assertValidates;

public class TestEvent
{
    @Test
    public void testEventValidation()
    {
        String type = "test";
        String uuid = UUID.randomUUID().toString();
        String host = "test.local";
        DateTime time = new DateTime();
        Map<String, ?> data = Collections.emptyMap();

        assertValidates(new Event(type, uuid, host, time, data));

        assertFailsValidation(new Event(null, uuid, host, time, data), "type", "is missing", NotNull.class);
        assertFailsValidation(new Event(type, null, host, time, data), "uuid", "is missing", NotNull.class);
        assertFailsValidation(new Event(type, uuid, null, time, data), "host", "is missing", NotNull.class);
        assertFailsValidation(new Event(type, uuid, host, null, data), "timestamp", "is missing", NotNull.class);
        assertFailsValidation(new Event(type, uuid, host, time, null), "data", "is missing", NotNull.class);

        assertFailsValidation(new Event("hello!", uuid, host, time, data), "type", "must be alphanumeric", Pattern.class);
        assertFailsValidation(new Event("!hello", uuid, host, time, data), "type", "must be alphanumeric", Pattern.class);
        assertFailsValidation(new Event("0abc", uuid, host, time, data), "type", "must be alphanumeric", Pattern.class);
    }
}
