/*
 * Copyright 2011-2014 Proofpoint, Inc.
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

import com.google.common.collect.ImmutableMap;
import org.testng.annotations.Test;

import javax.validation.constraints.NotNull;
import javax.validation.constraints.Size;
import java.util.Map;

import static com.proofpoint.configuration.testing.ConfigAssertions.assertFullMapping;
import static com.proofpoint.testing.ValidationAssertions.assertFailsValidation;
import static com.proofpoint.testing.ValidationAssertions.assertValidates;

public class TestPerFlowStaticEventTapConfig
{
    private static final String ARBITRARY_FLOW_ID = "foo";
    private static final String ARBITRARY_EVENT_TYPES = "typeA, typeB";
    private static final String ARBITRARY_URIS = "1.2.3.40, 1.2.3.41";

    @Test
    public void testExplicitPropertyMappings()
    {
        Map<String, String> properties = ImmutableMap.of(
                "event-types", ARBITRARY_EVENT_TYPES,
                "flow-id", ARBITRARY_FLOW_ID,
                "uris", ARBITRARY_URIS
        );

        PerFlowStaticEventTapConfig expected = new PerFlowStaticEventTapConfig()
                .setEventTypes(ARBITRARY_EVENT_TYPES)
                .setFlowId(ARBITRARY_FLOW_ID)
                .setUris(ARBITRARY_URIS);

        assertFullMapping(properties, expected);
    }

    @Test
    public void testValidation()
    {
        PerFlowStaticEventTapConfig configWithNullProperties = new PerFlowStaticEventTapConfig()
                .setEventTypes(null)
                .setFlowId(null)
                .setUris(null);

        assertFailsValidation(configWithNullProperties, "flowId", "may not be null", NotNull.class);
        assertFailsValidation(configWithNullProperties, "eventTypes", "may not be empty", Size.class);
        assertFailsValidation(configWithNullProperties, "uris", "may not be empty", Size.class);

        PerFlowStaticEventTapConfig configWithEmptyProperties = new PerFlowStaticEventTapConfig()
                .setEventTypes("")
                .setFlowId("")
                .setUris("");

        assertFailsValidation(configWithEmptyProperties, "eventTypes", "may not be empty", Size.class);
        assertFailsValidation(configWithEmptyProperties, "flowId", "may not be empty", Size.class);
        assertFailsValidation(configWithEmptyProperties, "uris", "may not be empty", Size.class);

        assertValidates(new PerFlowStaticEventTapConfig()
                .setEventTypes(ARBITRARY_EVENT_TYPES)
                .setFlowId(ARBITRARY_FLOW_ID)
                .setUris(ARBITRARY_URIS));
    }
}
