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

import java.util.Map;

import static com.proofpoint.configuration.testing.ConfigAssertions.assertFullMapping;
import static com.proofpoint.configuration.testing.ConfigAssertions.assertRecordedDefaults;
import static com.proofpoint.configuration.testing.ConfigAssertions.recordDefaults;
import static com.proofpoint.testing.ValidationAssertions.assertValidates;

public class TestStaticEventTapConfig
{
    private static final String ARBITRARY_FLOW_ID_A = "foo";
    private static final String ARBITRARY_FLOW_ID_B = "bar";
    private static final String ARBITRARY_EVENT_TYPES = "typeA, typeB";
    private static final String ARBITRARY_URIS_A = "1.2.3.40, 1.2.3.41";
    private static final String ARBITRARY_URIS_B = "1.2.3.50, 1.2.3.51";

    @Test
    public void testDefaults()
    {
        assertRecordedDefaults(recordDefaults(StaticEventTapConfig.class)
                .setFlowConfigMap(ImmutableMap.<String, PerFlowStaticEventTapConfig>of())
        );
    }

    @Test
    public void testExplicitPropertyMappings()
    {
        ImmutableMap.Builder<String, String> properties = ImmutableMap.builder();
        properties.put("taps.foo.event-types", ARBITRARY_EVENT_TYPES);
        properties.put("taps.foo.flow-id", ARBITRARY_FLOW_ID_A);
        properties.put("taps.foo.uris", ARBITRARY_URIS_A);

        properties.put("taps.bar.event-types", ARBITRARY_EVENT_TYPES);
        properties.put("taps.bar.flow-id", ARBITRARY_FLOW_ID_B);
        properties.put("taps.bar.uris", ARBITRARY_URIS_B);

        Map<String, PerFlowStaticEventTapConfig> inputMap = ImmutableMap.of(
            "foo", new PerFlowStaticEventTapConfig()
                .setEventTypes(ARBITRARY_EVENT_TYPES)
                .setFlowId(ARBITRARY_FLOW_ID_A)
                .setUris(ARBITRARY_URIS_A),

            "bar", new PerFlowStaticEventTapConfig()
                .setEventTypes(ARBITRARY_EVENT_TYPES)
                .setFlowId(ARBITRARY_FLOW_ID_B)
                .setUris(ARBITRARY_URIS_B)
        );

        assertFullMapping(properties.build(), getConfig().setFlowConfigMap(inputMap));
    }

    @Test
    public void testValidation()
    {
        assertValidates(getConfig().setFlowConfigMap(ImmutableMap.<String, PerFlowStaticEventTapConfig>of()));
    }

    private StaticEventTapConfig getConfig()
    {
        return new StaticEventTapConfig();
    }
}
