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
import com.proofpoint.event.collector.PerFlowStaticEventTapConfig.QosDelivery;
import com.proofpoint.event.collector.StaticEventTapConfig.FlowKey;
import org.testng.annotations.Test;

import javax.validation.constraints.Size;
import java.util.Map;

import static com.proofpoint.configuration.testing.ConfigAssertions.assertFullMapping;
import static com.proofpoint.configuration.testing.ConfigAssertions.assertLegacyEquivalence;
import static com.proofpoint.configuration.testing.ConfigAssertions.assertRecordedDefaults;
import static com.proofpoint.configuration.testing.ConfigAssertions.recordDefaults;
import static com.proofpoint.testing.ValidationAssertions.assertFailsValidation;

public class TestStaticEventTapConfig
{
    private static final String URIS = "http://1.2.3.40  , http://1.2.3.41";
    private static final String URIS_A = URIS;
    private static final String URIS_B = "http://1.2.3.50 , http://1.2.3.51";

    @Test
    public void testDefaults()
    {
        assertRecordedDefaults(
                recordDefaults(StaticEventTapConfig.class)
                        .setStaticTaps(ImmutableMap.<FlowKey, PerFlowStaticEventTapConfig>of())
        );
    }

    @Test
    public void testExplicitPropertyMappings()
    {
        ImmutableMap.Builder<String, String> properties = ImmutableMap.builder();
        properties.put("static-taps.typeA@foo.uris", URIS_A);
        properties.put("static-taps.typeA@foo.qos-delivery", "retry");

        properties.put("static-taps.typeA@bar.uris", URIS_B);
        properties.put("static-taps.typeA@bar.qos-delivery", "retry");

        Map<FlowKey, PerFlowStaticEventTapConfig> inputMap = ImmutableMap.of(
                FlowKey.valueOf("typeA@foo"),
                new PerFlowStaticEventTapConfig()
                        .setQosDelivery(QosDelivery.RETRY)
                        .setUris(URIS_A),

                FlowKey.valueOf("typeA@bar"),
                new PerFlowStaticEventTapConfig()
                        .setQosDelivery(QosDelivery.RETRY)
                        .setUris(URIS_B)
        );

        assertFullMapping(properties.build(), new StaticEventTapConfig().setStaticTaps(inputMap));
    }

    @Test
    public void testLegacyProperties()
    {
        assertLegacyEquivalence(StaticEventTapConfig.class, ImmutableMap.<String, String>of());
    }

    @Test(expectedExceptions = IllegalArgumentException.class, expectedExceptionsMessageRegExp = "Invalid flow key: @foo; Elements separate by @ cannot be empty")
    public void testFlowKeyConstructorMissingEventTypeThrowsException()
    {
        FlowKey.valueOf("@foo");
    }

    @Test(expectedExceptions = IllegalArgumentException.class, expectedExceptionsMessageRegExp = "Invalid flow key: typeA@; Elements separate by @ cannot be empty")
    public void testFlowKeyConstructorMissingFlowIdThrowsException()
    {
        FlowKey.valueOf("typeA@");
    }

    @Test(expectedExceptions = IllegalArgumentException.class, expectedExceptionsMessageRegExp = "Invalid flow key: @; Elements separate by @ cannot be empty")
    public void testFlowKeyConstructorMissingEventTypeAndFlowIdThrowsException()
    {
        FlowKey.valueOf("@");
    }

    @Test(expectedExceptions = IllegalArgumentException.class, expectedExceptionsMessageRegExp = "Invalid flow key: typeA_foo; Flow key must have a single @ character")
    public void testFlowKeyConstructorInvalidKeySyntaxThrowsException()
    {
        FlowKey.valueOf("typeA_foo");
    }

    @Test(expectedExceptions = IllegalArgumentException.class, expectedExceptionsMessageRegExp = "Invalid flow key: typeA@@@@@foo; Flow key must have a single @ character")
    public void testFlowKeyConstructorInvalidKeySyntax2ThrowsException()
    {
        FlowKey.valueOf("typeA@@@@@foo");
    }

    @Test
    public void testInvalidPerFlowStaticEventTapConfigFailsValidation()
    {
        Map<FlowKey, PerFlowStaticEventTapConfig> inputMap = ImmutableMap.of(
                FlowKey.valueOf("typeA@foo"),
                new PerFlowStaticEventTapConfig()
                        .setUris("")
        );

        StaticEventTapConfig config = new StaticEventTapConfig().setStaticTaps(inputMap);
        assertFailsValidation(config, "staticTaps[FlowKey{eventType=typeA, flowId=foo}].uris", "may not be empty", Size.class);
    }
}
