package com.proofpoint.event.collector;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Maps;
import com.proofpoint.json.JsonCodec;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import javax.validation.constraints.NotNull;
import java.util.Map;

import static com.proofpoint.json.JsonCodec.jsonCodec;
import static com.proofpoint.json.testing.JsonTester.assertJsonEncode;
import static com.proofpoint.json.testing.JsonTester.decodeJson;
import static com.proofpoint.testing.ValidationAssertions.assertFailsValidation;
import static com.proofpoint.testing.ValidationAssertions.assertValidates;


public class TestFlowConfiguration
{
    private final JsonCodec<FlowConfiguration> codec = jsonCodec(FlowConfiguration.class);
    private Map<String,?> map;

    @BeforeMethod
    public void setup() {
        map = Maps.newHashMap(ImmutableMap.of("propertiesToSerialize", ImmutableList.of("property1")));
    }

    @Test
    public void testNoPropertiesToSerializeInJsonDecode()
    {
        map.remove("propertiesToSerialize");
        assertFailsValidation(decodeJson(codec, map), "propertiesToSerialize", "is missing", NotNull.class);
    }

    @Test
    public void testJsonEncode()
    {
        assertJsonEncode(assertValidates(new FlowConfiguration(ImmutableSet.of("property1"))), map);
    }

    @Test
    public void testNullPropertiesToSerialize()
    {
        FlowConfiguration flowConfig = new FlowConfiguration(null);
        assertFailsValidation(flowConfig, "propertiesToSerialize", "is missing", NotNull.class);
    }

    @Test
    public void testPassesValidation()
    {
        FlowConfiguration flowConfig = new FlowConfiguration(ImmutableSet.of("foo"));
        assertValidates(flowConfig);
    }

    @Test
    public void testEmptyPropertiesPassesValidation()
    {
        FlowConfiguration flowConfig = new FlowConfiguration(ImmutableSet.<String>of());
        assertValidates(flowConfig);
    }
}
