package com.proofpoint.event.collector;

import org.testng.annotations.Test;

import javax.validation.constraints.NotNull;

import static com.proofpoint.testing.ValidationAssertions.assertFailsValidation;

public class TestFlowConfiguration
{
    @Test()
    public void testNullPropertiesToSerialize()
    {
        FlowConfiguration flowConfig = new FlowConfiguration(null);
        assertFailsValidation(flowConfig, "propertiesToSerialize", "is missing", NotNull.class);
    }
}
