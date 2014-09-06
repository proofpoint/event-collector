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
package com.proofpoint.event.collector.validation;

import com.google.common.collect.ImmutableList;
import org.testng.annotations.Test;

import static com.proofpoint.testing.ValidationAssertions.assertFailsValidation;
import static com.proofpoint.testing.ValidationAssertions.assertValidates;

public class TestValidUriValidator
{
    @Test
    public void testAssertValidates()
    {
        assertValidates(new IterableStringBean(ImmutableList.of("http://www.example.com")));
        assertValidates(new IterableStringBean(ImmutableList.of("http://www.example.com", "https://www.example2.com")));
    }

    @Test
    public void testAssertEmptyIterableValidates()
    {
        assertValidates(new IterableStringBean(ImmutableList.<String>of()));
    }

    @Test
    public void testFailsSyntaxValidation()
    {
        assertFailsUriValidation(ImmutableList.of("http://1.2.3.4/path|dummy"), "Invalid URIs: Invalid syntax: http://1.2.3.4/path|dummy");
        assertFailsUriValidation(ImmutableList.of("http://1.2.3.4/path|dummy", "http://1.2.3.5/path|dummy"), "Invalid URIs: Invalid syntax: http://1.2.3.4/path|dummy, http://1.2.3.5/path|dummy");
        assertFailsUriValidation(ImmutableList.of("http://1.2.3.4/path", "http://1.2.3.4/path|dummy"), "Invalid URIs: Invalid syntax: http://1.2.3.4/path|dummy");
    }

    @Test
    public void testFailsSchemeValidation()
    {
        assertFailsUriValidation(ImmutableList.of("ftp://1.2.3.4/path"), "Invalid URIs: Invalid scheme: ftp://1.2.3.4/path");
        assertFailsUriValidation(ImmutableList.of("ftp://1.2.3.4/path", "ftp://1.2.3.5/path"), "Invalid URIs: Invalid scheme: ftp://1.2.3.4/path, ftp://1.2.3.5/path");
        assertFailsUriValidation(ImmutableList.of("ftp://1.2.3.4/path", "http://1.2.3.4/valid"), "Invalid URIs: Invalid scheme: ftp://1.2.3.4/path");
    }

    @Test
    public void testFailsSyntaxAndSchemeValidation()
    {
        assertFailsUriValidation(ImmutableList.of("ftp://1.2.3.4/path|dummy"), "Invalid URIs: Invalid syntax: ftp://1.2.3.4/path|dummy");
    }

    public void assertFailsUriValidation(Iterable<String> values, String expectedErrorMessage)
    {
        assertFailsValidation(new IterableStringBean(values), "values", expectedErrorMessage, ValidUri.class);
    }

    public static class IterableStringBean
    {
        private Iterable<String> values;

        private IterableStringBean(Iterable<String> values)
        {
            this.values = values;
        }

        @ValidUri(schemes = {"http", "https"})
        public Iterable<String> getValues()
        {
            return values;
        }
    }
}
