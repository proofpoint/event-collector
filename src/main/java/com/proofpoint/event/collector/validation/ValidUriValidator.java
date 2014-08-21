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

import com.google.common.base.Function;
import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.ImmutableSortedSet;
import com.google.common.collect.Iterables;

import javax.validation.ConstraintValidator;
import javax.validation.ConstraintValidatorContext;
import java.net.URI;
import java.util.Arrays;
import java.util.Set;

public class ValidUriValidator
        implements ConstraintValidator<ValidUri, Iterable<String>>
{
    private static final Joiner ERROR_JOINER = Joiner.on(", ");
    private static final String ERROR_SEPARATOR = "; ";

    private static final Function<String, String> TO_LOWER_CASE = new Function<String, String>()
    {
        @Override
        public String apply(String input)
        {
            return input.toLowerCase();
        }
    };

    private Set<String> validSchemes;

    @Override
    public void initialize(ValidUri constraintAnnotation)
    {
        validSchemes = ImmutableSet.copyOf(Iterables.transform(Arrays.asList(constraintAnnotation.schemes()), TO_LOWER_CASE));
    }

    @Override
    public boolean isValid(Iterable<String> value, ConstraintValidatorContext context)
    {
        ImmutableSortedSet.Builder<String> uriWithInvalidSyntaxBuilder = ImmutableSortedSet.naturalOrder();
        ImmutableSortedSet.Builder<String> uriWithInvalidSchemeBuilder = ImmutableSortedSet.naturalOrder();

        for (String uriString : value) {
            URI uri;
            try {
                uri = URI.create(uriString);
            }
            catch (Exception ignored) {
                uriWithInvalidSyntaxBuilder.add(uriString);
                continue;
            }

            if (validSchemes.isEmpty()) {
                continue;
            }

            String uriScheme = uri.getScheme();

            if (uriScheme == null || !validSchemes.contains(uriScheme.toLowerCase())) {
                uriWithInvalidSchemeBuilder.add(uriString);
                continue;
            }
        }

        StringBuilder messageBuilder = new StringBuilder();
        Set<String> uriWithInvalidSyntax = uriWithInvalidSyntaxBuilder.build();
        Set<String> uriWithInvalidScheme = uriWithInvalidSchemeBuilder.build();

        if (uriWithInvalidSyntax.isEmpty() && uriWithInvalidScheme.isEmpty()) {
            return true;
        }

        messageBuilder.append("Invalid URIs: ");
        String prefix = "";
        if (!uriWithInvalidSyntax.isEmpty()) {
            messageBuilder
                    .append(prefix)
                    .append("Invalid syntax: ")
                    .append(ERROR_JOINER.join(uriWithInvalidSyntax));
            prefix = ERROR_SEPARATOR;
        }
        if (!uriWithInvalidScheme.isEmpty()) {
            messageBuilder
                    .append(prefix)
                    .append("Invalid scheme: ")
                    .append(ERROR_JOINER.join(uriWithInvalidScheme));
            prefix = ERROR_SEPARATOR;
        }

        context.disableDefaultConstraintViolation();
        context.buildConstraintViolationWithTemplate(messageBuilder.toString())
                .addConstraintViolation();

        return false;
    }
}
