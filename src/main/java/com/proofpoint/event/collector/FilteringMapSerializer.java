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

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.JsonStreamContext;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.google.common.collect.ImmutableSet;

import java.io.IOException;
import java.util.Collection;
import java.util.Map;
import java.util.Set;

import static com.google.common.base.Objects.firstNonNull;
import static com.google.common.base.Preconditions.checkNotNull;

public class FilteringMapSerializer extends JsonSerializer<Map<String, ?>>
{
    private final Collection<DefinedPropertiesSelectionPolicy> filters;

    public FilteringMapSerializer(Collection<DefinedPropertiesSelectionPolicy> filters)
    {
        this.filters = checkNotNull(filters, "filters were null");
    }

    @Override
    public void serialize(Map<String, ?> map, JsonGenerator jgen, SerializerProvider provider)
            throws IOException, JsonProcessingException
    {
        //have to do this before starting to write the object, otherwise proper context is lost.
        PropertyMapSelectionPolicy filter = findApplicableFilter(jgen.getOutputContext());
        jgen.writeStartObject();
        for (Map.Entry<String, ?> entry : map.entrySet()) {
            String name = entry.getKey();

            if (!filter.matches(name)) {
                continue;
            }

            Object value = entry.getValue();
            jgen.writeObjectField(name, value);
        }
        jgen.writeEndObject();
    }

    private PropertyMapSelectionPolicy findApplicableFilter(JsonStreamContext outputContext)
    {
        for (DefinedPropertiesSelectionPolicy filter : filters) {
            //second condition ensures we are at the proper level (i.e. one below root)
            if (filter.getNodeName().equals(outputContext.getCurrentName()) && isRootThisContextsParent(outputContext)) {
                return filter;
            }
        }
        return UniversalMatchSelectionPolicy.get();
    }

    private boolean isRootThisContextsParent(JsonStreamContext outputContext)
    {
        return outputContext.getParent().getCurrentName() == null;
    }

    @Override
    public Class<Map<String, ?>> handledType()
    {
        return (Class<Map<String, ?>>) (Class<?>) Map.class;
    }

    public interface PropertyMapSelectionPolicy
    {
        public boolean matches(String property);
    }

    private static class UniversalMatchSelectionPolicy
        implements PropertyMapSelectionPolicy
    {
        private static PropertyMapSelectionPolicy matchesEverything = new UniversalMatchSelectionPolicy();

        public static PropertyMapSelectionPolicy get()
        {
            return matchesEverything;
        }

        @Override
        public boolean matches(String property)
        {
            return true;
        }
    }

    public static class DefinedPropertiesSelectionPolicy
        implements PropertyMapSelectionPolicy
    {
        private String nodeName;
        private Set<String> propertiesToSerialize;

        public DefinedPropertiesSelectionPolicy(String nodeName, Set<String> propertiesToSerialize)
        {
            this.nodeName = nodeName;
            this.propertiesToSerialize = ImmutableSet.copyOf(firstNonNull(propertiesToSerialize, ImmutableSet.<String>of()));
        }

        public String getNodeName()
        {
            return nodeName;
        }

        @Override
        public boolean matches(String property)
        {
            return propertiesToSerialize.isEmpty() || propertiesToSerialize.contains(property);
        }
    }
}
