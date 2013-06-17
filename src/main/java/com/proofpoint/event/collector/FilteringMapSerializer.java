package com.proofpoint.event.collector;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.JsonStreamContext;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.google.common.collect.ImmutableSet;
import com.google.inject.TypeLiteral;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static com.google.common.base.Objects.firstNonNull;
import static com.google.common.base.Preconditions.checkNotNull;

public class FilteringMapSerializer extends JsonSerializer<Map<String, ?>>
{
    private final List<PropertyMapFilter> filters;

    public FilteringMapSerializer(List<PropertyMapFilter> filters)
    {
        this.filters = checkNotNull(filters, "filters were null");
    }

    @Override
    public void serialize(Map<String, ?> map, JsonGenerator jgen, SerializerProvider provider)
            throws IOException, JsonProcessingException
    {
        //have to do this before starting to write the object, otherwise proper context is lost.
        MapFilter filter = findApplicableFilter(jgen.getOutputContext());
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

    private MapFilter findApplicableFilter(JsonStreamContext outputContext)
    {
        for (PropertyMapFilter filter : filters) {
            //second condition ensures we are at the proper level (i.e. one below root)
            if (filter.getNodeName().equals(outputContext.getCurrentName()) && isRootThisContextsParent(outputContext)) {
                return filter;
            }
        }
        return UniversalMatchFilter.get();
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

    public interface MapFilter {
        public boolean matches(String property);
    }

    public static class UniversalMatchFilter
        implements MapFilter
    {
        static MapFilter matchesEverything = new UniversalMatchFilter();

        public static MapFilter get()
        {
            return matchesEverything;
        }

        @Override
        public boolean matches(String property)
        {
            return true;
        }
    }

    public static class PropertyMapFilter
        implements MapFilter
    {
        private String nodeName;
        private Set<String> propertiesToSerialize;

        public PropertyMapFilter(String nodeName, Set<String> propertiesToSerialize)
        {
            this.nodeName = nodeName;
            this.propertiesToSerialize = ImmutableSet.copyOf(firstNonNull(propertiesToSerialize, ImmutableSet.<String>of()));
        }

        public String getNodeName()
        {
            return nodeName;
        }

        public Set<String> getPropertiesToSerialize()
        {
            return propertiesToSerialize;
        }

        @Override
        public boolean matches(String property)
        {
            return propertiesToSerialize.isEmpty() || propertiesToSerialize.contains(property);
        }
    }
}
