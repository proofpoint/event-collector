package com.proofpoint.event.collector;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.JsonStreamContext;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.google.inject.TypeLiteral;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static com.google.common.base.Preconditions.checkNotNull;

public class FilteringMapSerializer extends JsonSerializer<Map<String, ?>>
{
    private final List<MapFilter> filters;

    public FilteringMapSerializer(List<MapFilter> filters)
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

            if (filter != null && !filter.getPropertiesToSerialize().contains(entry.getKey())) {
                continue;
            }

            Object value = entry.getValue();
            jgen.writeObjectField(name, value);
        }
        jgen.writeEndObject();
    }

    private MapFilter findApplicableFilter(JsonStreamContext outputContext)
    {
        for (MapFilter filter : filters) {
            //second condition ensures we are at the proper level (i.e. one below root)
            if (filter.getNodeName().equals(outputContext.getCurrentName()) && isRootThisContextsParent(outputContext)) {
                return filter;
            }
        }
        return null;
    }

    private boolean isRootThisContextsParent(JsonStreamContext outputContext)
    {
        return outputContext.getParent().getCurrentName() == null;
    }

    @Override
    public Class<Map<String, ?>> handledType()
    {
        return (Class<Map<String, ?>>) new TypeLiteral<Map<String, ?>>() {}.getRawType();    //To change body of overridden methods use File | Settings | File Templates.
    }

    public static class MapFilter
    {
        private String nodeName;
        private Set<String> propertiesToSerialize;

        public MapFilter(String nodeName, Set<String> propertiesToSerialize)
        {
            this.nodeName = nodeName;
            this.propertiesToSerialize = propertiesToSerialize;
        }

        public String getNodeName()
        {
            return nodeName;
        }

        public Set<String> getPropertiesToSerialize()
        {
            return propertiesToSerialize;
        }
    }
}
