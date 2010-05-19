/*
 * JBoss, Home of Professional Open Source.
 * See the COPYRIGHT.txt file distributed with this work for information
 * regarding copyright ownership.  Some portions may be licensed
 * to Red Hat, Inc. under one or more contributor license agreements.
 * 
 * This library is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License as published by the Free Software Foundation; either
 * version 2.1 of the License, or (at your option) any later version.
 * 
 * This library is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 * Lesser General Public License for more details.
 * 
 * You should have received a copy of the GNU Lesser General Public
 * License along with this library; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA
 * 02110-1301 USA.
 */
package org.teiid.rhq.plugin.adapter.impl;

import org.rhq.core.domain.configuration.PropertyMap;
import org.rhq.core.domain.configuration.definition.PropertyDefinitionMap;
import org.teiid.rhq.plugin.adapter.api.PropertyAdapter;

import org.jboss.metatype.api.types.MapCompositeMetaType;
import org.jboss.metatype.api.types.MetaType;
import org.jboss.metatype.api.values.CompositeValue;
import org.jboss.metatype.api.values.MapCompositeValueSupport;
import org.jboss.metatype.api.values.MetaValue;

/**
 * This class provides code that maps back and forth between a {@link PropertyMap} and a {@link
 * MapCompositeValueSupport}. A <code>MapCompositeValueSupport</code> is a {@link CompositeValue} implementation that
 * contains items that are all of the same type; there is no predefined list of valid keys, so it maps nicely to an
 * "open" PropertyMap.
 *
 */
public class PropertyMapToMapCompositeValueSupportAdapter extends AbstractPropertyMapToCompositeValueAdapter implements PropertyAdapter<PropertyMap, PropertyDefinitionMap>
{
    @Override
    public void populateMetaValueFromProperty(PropertyMap propMap, MetaValue metaValue, PropertyDefinitionMap propDefMap)
    {
        MapCompositeValueSupport mapCompositeValueSupport = (MapCompositeValueSupport)metaValue;
        // First clear out all existing values from the MapCompositeValue.
        for (String key : mapCompositeValueSupport.getMetaType().keySet())
        {
            mapCompositeValueSupport.remove(key);
        }
        // Now re-populate it with the values from the PropertyMap.
        super.populateMetaValueFromProperty(propMap, metaValue, propDefMap);
    }

    protected void putValue(CompositeValue compositeValue, String key, MetaValue value)
    {
        MapCompositeValueSupport mapCompositeValueSupport = (MapCompositeValueSupport)compositeValue;
        mapCompositeValueSupport.put(key, value);
    }

    protected CompositeValue createCompositeValue(PropertyDefinitionMap propDefMap, MetaType metaType)
    {
        MapCompositeMetaType mapCompositeMetaType = (MapCompositeMetaType)metaType;
        MetaType mapMemberMetaType = mapCompositeMetaType.getValueType();
        return new MapCompositeValueSupport(mapMemberMetaType);
    }
}
