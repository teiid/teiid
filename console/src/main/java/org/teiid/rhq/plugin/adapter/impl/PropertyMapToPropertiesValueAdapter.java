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

import java.util.HashSet;
import java.util.Set;

import org.jboss.metatype.api.types.MetaType;
import org.jboss.metatype.api.values.MetaValue;
import org.jboss.metatype.api.values.PropertiesMetaValue;
import org.rhq.core.domain.configuration.Property;
import org.rhq.core.domain.configuration.PropertyMap;
import org.rhq.core.domain.configuration.PropertySimple;
import org.rhq.core.domain.configuration.definition.PropertyDefinitionMap;
import org.teiid.rhq.plugin.adapter.api.AbstractPropertyMapAdapter;
import org.teiid.rhq.plugin.adapter.api.PropertyAdapter;

/**
 * This class provides code that maps back and forth between a {@link PropertyMap} and a {@link PropertiesMetaValue}. A
 * <code>PropertiesMetaValue</code> extends java.util.Properties. Its associated PropertiesMetaType may or may not
 * define member property names; since the RHQ Configuration system assumes a PropertyDefinitionMap either defines none
 * of its member properties (i.e. an "open map") or defines all of them, we just ignore the PropertiesMetaType and
 * always map to an open map.
 *
 */
public class PropertyMapToPropertiesValueAdapter extends AbstractPropertyMapAdapter
        implements PropertyAdapter<PropertyMap, PropertyDefinitionMap>
{
    public void populateMetaValueFromProperty(PropertyMap propMap, MetaValue metaValue, PropertyDefinitionMap propDefMap)
    {
        PropertiesMetaValue propertiesValue = (PropertiesMetaValue)metaValue;
        for (String mapMemberPropName : propMap.getMap().keySet())
        {
            Property mapMemberProp = propMap.get(mapMemberPropName);
            propertiesValue.setProperty(mapMemberProp.getName(), ((PropertySimple)mapMemberProp).getStringValue());
        }
    }

    public MetaValue convertToMetaValue(PropertyMap propMap, PropertyDefinitionMap propDefMap, MetaType metaType)
    {
        PropertiesMetaValue propertiesValue = new PropertiesMetaValue();
        populateMetaValueFromProperty(propMap, propertiesValue, propDefMap);
        return propertiesValue;
    }

    public void populatePropertyFromMetaValue(PropertyMap propMap, MetaValue metaValue, PropertyDefinitionMap propDefMap)
    {
        if (metaValue == null)
            return;
        PropertiesMetaValue propertiesValue = (PropertiesMetaValue)metaValue;
        Set<String> mapMemberPropNames = new HashSet();
        for (Object key : propertiesValue.keySet())
            mapMemberPropNames.add((String)key);
        // There won't be any keys when loading a Configuration for the first time.
        for (String mapMemberPropName : mapMemberPropNames)
        {
            // We assume the PropertyMap is an "open map", since that's what PropertiesMetaValue maps to.
            PropertySimple mapMemberProp = propMap.getSimple(mapMemberPropName);
            // Create a PropertySimple and populate it.
            if (mapMemberProp == null)
            {
                mapMemberProp = new PropertySimple(mapMemberPropName, propertiesValue.getProperty(mapMemberPropName));
                propMap.put(mapMemberProp);
            }
        }
    }
}