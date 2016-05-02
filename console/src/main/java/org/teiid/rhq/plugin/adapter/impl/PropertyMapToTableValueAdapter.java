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

import java.util.Collection;
import java.util.List;
import java.util.Map;

import org.jboss.metatype.api.types.MetaType;
import org.jboss.metatype.api.values.CompositeValue;
import org.jboss.metatype.api.values.CompositeValueSupport;
import org.jboss.metatype.api.values.MetaValue;
import org.jboss.metatype.api.values.SimpleValueSupport;
import org.jboss.metatype.api.values.TableValueSupport;
import org.rhq.core.domain.configuration.Property;
import org.rhq.core.domain.configuration.PropertyMap;
import org.rhq.core.domain.configuration.definition.PropertyDefinition;
import org.rhq.core.domain.configuration.definition.PropertyDefinitionMap;
import org.teiid.rhq.plugin.adapter.api.AbstractPropertyMapAdapter;
import org.teiid.rhq.plugin.adapter.api.PropertyAdapter;
import org.teiid.rhq.plugin.adapter.api.PropertyAdapterFactory;
import org.teiid.rhq.plugin.util.ProfileServiceUtil;

/**
 * 
 */
public class PropertyMapToTableValueAdapter extends AbstractPropertyMapAdapter implements PropertyAdapter<PropertyMap, PropertyDefinitionMap>
{
    public PropertyMap convertToProperty(MetaValue metaValue, PropertyDefinitionMap propertyDefinition)
    {
        PropertyMap property = new PropertyMap();
        populatePropertyFromMetaValue(property, metaValue, propertyDefinition);
        return property;
    }

    //@todo need to implement this like the other Map to Composite, but not until there is an actual property that needs this
    public void populateMetaValueFromProperty(PropertyMap property, MetaValue metaValue, PropertyDefinitionMap propertyDefinitionMap)
    {
        if (metaValue != null)
        {
            TableValueSupport tableValueSupport = (TableValueSupport)metaValue;
            //Need to handle RHQ 4.4 and 4.2.
            List<PropertyDefinition> propDefList =ProfileServiceUtil.reflectivelyInvokeGetMapMethod(propertyDefinitionMap);
    	
			Map<String, Property> properties = property.getMap();
			for  (PropertyDefinition definition : propDefList) {
			    MetaValue[] getKey = new MetaValue[]{SimpleValueSupport.wrap(definition.getName())};
                MetaValue value = tableValueSupport.get(getKey);
                Property innerProperty = properties.get(definition.getName());
                PropertyAdapter adapter = PropertyAdapterFactory.getPropertyAdapter(value);
                adapter.populateMetaValueFromProperty(innerProperty, value, definition);
            }
        }
    }

    //@todo need to implement this like the other Map to Composite, but not until there is an actual property that needs this
    public MetaValue convertToMetaValue(PropertyMap property, PropertyDefinitionMap propertyDefinition, MetaType type)
    {
        return null;
    }

    public void populatePropertyFromMetaValue(PropertyMap property, MetaValue metaValue, PropertyDefinitionMap propertyDefinition)
    {
        // Not important at this moment to implement as there sin't a need for this mapping yet.
        if (metaValue != null)
        {
            TableValueSupport valueSupport = (TableValueSupport)metaValue;
            Collection<CompositeValue> values = valueSupport.values();
            for (CompositeValue value : values)
            {
                CompositeValueSupport support = (CompositeValueSupport)value;
            }
        }
    }
}
