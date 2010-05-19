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

import java.util.ArrayList;
import java.util.List;

import org.rhq.core.domain.configuration.Property;
import org.rhq.core.domain.configuration.PropertyList;
import org.rhq.core.domain.configuration.definition.PropertyDefinition;
import org.rhq.core.domain.configuration.definition.PropertyDefinitionList;
import org.teiid.rhq.plugin.adapter.api.AbstractPropertyListAdapter;
import org.teiid.rhq.plugin.adapter.api.PropertyAdapter;
import org.teiid.rhq.plugin.adapter.api.PropertyAdapterFactory;

import org.jboss.metatype.api.types.MetaType;
import org.jboss.metatype.api.values.ArrayValueSupport;
import org.jboss.metatype.api.values.MetaValue;
import org.jboss.metatype.api.values.MetaValueFactory;

/**
 * 
 */
public class PropertyListToArrayValueAdapter extends AbstractPropertyListAdapter implements PropertyAdapter<PropertyList, PropertyDefinitionList>
{
    //@todo need to implement this like the other List to Collection, but not until there is an actual property that needs this
    public void populateMetaValueFromProperty(PropertyList property, MetaValue metaValue, PropertyDefinitionList propertyDefinition)
    {
        PropertyDefinition memberDefinition = propertyDefinition.getMemberDefinition();
        List<Property> properties = property.getList();
        if (metaValue != null)
        {
            ArrayValueSupport valueSupport = (ArrayValueSupport)metaValue;
            MetaType listMetaType = valueSupport.getMetaType().getElementType();
            List<MetaValue> values = new ArrayList<MetaValue>(properties.size());
            for (Property propertyWithinList : properties)
            {
                MetaValue value = MetaValueFactory.getInstance().create(null);
                PropertyAdapter<Property, PropertyDefinition> propertyAdapter = PropertyAdapterFactory.getPropertyAdapter(listMetaType);
                propertyAdapter.populateMetaValueFromProperty(propertyWithinList, value, memberDefinition);
                values.add(value);
            }
            valueSupport.setValue(values.toArray());
        }
    }

    //@todo need to implement this like the other List to Collection, but not until there is an actual property that needs this
    public MetaValue convertToMetaValue(PropertyList property, PropertyDefinitionList propertyDefinition, MetaType type)
    {
        return null;
    }

    public void populatePropertyFromMetaValue(PropertyList property, MetaValue metaValue, PropertyDefinitionList propertyDefinition)
    {
        PropertyDefinition memberDefinition = propertyDefinition.getMemberDefinition();
        List<Property> properties = property.getList();

        // Since we want to load the PropertyList with fresh values, we want it cleared out
        properties.clear();

        if (metaValue != null)
        {
            ArrayValueSupport valueSupport = (ArrayValueSupport)metaValue;
            MetaType listMetaType = valueSupport.getMetaType().getElementType();
            MetaValue[] metaValues = (MetaValue[])valueSupport.getValue();
            PropertyAdapter propertyAdapter = PropertyAdapterFactory.getPropertyAdapter(listMetaType);
            for (MetaValue value : metaValues)
            {
                Property propertyToAddToList = propertyAdapter.convertToProperty(value, memberDefinition);
                properties.add(propertyToAddToList);
            }
        }
    }
}
