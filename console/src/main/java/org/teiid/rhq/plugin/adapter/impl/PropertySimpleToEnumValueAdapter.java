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

import org.jboss.metatype.api.types.EnumMetaType;
import org.jboss.metatype.api.types.MetaType;
import org.jboss.metatype.api.values.EnumValue;
import org.jboss.metatype.api.values.EnumValueSupport;
import org.jboss.metatype.api.values.MetaValue;
import org.rhq.core.domain.configuration.PropertySimple;
import org.rhq.core.domain.configuration.definition.PropertyDefinitionSimple;
import org.teiid.rhq.plugin.adapter.api.AbstractPropertySimpleAdapter;
import org.teiid.rhq.plugin.adapter.api.PropertyAdapter;

/**
 * This class provides code that maps back and forth between a {@link PropertySimple} and an {@link EnumValueSupport}.
 *
 */
public class PropertySimpleToEnumValueAdapter extends AbstractPropertySimpleAdapter implements PropertyAdapter<PropertySimple, PropertyDefinitionSimple>
{
    public void populatePropertyFromMetaValue(PropertySimple propSimple, MetaValue metaValue, PropertyDefinitionSimple propDefSimple)
    {
        Object value = (metaValue != null) ? ((EnumValue)metaValue).getValue() : null;
        propSimple.setValue(value);
    }

    public MetaValue convertToMetaValue(PropertySimple propSimple, PropertyDefinitionSimple propDefSimple, MetaType metaType)
    {
        EnumValue enumValue = new EnumValueSupport((EnumMetaType)metaType, propSimple.getStringValue());
        populateMetaValueFromProperty(propSimple, enumValue, propDefSimple);
        return enumValue;
    }

    protected void setInnerValue(String propSimpleValue, MetaValue metaValue, PropertyDefinitionSimple propDefSimple)
    {
        EnumValueSupport enumValueSupport = (EnumValueSupport)metaValue;
        enumValueSupport.setValue(propSimpleValue);
    }
}