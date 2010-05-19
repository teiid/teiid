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

package org.teiid.rhq.plugin.adapter.api;

import org.rhq.core.domain.configuration.PropertyList;
import org.rhq.core.domain.configuration.definition.PropertyDefinitionList;

import org.jboss.metatype.api.values.MetaValue;

/**
 * A base class for PropertyList <-> ???MetaValue adapters.
 *
 */
public abstract class AbstractPropertyListAdapter implements PropertyAdapter<PropertyList, PropertyDefinitionList>
{
    public PropertyList convertToProperty(MetaValue metaValue, PropertyDefinitionList propDefList)
    {
        PropertyList propList = new PropertyList(propDefList.getName());
        populatePropertyFromMetaValue(propList, metaValue, propDefList);
        return propList;
    }
}
