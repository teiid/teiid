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

import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.jboss.metatype.api.types.CollectionMetaType;
import org.jboss.metatype.api.types.MetaType;
import org.jboss.metatype.api.values.CollectionValue;
import org.jboss.metatype.api.values.CollectionValueSupport;
import org.jboss.metatype.api.values.MetaValue;
import org.rhq.core.domain.configuration.Property;
import org.rhq.core.domain.configuration.PropertyList;
import org.rhq.core.domain.configuration.definition.PropertyDefinition;
import org.rhq.core.domain.configuration.definition.PropertyDefinitionList;
import org.teiid.rhq.plugin.adapter.api.AbstractPropertyListAdapter;
import org.teiid.rhq.plugin.adapter.api.PropertyAdapter;
import org.teiid.rhq.plugin.adapter.api.PropertyAdapterFactory;
import org.teiid.rhq.plugin.util.PluginConstants;

/**
 *  
 */
public class PropertyListToCollectionValueAdapter extends AbstractPropertyListAdapter implements PropertyAdapter<PropertyList, PropertyDefinitionList>
{
    private static final Log LOG = LogFactory.getLog(PluginConstants.DEFAULT_LOGGER_CATEGORY);

    public void populateMetaValueFromProperty(PropertyList property, MetaValue metaValue, PropertyDefinitionList propertyDefinition)
    {
        PropertyDefinition listMemberPropDef = propertyDefinition.getMemberDefinition();
        List<Property> listMemberProps = property.getList();
        CollectionValueSupport collectionValue = (CollectionValueSupport)metaValue;
        MetaType listMemberMetaType = collectionValue.getMetaType().getElementType();
        MetaValue[] listMemberValues = new MetaValue[listMemberProps.size()];
        PropertyAdapter propertyAdapter = PropertyAdapterFactory.getPropertyAdapter(listMemberMetaType);
        int memberIndex = 0;
        for (Property listMemberProp : listMemberProps)
        {
            MetaValue listMemberValue = propertyAdapter.convertToMetaValue(listMemberProp, listMemberPropDef, listMemberMetaType);
            listMemberValues[memberIndex++] = listMemberValue;
        }
        // Replace the existing elements with the new ones.
        collectionValue.setElements(listMemberValues);
    }

    public MetaValue convertToMetaValue(PropertyList propertyList, PropertyDefinitionList propertyListDefinition, MetaType metaType)
    {
        LOG.debug("GetMetaValue for property: " + propertyList.getName() + " values: " + propertyList.getList().toString());
        CollectionMetaType collectionMetaType = (CollectionMetaType)metaType;
        MetaType memberMetaType = collectionMetaType.getElementType();
        CollectionMetaType collectionType = new CollectionMetaType(propertyListDefinition.getName(), memberMetaType);
        CollectionValue collectionValue = new CollectionValueSupport(collectionType);
        populateMetaValueFromProperty(propertyList, collectionValue, propertyListDefinition);
        return collectionValue;
    }

    public void populatePropertyFromMetaValue(PropertyList propList, MetaValue metaValue, PropertyDefinitionList propDefList)
    {
        PropertyDefinition memberPropDef = propDefList.getMemberDefinition();
        if (propList != null)
        {
            // Since we want to load the PropertyList with fresh values, we want it cleared out.
            propList.getList().clear();
            if (metaValue != null)
            {
                CollectionValue collectionValue = (CollectionValue)metaValue;
                MetaType listMemberMetaType = collectionValue.getMetaType().getElementType();
                MetaValue[] listMemberValues = collectionValue.getElements();
                PropertyAdapter propertyAdapter = PropertyAdapterFactory.getPropertyAdapter(listMemberMetaType);
                for (MetaValue listMemberValue : listMemberValues)
                {
                    Property listMemberProp = propertyAdapter.convertToProperty(listMemberValue, memberPropDef);
                    propList.add(listMemberProp);
                }
            }
        }
    }
}
