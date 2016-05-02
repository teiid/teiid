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
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.jboss.managed.api.ManagedObject;
import org.jboss.managed.api.ManagedProperty;
import org.jboss.managed.plugins.ManagedObjectImpl;
import org.jboss.managed.plugins.ManagedPropertyImpl;
import org.jboss.metatype.api.types.GenericMetaType;
import org.jboss.metatype.api.types.MetaType;
import org.jboss.metatype.api.values.EnumValue;
import org.jboss.metatype.api.values.GenericValue;
import org.jboss.metatype.api.values.GenericValueSupport;
import org.jboss.metatype.api.values.MetaValue;
import org.jboss.metatype.api.values.SimpleValue;
import org.rhq.core.domain.configuration.Property;
import org.rhq.core.domain.configuration.PropertyMap;
import org.rhq.core.domain.configuration.PropertySimple;
import org.rhq.core.domain.configuration.definition.PropertyDefinition;
import org.rhq.core.domain.configuration.definition.PropertyDefinitionMap;
import org.teiid.rhq.plugin.adapter.api.AbstractPropertyMapAdapter;
import org.teiid.rhq.plugin.adapter.api.PropertyAdapter;
import org.teiid.rhq.plugin.adapter.api.PropertyAdapterFactory;
import org.teiid.rhq.plugin.util.PluginConstants;
import org.teiid.rhq.plugin.util.ProfileServiceUtil;

/**
 * This class provides code that maps back and forth between a {@link PropertyMap} and a {@link GenericValue}. The
 * GenericValue's value is assumed to be a {@link ManagedObject}.
 *
 */
public class PropertyMapToGenericValueAdapter extends AbstractPropertyMapAdapter
        implements PropertyAdapter<PropertyMap, PropertyDefinitionMap>
{
    private final Log log = LogFactory.getLog(PluginConstants.DEFAULT_LOGGER_CATEGORY);

    public void populateMetaValueFromProperty(PropertyMap propMap, MetaValue metaValue, PropertyDefinitionMap propDefMap)
    {
        GenericValue genericValue = (GenericValue)metaValue;
        if (!(genericValue.getValue() instanceof ManagedObject))
        {
            log.error("GenericValue's value [" + genericValue.getValue() + "] is not a ManagedObject - not supported!");
            return;
        }
        ManagedObject managedObject = (ManagedObject)genericValue.getValue();
        for (String propName : propMap.getMap().keySet())
        {
            Property mapMemberProp = propMap.get(propName);
            ManagedProperty managedProp = managedObject.getProperty(propName);
            MetaType metaType = managedProp.getMetaType();
            PropertyAdapter propertyAdapter = PropertyAdapterFactory.getPropertyAdapter(metaType);
            PropertyDefinition mapMemberPropDef = propDefMap.get(propName);
            if (managedProp.getValue() == null)
            {
                MetaValue managedPropMetaValue = propertyAdapter.convertToMetaValue(mapMemberProp, mapMemberPropDef, metaType);
                managedProp.setValue(managedPropMetaValue);
            }
            else
            {
                MetaValue managedPropMetaValue = (MetaValue)managedProp.getValue();
                propertyAdapter.populateMetaValueFromProperty(mapMemberProp, managedPropMetaValue, mapMemberPropDef);
            }
        }
    }

    public MetaValue convertToMetaValue(PropertyMap propMap, PropertyDefinitionMap propertyDefinitionMap, MetaType metaType)
    {
        //GenericMetaType genericMetaType = (GenericMetaType)metaType;
        ManagedObjectImpl managedObject = new ManagedObjectImpl(propertyDefinitionMap.getName());
      //Need to handle RHQ 4.4 and 4.2. Return types changed for PropertyDefinitionMap.getPropertyDefinitions()
        //so we need to check types.
        Object object = ((PropertyDefinitionMap) propertyDefinitionMap).getPropertyDefinitions();
        Iterable<PropertyDefinition> propDefIter = null;
        List<PropertyDefinition> propDefList = null;
        
        if (object instanceof Map){
        	propDefIter = (Iterable<PropertyDefinition>) ((Map)object).values().iterator();
        }else{
        	propDefList =  (List<PropertyDefinition>)object;
        }
	
		for  (PropertyDefinition definition : propDefIter != null?propDefIter:propDefList) {
            ManagedPropertyImpl managedProp = new ManagedPropertyImpl(definition.getName());
            MetaType managedPropMetaType = ProfileServiceUtil.convertPropertyDefinitionToMetaType(definition);
            managedProp.setMetaType(managedPropMetaType);
            managedProp.setManagedObject(managedObject);
            managedObject.getProperties().put(managedProp.getName(), managedProp);
        }
        GenericValue genericValue = new GenericValueSupport(new GenericMetaType(propertyDefinitionMap.getName(),
        		propertyDefinitionMap.getDescription()), managedObject);
        populateMetaValueFromProperty(propMap, genericValue, propertyDefinitionMap);
        return genericValue;
    }

    public void populatePropertyFromMetaValue(PropertyMap propMap, MetaValue metaValue, PropertyDefinitionMap propertyDefinitionMap)
    {
        GenericValue genericValue = (GenericValue)metaValue;
        ManagedObject managedObject = (ManagedObject)genericValue.getValue();
    	//Need to handle RHQ 4.4 and 4.2. Return types changed for PropertyDefinitionMap.getPropertyDefinitions()
        //so we need to check types.
        Object object = ((PropertyDefinitionMap) propertyDefinitionMap).getPropertyDefinitions();
        Iterable<PropertyDefinition> propDefIter = null;
      
        //Need to handle RHQ 4.4 and 4.2.
        List<PropertyDefinition> propDefList =ProfileServiceUtil.reflectivelyInvokeGetMapMethod(propertyDefinitionMap);
      
        for  (PropertyDefinition definition : propDefList) {
            ManagedProperty managedProp = managedObject.getProperty(definition.getName());
            if (managedProp != null)
            {
                MetaType metaType = managedProp.getMetaType();
                Object value;
                if (metaType.isSimple())
                {
                    SimpleValue simpleValue = (SimpleValue)managedProp.getValue();
                    value = simpleValue.getValue();
                }
                else if (metaType.isEnum())
                {
                    EnumValue enumValue = (EnumValue)managedProp.getValue();
                    value = enumValue.getValue();
                }
                else
                {
                    log.error("Nested ManagedProperty's value [" + managedProp.getValue()
                            + "] is not a SimpleValue or EnumValue - unsupported!");
                    continue;
                }
                propMap.put(new PropertySimple(definition.getName(), value));
            }
        }
    }
}
