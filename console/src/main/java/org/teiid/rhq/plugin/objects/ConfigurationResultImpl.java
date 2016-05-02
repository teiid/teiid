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
package org.teiid.rhq.plugin.objects;

import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import org.rhq.core.domain.configuration.PropertySimple;
import org.rhq.core.domain.configuration.definition.PropertyDefinition;
import org.rhq.core.domain.configuration.definition.PropertyDefinitionList;
import org.rhq.core.domain.configuration.definition.PropertyDefinitionSimple;
import org.rhq.core.pluginapi.inventory.ResourceDiscoveryContext;
import org.teiid.rhq.plugin.util.ProfileServiceUtil;


public class ConfigurationResultImpl  {
	
		
	public static Collection mapProperties(ResourceDiscoveryContext discoveryContext, Properties values ) {
		Set<PropertySimple> properties = new HashSet();
		if (discoveryContext.getResourceType().getPluginConfigurationDefinition() == null) {
			return Collections.EMPTY_LIST;
		}
		Map<String , PropertyDefinition> propDefs = discoveryContext.getResourceType().getPluginConfigurationDefinition().getPropertyDefinitions();
		
		Iterator<String> propkeys = propDefs.keySet().iterator();
		
		while (propkeys.hasNext()) {
			final String key = propkeys.next();
			PropertyDefinition pdef = propDefs.get(key);
			
			if (pdef instanceof PropertyDefinitionSimple) {
				String fieldName = ((PropertyDefinitionSimple) pdef).getName();
				if (values.containsKey(fieldName)) {
				
					properties.add(new PropertySimple(key, values.get(fieldName)));
				}
			} else if (pdef instanceof PropertyDefinitionList) {
				
				PropertyDefinition propertyDefinitionMap = ((PropertyDefinitionList) pdef)
						.getMemberDefinition();
				
				  //Need to handle RHQ 4.4 and 4.2.
	            List<PropertyDefinition> propDefList =ProfileServiceUtil.reflectivelyInvokeGetMapMethod(propertyDefinitionMap);
	    	
				for  (PropertyDefinition definition : propDefList) {
					String fieldName = definition.getName();
					if (values.contains(fieldName)) {
						properties.add(new PropertySimple(key, values.get(fieldName)));
					}
				}
		
			}
			
		}
				
		return properties;
	
	}	
	
}
