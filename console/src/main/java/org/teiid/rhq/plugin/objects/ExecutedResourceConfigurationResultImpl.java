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

import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.rhq.core.domain.configuration.PropertyList;
import org.rhq.core.domain.configuration.PropertyMap;
import org.rhq.core.domain.configuration.PropertySimple;
import org.rhq.core.domain.configuration.definition.PropertyDefinition;
import org.rhq.core.domain.configuration.definition.PropertyDefinitionList;
import org.rhq.core.domain.configuration.definition.PropertyDefinitionMap;
import org.rhq.core.domain.configuration.definition.PropertyDefinitionSimple;
import org.rhq.core.domain.operation.OperationDefinition;
import org.rhq.core.pluginapi.operation.OperationResult;
import org.teiid.rhq.comm.ExecutedResult;


public class ExecutedResourceConfigurationResultImpl implements ExecutedResult {

	Map propertDefinitions;
	
	String operationName;
	
	String componentType;

	final static String LISTNAME = "list"; //$NON-NLS-1$
	
	final static String MAPNAME = "map"; //$NON-NLS-1$

	Object result;
	
	Object content;

	List fieldNameList;
	

	public ExecutedResourceConfigurationResultImpl(String componentType, String operationName, Map propDefs) {
		this.componentType = componentType;
		this.operationName = operationName;
		this.propertDefinitions = propDefs;
		init();
	}
	
	public void reset() {
		result = null;
		content = null;
	}
	
	public String getComponentType() {
		return this.componentType;
	}
	
	public String getOperationName() {
		return this.operationName;
	}
		

	
	public List getFieldNameList() {
		return fieldNameList;
	}

	public Object getResult() {
		return result;
	}
	
	private void setComplexResult() {
		PropertyList list = new PropertyList(LISTNAME); //$NON-NLS-1$
		PropertyMap pm;
		Iterator resultIter = ((List)content).iterator();
		while (resultIter.hasNext()) {
			Map reportRowMap = (Map) resultIter.next();
			Iterator reportRowKeySetIter = reportRowMap.keySet().iterator();
			pm = new PropertyMap("userMap"); //$NON-NLS-1$			

			while (reportRowKeySetIter.hasNext()) {
				String key = (String) reportRowKeySetIter.next();
				pm.put(new PropertySimple(key, reportRowMap.get(key)==null?"":reportRowMap.get(key))); //$NON-NLS-1$
			}
			list.add(pm);
		}
		
		result = list;
	}
	

	public void setContent(List content) {
		this.content = content;
		setComplexResult();
	}

	public void setContent(String content) {
		this.content = content;
	}

	private void init() {
		fieldNameList = new LinkedList();

		PropertyDefinition listPropDefinition = (PropertyDefinition) propertDefinitions
				.get(LISTNAME);

		PropertyDefinition propertyDefinitionMap = ((PropertyDefinitionList) listPropDefinition)
				.getMemberDefinition();
		Map simpleProperties = ((PropertyDefinitionMap) propertyDefinitionMap)
				.getPropertyDefinitions();
		Iterator simplePropertiesIter = simpleProperties.values()
				.iterator();

		while (simplePropertiesIter.hasNext()) {
			PropertyDefinition simpleProp = (PropertyDefinition) simplePropertiesIter
					.next();
			String fieldName = ((PropertyDefinitionSimple) simpleProp)
					.getName();
			fieldNameList.add(fieldName);
		}
				

	}	
}
