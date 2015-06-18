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
package org.teiid.odata;

import java.io.IOException;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.odata4j.core.OEntities;
import org.odata4j.core.OEntity;
import org.odata4j.core.OEntityKey;
import org.odata4j.core.OLink;
import org.odata4j.core.OLinks;
import org.odata4j.core.OProperties;
import org.odata4j.core.OProperty;
import org.odata4j.edm.EdmEntitySet;
import org.odata4j.edm.EdmNavigationProperty;
import org.odata4j.edm.EdmType;
import org.teiid.core.types.TransformationException;
import org.teiid.odata.LocalClient.TypeInfo;

class EntityList extends ArrayList<OEntity>{
	private int count = 0;
	private String skipToken;
	private String invalidCharacterReplacement;
	
	public EntityList(String invalidCharacterReplacement) throws SQLException, TransformationException, IOException {
	    this.invalidCharacterReplacement = invalidCharacterReplacement;
	}
	
	void addEntity(ResultSet rs, Map<String, TypeInfo> propertyTypes, Map<String, Boolean> projectedProperties, EdmEntitySet entitySet) throws TransformationException, SQLException, IOException {
		LinkedHashMap<String, OProperty<?>> properties = new LinkedHashMap<String, OProperty<?>>();
		LinkedHashMap<String, List<OProperty<?>>> complexProperties = new LinkedHashMap<String, List<OProperty<?>>>();
		for (int i = 0; i < rs.getMetaData().getColumnCount(); i++) {
			Object value = rs.getObject(i+1);
			String propName = rs.getMetaData().getColumnLabel(i+1);
			TypeInfo typeInfo = propertyTypes.get(propName);
			EdmType type = typeInfo.fieldType;
			OProperty<?> property = LocalClient.buildPropery(propName, type, value, invalidCharacterReplacement);
			if (typeInfo.complexType != null) {
				List<OProperty<?>> props = complexProperties.get(typeInfo.columnGroup);
				if (props == null) {
					props = new ArrayList<OProperty<?>>();
					complexProperties.put(typeInfo.columnGroup, props);
				}
				props.add(property);
			} else {
				properties.put(rs.getMetaData().getColumnLabel(i+1), property);
			}
		}			
		for (Map.Entry<String, List<OProperty<?>>> entry : complexProperties.entrySet()) {
			properties.put(entry.getKey(), OProperties.complex(entry.getKey(), propertyTypes.get(entry.getValue().get(0).getName()).complexType, entry.getValue()));
		}
		OEntityKey key = OEntityKey.infer(entitySet, new ArrayList<OProperty<?>>(properties.values()));
		
		ArrayList<OLink> links = new ArrayList<OLink>();
		
		for (EdmNavigationProperty navProperty:entitySet.getType().getNavigationProperties()) {
			links.add(OLinks.relatedEntity(navProperty.getRelationship().getName(), navProperty.getToRole().getRole(), key.toKeyString()));
		}

		// properties can contain more than what is requested in project to build links
		// filter those columns out.
		ArrayList<OProperty<?>> projected = new ArrayList<OProperty<?>>();
		for (Map.Entry<String,Boolean> entry:projectedProperties.entrySet()) {
			if (entry.getValue() != null && entry.getValue()) {
				projected.add(properties.get(entry.getKey()));
			}
		}
		this.add(OEntities.create(entitySet, key, projected, links));
	}
	
	public int getCount() {
		return count;
	}

	public String nextToken() {
		return this.skipToken;
	}
	
	public void setNextToken(String skipToken) {
		this.skipToken = skipToken;
	}
	
	public void setCount(int count) {
		this.count = count;
	}
	
}
