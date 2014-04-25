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
import java.util.HashMap;
import java.util.Map;

import org.odata4j.core.OEntities;
import org.odata4j.core.OEntity;
import org.odata4j.core.OEntityKey;
import org.odata4j.core.OLink;
import org.odata4j.core.OLinks;
import org.odata4j.core.OProperty;
import org.odata4j.edm.EdmEntitySet;
import org.odata4j.edm.EdmNavigationProperty;
import org.odata4j.edm.EdmProperty;
import org.odata4j.edm.EdmType;
import org.teiid.core.types.TransformationException;

class EntityList extends ArrayList<OEntity>{
	private int count = 0;
	private String skipToken;
	private String invalidCharacterReplacement;
	
	public EntityList(String invalidCharacterReplacement) throws SQLException, TransformationException, IOException {
	    this.invalidCharacterReplacement = invalidCharacterReplacement;
	}
	
	void addEntity(ResultSet rs, Map<String, EdmProperty> propertyTypes, Map<String, Boolean> columns, EdmEntitySet entitySet) throws TransformationException, SQLException, IOException {
		this.add(getEntity(rs, propertyTypes, columns, entitySet));
	}

	private OEntity getEntity(ResultSet rs, Map<String, EdmProperty> propertyTypes, Map<String, Boolean> columns, EdmEntitySet entitySet) throws TransformationException, SQLException, IOException {
		HashMap<String, OProperty<?>> properties = new HashMap<String, OProperty<?>>();
		for (int i = 0; i < rs.getMetaData().getColumnCount(); i++) {
			Object value = rs.getObject(i+1);
			String propName = rs.getMetaData().getColumnLabel(i+1);
			EdmType type = propertyTypes.get(propName).getType();
			OProperty<?> property = LocalClient.buildPropery(propName, type, value, invalidCharacterReplacement);
			properties.put(rs.getMetaData().getColumnLabel(i+1), property);	
		}			
		
		OEntityKey key = OEntityKey.infer(entitySet, new ArrayList<OProperty<?>>(properties.values()));
		
		ArrayList<OLink> links = new ArrayList<OLink>();
		
		for (EdmNavigationProperty navProperty:entitySet.getType().getNavigationProperties()) {
			links.add(OLinks.relatedEntity(navProperty.getRelationship().getName(), navProperty.getToRole().getRole(), key.toKeyString()));
		}

		// properties can contain more than what is requested in project to build links
		// filter those columns out.
		ArrayList<OProperty<?>> projected = new ArrayList<OProperty<?>>();
		for (Map.Entry<String,Boolean> entry:columns.entrySet()) {
			if (entry.getValue() != null && entry.getValue()) {
				projected.add(properties.get(entry.getKey()));
			}
		}
		return OEntities.create(entitySet, key, projected, links);
	}
	
	public int getCount() {
		return count;
	}

	public String nextToken() {
		return this.skipToken;
	}
	
	public void setSkipToken(String skipToken) {
		this.skipToken = skipToken;
	}
	
	public void setCount(int count) {
		this.count = count;
	}
	
}
