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
import java.sql.Clob;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLXML;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import org.odata4j.core.OEntities;
import org.odata4j.core.OEntity;
import org.odata4j.core.OEntityKey;
import org.odata4j.core.OLink;
import org.odata4j.core.OLinks;
import org.odata4j.core.OProperties;
import org.odata4j.core.OProperty;
import org.odata4j.edm.EdmEntitySet;
import org.odata4j.edm.EdmEntityType;
import org.odata4j.edm.EdmNavigationProperty;
import org.odata4j.edm.EdmProperty;
import org.odata4j.edm.EdmSimpleType;
import org.teiid.core.types.BlobType;
import org.teiid.core.types.ClobType;
import org.teiid.core.types.DataTypeManager;
import org.teiid.core.types.Transform;
import org.teiid.core.types.TransformationException;
import org.teiid.translator.odata.ODataTypeManager;

class EntityList extends ArrayList<OEntity>{
	private int count = 0;
	private int batchSize;
	private int skipSize;
	
	public EntityList(Map<String, Boolean> columns, EdmEntitySet entitySet, ResultSet rs, int skipSize, int batchSize, boolean getCount) throws SQLException, TransformationException, IOException {
		this.batchSize = batchSize;
		this.skipSize = skipSize;
		
		if (columns == null) {
			columns = new HashMap<String, Boolean>();
			for (int i = 0; i < rs.getMetaData().getColumnCount(); i++) {
				columns.put(rs.getMetaData().getColumnLabel(i+1), Boolean.TRUE);
			}
		}

		HashMap<String, EdmProperty> propertyTypes = new HashMap<String, EdmProperty>();
		
		EdmEntityType entityType = entitySet.getType();
		Iterator<EdmProperty> propIter = entityType.getProperties().iterator();
		while(propIter.hasNext()) {
			EdmProperty prop = propIter.next();
			propertyTypes.put(prop.getName(), prop);
		}
		int size = batchSize;
		if (getCount && rs.last()) {
			this.count = rs.getRow();
		}
		for (int i = 1; i <= size; i++) {
			if (!rs.absolute(i)) {
				break;
			}
			this.add(getEntity(rs, propertyTypes, columns, entitySet));
		}
	}
	
	private OProperty<?> buildPropery(String propName, EdmSimpleType expectedType, Object value) throws TransformationException, SQLException, IOException {
		if (value == null) {
			return OProperties.null_(propName, expectedType);
		}
		Class<?> sourceType = DataTypeManager.getRuntimeType(value.getClass());
		Class<?> targetType = DataTypeManager.getDataTypeClass(ODataTypeManager.teiidType(expectedType.getFullyQualifiedTypeName()));
		if (sourceType != targetType) {
			Transform t = DataTypeManager.getTransform(sourceType,targetType);
			if (t == null && BlobType.class == targetType && sourceType == ClobType.class) {
				return OProperties.binary(propName, ClobType.getString((Clob)value).getBytes());
			}
			else if (t == null && BlobType.class == targetType && sourceType == SQLXML.class) {
				return OProperties.binary(propName, ((SQLXML)value).getString().getBytes());
			}			
			return OProperties.simple(propName, expectedType, t!=null?t.transform(value, targetType):value);
		}
		return OProperties.simple(propName, expectedType,value);
	}

	private OEntity getEntity(ResultSet rs, Map<String, EdmProperty> propertyTypes, Map<String, Boolean> columns, EdmEntitySet entitySet) throws TransformationException, SQLException, IOException {
		HashMap<String, OProperty<?>> properties = new HashMap<String, OProperty<?>>();
		for (int i = 0; i < rs.getMetaData().getColumnCount(); i++) {
			Object value = rs.getObject(i+1);
			String propName = rs.getMetaData().getColumnLabel(i+1);
			EdmSimpleType type = (EdmSimpleType)propertyTypes.get(propName).getType();
			OProperty<?> property = buildPropery(propName, type, value);
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
		if (size() < this.batchSize) {
			return null;
		}
		return String.valueOf(this.skipSize + size());
	}

}
