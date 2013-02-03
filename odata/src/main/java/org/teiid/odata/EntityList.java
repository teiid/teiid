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
import java.util.*;

import org.odata4j.core.*;
import org.odata4j.edm.*;
import org.odata4j.exceptions.ServerErrorException;
import org.teiid.client.util.ResultsFuture;
import org.teiid.core.types.BlobType;
import org.teiid.core.types.ClobType;
import org.teiid.core.types.DataTypeManager;
import org.teiid.core.types.Transform;
import org.teiid.core.types.TransformationException;
import org.teiid.translator.odata.ODataTypeManager;

class EntityList extends AbstractList<OEntity>{
	private ResultSet rs;
	private EdmEntitySet entitySet;
	ResultsFuture<Boolean> completion;
	private int size = 0;
	private OEntity prevEntity;
	private OEntity currentEntity;
	private boolean closed = false;
	private Map<String, Boolean> projectedColumns;
	private HashMap<String, EdmProperty> propertyTypes = new HashMap<String, EdmProperty>();
	
	public EntityList(Map<String, Boolean> columns, EdmEntitySet entitySet, ResultSet rs,  ResultsFuture<Boolean> complition) throws SQLException {
		this.entitySet = entitySet;
		this.rs = rs;
		this.completion = complition;
		this.projectedColumns = columns;
		
		if (this.projectedColumns == null) {
			this.projectedColumns = new HashMap<String, Boolean>();
			for (int i = 0; i < rs.getMetaData().getColumnCount(); i++) {
				this.projectedColumns.put(rs.getMetaData().getColumnLabel(i+1), Boolean.TRUE);
			}
		}

		EdmEntityType entityType = this.entitySet.getType();
		Iterator<EdmProperty> propIter = entityType.getProperties().iterator();
		while(propIter.hasNext()) {
			EdmProperty prop = propIter.next();
			this.propertyTypes.put(prop.getName(), prop);
		}
		
		this.prevEntity = getEntity();
		if (this.prevEntity != null) {
			this.size += 1;
		}
	}
	
	@Override
	public OEntity get(int index) {
		if (this.prevEntity != null) {
			this.currentEntity = this.prevEntity;
			this.prevEntity = getEntity();;
			if (this.prevEntity != null) {
				this.size += 1;
			}
			return this.currentEntity;
		}
		return null;
	}
	
	private OProperty<?> buildPropery(String propName, EdmSimpleType expectedType, Object value) throws TransformationException, SQLException, IOException {
		if (value == null) {
			return OProperties.null_(propName, expectedType);
		}
		Class sourceType = DataTypeManager.getRuntimeType(value.getClass());
		Class targetType = DataTypeManager.getDataTypeClass(ODataTypeManager.teiidType(expectedType.getFullyQualifiedTypeName()));
		if (sourceType != targetType) {
			Transform t = DataTypeManager.getTransform(sourceType,targetType);
			if (t == null && BlobType.class == targetType && sourceType == ClobType.class) {
				return OProperties.binary(propName, ClobType.getString((Clob)value).getBytes());
			}
			else if (t == null && BlobType.class == targetType && sourceType == SQLXML.class) {
				return OProperties.binary(propName, ((SQLXML)value).getString().getBytes());
			}			
			return OProperties.simple(propName, expectedType, t!=null?t.transform(value):value);
		}
		return OProperties.simple(propName, expectedType,value);
	}

	private OEntity getEntity() {
		if (!this.closed) {
			try {
				if (rs.next()) {					
					HashMap<String, OProperty<?>> properties = new HashMap<String, OProperty<?>>();
					for (int i = 0; i < rs.getMetaData().getColumnCount(); i++) {
						Object value = rs.getObject(i+1);
						String propName = rs.getMetaData().getColumnLabel(i+1);
						EdmSimpleType type = (EdmSimpleType)this.propertyTypes.get(propName).getType();
						OProperty<?> property = buildPropery(propName, type, value);
						properties.put(rs.getMetaData().getColumnLabel(i+1), property);	
					}			
					
					OEntityKey key = OEntityKey.infer(this.entitySet, new ArrayList<OProperty<?>>(properties.values()));
					
					ArrayList<OLink> links = new ArrayList<OLink>();
					
					for (EdmNavigationProperty navProperty:this.entitySet.getType().getNavigationProperties()) {
						links.add(OLinks.relatedEntity(navProperty.getRelationship().getName(), navProperty.getToRole().getRole(), key.toKeyString()));
					}

					// properties can contain more than what is requested in project to build links
					// filter those columns out.
					ArrayList<OProperty<?>> projected = new ArrayList<OProperty<?>>();
					for (String column:this.projectedColumns.keySet()) {
						if (this.projectedColumns.get(column)) {
							projected.add(properties.get(column));
						}
					}
					return OEntities.create(this.entitySet, key, projected, links);
				}
				this.closed = true;
				this.completion.getResultsReceiver().receiveResults(Boolean.TRUE);
			} catch(SQLException e) {
				// ex ignored on completion				
				this.completion.getResultsReceiver().exceptionOccurred(e);
				throw new ServerErrorException(e.getMessage(), e);
			} catch (TransformationException e) {
				// ex ignored on completion
				this.completion.getResultsReceiver().exceptionOccurred(e);
				throw new ServerErrorException(e.getMessage(), e);
			} catch (IOException e) {
				// ex ignored on completion
				this.completion.getResultsReceiver().exceptionOccurred(e);
				throw new ServerErrorException(e.getMessage(), e);
			}
		}
		return null;
	}

	@Override
	public int size() {
		return size;
	}

}
