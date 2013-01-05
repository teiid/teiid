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

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.*;

import org.odata4j.core.*;
import org.odata4j.edm.*;
import org.teiid.client.util.ResultsFuture;

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
	
	public EntityList(Map<String, Boolean> columns, EdmEntitySet entitySet, ResultSet rs,  ResultsFuture<Boolean> complition) {
		this.entitySet = entitySet;
		this.rs = rs;
		this.completion = complition;
		this.projectedColumns = columns;

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

	private OEntity getEntity() {
		if (!this.closed) {
			try {
				if (rs.next()) {					
					HashMap<String, OProperty<?>> properties = new HashMap<String, OProperty<?>>();
					for (int i = 0; i < rs.getMetaData().getColumnCount(); i++) {
						Object value = rs.getObject(i+1);
						OProperty<?> property = null;
						String propName = rs.getMetaData().getColumnLabel(i+1);
						if (value != null) {
							property = OProperties.simple(propName, (EdmSimpleType)this.propertyTypes.get(propName).getType(), value);
						}
						else {
							property = OProperties.null_(rs.getMetaData().getColumnLabel(i+1), (EdmSimpleType)this.propertyTypes.get(propName).getType());
						}
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
				this.completion.getResultsReceiver().exceptionOccurred(e);
			}
		}
		return null;
	}

	@Override
	public int size() {
		return size;
	}

}
