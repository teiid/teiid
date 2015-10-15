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
import java.util.List;

import org.odata4j.core.OEntities;
import org.odata4j.core.OEntity;
import org.odata4j.core.OEntityKey;
import org.odata4j.core.OLink;
import org.odata4j.core.OLinks;
import org.odata4j.core.OProperty;
import org.odata4j.edm.EdmDataServices;
import org.odata4j.edm.EdmEntitySet;
import org.odata4j.edm.EdmEntityType;
import org.odata4j.edm.EdmMultiplicity;
import org.odata4j.edm.EdmNavigationProperty;
import org.odata4j.edm.EdmType;
import org.odata4j.producer.EntitiesResponse;
import org.odata4j.producer.InlineCount;
import org.odata4j.producer.QueryInfo;
import org.teiid.core.types.TransformationException;
import org.teiid.odata.DocumentNode.ProjectedColumn;
import org.teiid.translator.odata.ODataTypeManager;

class EntityList extends ArrayList<OEntity> implements EntitiesResponse, EntityCollector<OEntity> {
    private static final long serialVersionUID = -6805594611842072222L;
    private int count = 0;
	private String skipToken;
	private QueryInfo queryInfo;
	private ArrayList<OEntity> expandEntities = new ArrayList<OEntity>();
	private DocumentNode documentNode;
	private EdmDataServices metadata;
	
    public EntityList(DocumentNode node, EdmDataServices metadata, QueryInfo queryInfo){
	    this.documentNode = node;
	    this.metadata = metadata;
	    this.queryInfo = queryInfo;
	}
	
    @Override
    public OEntity addRow(Object previous, ResultSet rs, String invalidCharacterReplacement)
            throws TransformationException, SQLException, IOException {
        
        OEntity previousEntity = (OEntity)previous;
        
        OEntity entity = buildEntity(rs, this.documentNode,
                this.documentNode.getEntitySet(this.metadata),
                invalidCharacterReplacement);
                
        if (previousEntity != null) {
            if(!isSameRow(previousEntity, entity)) {
              if (this.expandEntities.isEmpty()) {
                  add(previousEntity);
              } else {
                  // this indicates the first row on next entity, so add the previous one to list 
                  EdmEntitySet expandEntitySet = this.documentNode
                          .getExpandNode()
                          .getEntitySet(this.metadata);          
                  this.add(buildEntityWithExpand(previousEntity, expandEntitySet, this.expandEntities));
                  this.expandEntities.clear();                  
              }
            }            
        }
        
        if (this.documentNode.getExpandNode() != null) {
            EdmEntitySet expandES = this.documentNode
                    .getExpandNode()
                    .getEntitySet(this.metadata);                                
            OEntity expand = buildEntity(rs, this.documentNode.getExpandNode(),
                    expandES, invalidCharacterReplacement);
            this.expandEntities.add(expand);
        }
        return entity;
    }
    
    private static OEntity buildEntityWithExpand(OEntity srcEntity, EdmEntitySet expandES, List<OEntity> expands) {
        if (expands.isEmpty()) {
            return srcEntity;
        }
        
        List<OLink> links = new ArrayList<OLink>();
        for (OLink link : srcEntity.getLinks()) {
            if (link.getTitle().equals(expandES.getName())) {
                if (link.isCollection()) {
                    links.add(OLinks.relatedEntitiesInline(link.getRelation(),
                            link.getTitle(), link.getHref(), new ArrayList<OEntity>(expands)));
                } else {
                    links.add(OLinks.relatedEntityInline(link.getRelation(),
                            link.getTitle(), link.getHref(), expands.get(0)));
                }
            } else {
                links.add(link);
            }
        }
        return OEntities.create(
                srcEntity.getEntitySet(), 
                srcEntity.getEntityKey(), 
                srcEntity.getProperties(), 
                links);
    }    

    private boolean isSameRow(OEntity previous, OEntity current) {
        EdmEntityType entityType = current.getEntityType();
        for (String name:entityType.getKeys()) {
            if (!current.getProperty(name).getValue().equals(previous.getProperty(name).getValue())) {
                return false;
            }
        }
        return true;
    }    
    
    @Override
    public void lastRow(Object last) {
        OEntity entity = (OEntity)last;
        if (entity != null) {
            if (this.expandEntities.isEmpty()) {
                this.add(entity);
            } else {
                EdmEntitySet expandEntitySet = this.documentNode
                        .getExpandNode()
                        .getEntitySet(this.metadata);          
                
                this.add(buildEntityWithExpand(entity,
                        expandEntitySet, this.expandEntities));
                this.expandEntities.clear();
            }
        }
    }

    @Override
    public boolean isSameRow(Object previous, Object current) {
        return isSameRow((OEntity)previous,  (OEntity)current);
    }    
    
    private OEntity buildEntity(ResultSet rs, DocumentNode node, EdmEntitySet edmEntitySet, String invalidCharacterReplacement)
            throws TransformationException, SQLException, IOException {
        
        HashMap<String, OProperty<?>> properties = new HashMap<String, OProperty<?>>();
        if (node.getProjectedColumns().isEmpty()) {
            for (int i = 0; i < rs.getMetaData().getColumnCount(); i++) {
                Object value = rs.getObject(i+1);
                String propName = rs.getMetaData().getColumnLabel(i+1);
                String tableName = rs.getMetaData().getTableName(i+1);
                if (tableName.equals(node.getEntityTable().getName())) {
                    EdmType type = ODataTypeManager.odataType(node.getEntityTable()
                            .getColumnByName(propName).getRuntimeType());
                    OProperty<?> property = LocalClient.buildPropery(propName,
                            type, value, invalidCharacterReplacement);
                    properties.put(rs.getMetaData().getColumnLabel(i+1), property); 
                }
            }
        }
        else {
            for (ProjectedColumn pc:node.getProjectedColumns().values()) {
                Object value = rs.getObject(pc.ordinal());
                OProperty<?> property = LocalClient.buildPropery(pc.name(), pc.type(),
                        value, invalidCharacterReplacement);
                properties.put(pc.name(), property);             
            }
        }
        
        OEntityKey key = OEntityKey.infer(edmEntitySet,
                new ArrayList<OProperty<?>>(properties.values()));

        ArrayList<OLink> links = new ArrayList<OLink>();
        for (EdmNavigationProperty navProperty:edmEntitySet.getType().getNavigationProperties()) {
          if (node.getParentNode() == null || !navProperty.getName().equals(node.getParentNode().getEntityTable().getName())) {            
            if (navProperty.getToRole().getMultiplicity().equals(EdmMultiplicity.ZERO_TO_ONE)) {
                links.add(OLinks.relatedEntity(navProperty.getRelationship().getName(), 
                        navProperty.getToRole().getRole(), key.toKeyString()));                
            } else {
                links.add(OLinks.relatedEntities(navProperty.getRelationship().getName(), 
                        navProperty.getToRole().getRole(), key.toKeyString()));                                
            }
          }
        }

        // properties can contain more than what is requested in project to build links
        // filter those columns out.        
        ArrayList<OProperty<?>> projected = new ArrayList<OProperty<?>>();
        if (node.getProjectedColumns().isEmpty()) {
            projected = new ArrayList<OProperty<?>>(properties.values());
        } else {
            for (ProjectedColumn pc:node.getProjectedColumns().values()) {
                if (properties.containsKey(pc.name()) && pc.visible()) {
                    projected.add(properties.get(pc.name()));
                }
            }
        }
        return OEntities.create(edmEntitySet, key, projected, links);
    }    
    
    @Override
	public void setSkipToken(String skipToken) {
		this.skipToken = skipToken;
	}
	
    @Override
	public void setInlineCount(int count) {
		this.count = count;
	}

    @Override
    public EdmEntitySet getEntitySet() {
        return this.documentNode.getEntitySet(this.metadata);
    }

    @Override
    public List<OEntity> getEntities() {
        return this;
    }

    @Override
    public Integer getInlineCount() {
        // when $expand is requested counting is not stright forward, 
        // and involves performance hit for counting, so avoid it
        if (queryInfo.inlineCount == InlineCount.ALLPAGES
                && this.documentNode.getExpandNode() == null) {
            return count;
        }
        return null;
    }

    @Override
    public String getSkipToken() {
        return this.skipToken;
    }
}
