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
package org.teiid.olingo.service;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.net.URI;
import java.net.URISyntaxException;
import java.sql.Array;
import java.sql.Blob;
import java.sql.Clob;
import java.sql.Date;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLXML;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.apache.olingo.commons.api.data.Entity;
import org.apache.olingo.commons.api.data.EntityCollection;
import org.apache.olingo.commons.api.data.Link;
import org.apache.olingo.commons.api.data.Property;
import org.apache.olingo.commons.api.data.ValueType;
import org.apache.olingo.commons.api.edm.EdmEntityType;
import org.apache.olingo.commons.api.edm.EdmPrimitiveType;
import org.apache.olingo.commons.api.edm.EdmPrimitiveTypeException;
import org.apache.olingo.commons.core.edm.primitivetype.EdmBinary;
import org.apache.olingo.commons.core.edm.primitivetype.EdmDate;
import org.apache.olingo.commons.core.edm.primitivetype.EdmDateTimeOffset;
import org.apache.olingo.commons.core.edm.primitivetype.EdmStream;
import org.apache.olingo.commons.core.edm.primitivetype.EdmTimeOfDay;
import org.apache.olingo.commons.core.edm.primitivetype.SingletonPrimitiveType;
import org.apache.olingo.server.core.responses.EntityResponse;
import org.teiid.core.TeiidProcessingException;
import org.teiid.core.types.BlobType;
import org.teiid.core.types.ClobType;
import org.teiid.core.types.DataTypeManager;
import org.teiid.core.types.Transform;
import org.teiid.core.types.TransformationException;
import org.teiid.core.util.ObjectConverterUtil;
import org.teiid.logging.LogConstants;
import org.teiid.logging.LogManager;
import org.teiid.odata.api.QueryResponse;
import org.teiid.olingo.ODataPlugin;
import org.teiid.olingo.ProjectedColumn;
import org.teiid.olingo.common.ODataTypeManager;
import org.teiid.query.sql.symbol.Symbol;

public class EntityCollectionResponse extends EntityCollection implements QueryResponse {
    private String nextToken;
    private Entity currentEntity;
    private DocumentNode documentNode;
    private String baseURL;
    private Map<String, Object> streams;

    private EntityCollectionResponse() {
    }
    
    public EntityCollectionResponse(String baseURL, DocumentNode resource) {
        this.baseURL = baseURL;
        this.documentNode = resource;
    }
    
    @Override
    public void addRow(ResultSet rs, boolean sameEntity) throws SQLException {
        Entity entity = null;
        boolean add = true;
        
        if (this.currentEntity == null) {
            entity = createEntity(rs, this.documentNode, this.baseURL, this);
            this.currentEntity = entity;
        } else {
            if(sameEntity) {
                entity = this.currentEntity;
                add = false;
            } else {
                entity = createEntity(rs, this.documentNode, this.baseURL, this);
                this.currentEntity = entity;            
            }
        }
        
        if (this.documentNode.getExpands() != null && !this.documentNode.getExpands().isEmpty()) {
            // right now it can do only one expand, when more than one this logic
            // need to be re-done.
            for (DocumentNode resource : this.documentNode.getExpands()) {
                ExpandDocumentNode expandNode = (ExpandDocumentNode)resource;
                Entity expandEntity = createEntity(rs, expandNode, this.baseURL, this);
                
                // make sure the expanded entity has valid key, otherwise it is just nulls on right side
                boolean valid = (expandEntity != null);
                if (!valid) {
                    continue;
                }
                                               
                Link link = entity.getNavigationLink(expandNode.getNavigationName());
                if (expandNode.isCollection()) {
                    if (link.getInlineEntitySet() == null) {
                        link.setInlineEntitySet(new EntityCollectionResponse());
                    }
                    EntityCollectionResponse expandResponse = (EntityCollectionResponse)link.getInlineEntitySet();
                    boolean addEntity = expandResponse.processOptions(
                            expandNode.getSkip(), expandNode.getTop(),
                            expandEntity);
                    if (addEntity) {
                        link.getInlineEntitySet().getEntities().add(expandEntity);
                    }
                }
                else {
                    link.setInlineEntity(expandEntity);
                }
            }
        }
        
        if (add) {
            getEntities().add(entity);
        } else {
            // this is property update incase of collection return 
            updateEntity(rs, entity, this.documentNode);            
        }
    }
    
    @Override
    public boolean isSameEntity(ResultSet rs) throws SQLException {
        if (this.currentEntity == null) {
            this.currentEntity = createEntity(rs, this.documentNode,this.baseURL, this);
            return false;
        } else {
            if (isSameRow(rs, this.documentNode, this.currentEntity)) {
                return true;
            } else {
                this.currentEntity = createEntity(rs, this.documentNode,this.baseURL, this);
                return false;
            }
        }
    }    
    
    @Override
    public void advanceRow(ResultSet rs, boolean sameEntity) throws SQLException {
        if (this.currentEntity == null) {
            this.currentEntity = createEntity(rs, this.documentNode, this.baseURL, this);
        } else {
            if (!sameEntity) {
                this.currentEntity = createEntity(rs, this.documentNode, this.baseURL, this);
            }
        }
    }
    
    private boolean isSameRow(ResultSet rs,  DocumentNode node, Entity other) throws SQLException {
        List<ProjectedColumn> projected = node.getAllProjectedColumns();
        EdmEntityType entityType = node.getEdmEntityType();
        
        for (String name:entityType.getKeyPredicateNames()) {
            ProjectedColumn pc = getProjectedColumn(name, projected);
            if (!(rs.getObject(pc.getOrdinal()).equals(other.getProperty(name).getValue()))) {
                return false;
            }
        }
        return true;
    }
    
    private ProjectedColumn getProjectedColumn(String name, List<ProjectedColumn> projected){
        for (ProjectedColumn pc:projected) {
            String propertyName = Symbol.getShortName(pc.getExpression());
            if (name.equals(propertyName)) {
                return pc;
            }
        }
        return null;
    }

    static Entity createEntity(ResultSet rs, DocumentNode node, String baseURL, EntityCollectionResponse response)
            throws SQLException {
        
        List<ProjectedColumn> projected = node.getAllProjectedColumns();
        EdmEntityType entityType = node.getEdmEntityType();
        
        LinkedHashMap<String, Link> streamProperties = new LinkedHashMap<String, Link>();
        
        Entity entity = new Entity();
        entity.setType(entityType.getFullQualifiedName().getFullQualifiedNameAsString());
        boolean allNulls = true;
        for (ProjectedColumn column: projected) {

            /*
            if (!column.isVisible()) {
                continue;
            }*/
            
            String propertyName = Symbol.getShortName(column.getExpression());
            Object value = rs.getObject(column.getOrdinal());
            if (value != null) {
                allNulls = false;
            }
            try {
                SingletonPrimitiveType type = (SingletonPrimitiveType) column.getEdmType();
                if (type instanceof EdmStream) {
                    buildStreamLink(streamProperties, value, propertyName);
                    if (response != null) {
                    	//this will only be used for a stream response off of the first entity. In all other scenarios it will be ignored.
                    	response.setStream(propertyName, value);
                    }
                }
                else {
                    Property property = buildPropery(propertyName, type, column.getPrecision(), column.getScale(),
                            column.isCollection(), value);
                    entity.addProperty(property);
                }
            } catch (IOException e) {
                throw new SQLException(e);
            } catch (TeiidProcessingException e) {
                throw new SQLException(e);
            }
        }
        
        if (allNulls) {
            return null;
        }
            
        // Build the navigation and Stream Links
        try {
            String id = EntityResponse.buildLocation(baseURL, entity, entityType.getName(), entityType);
            entity.setId(new URI(id));
            
            // build stream properties
            for (String name:streamProperties.keySet()) {
                Link link = streamProperties.get(name);
                link.setHref(id+"/"+name);
                entity.getMediaEditLinks().add(link);
                entity.addProperty(createPrimitive(name, EdmStream.getInstance(), new URI(link.getHref())));
            }
            
            // build navigations
            for (String name:entityType.getNavigationPropertyNames()) {
                Link navLink = new Link();
                navLink.setTitle(name);
                navLink.setHref(id+"/"+name);
                navLink.setRel("http://docs.oasis-open.org/odata/ns/related/"+name);
                entity.getNavigationLinks().add(navLink);
                
                Link assosiationLink = new Link();
                assosiationLink.setTitle(name);
                assosiationLink.setHref(id+"/"+name+"/$ref");
                assosiationLink.setRel("http://docs.oasis-open.org/odata/ns/relatedlinks/"+name);
                entity.getAssociationLinks().add(assosiationLink);
            }
        } catch (URISyntaxException e) {
            throw new SQLException(e);
        } catch (EdmPrimitiveTypeException e) {
            throw new SQLException(e);
        }            
        
        return entity;
    }    
    
    void setStream(String propertyName, Object value) {
    	if (this.streams == null) {
    		this.streams = new HashMap<String, Object>();
    	}
    	streams.put(propertyName, value);
	}
    
    public Object getStream(String propertyName) {
    	if (this.streams == null) {
    		return null;
    	}
    	return streams.get(propertyName);
    }

	private static void updateEntity(ResultSet rs, Entity entity, DocumentNode node)
            throws SQLException {
        
        List<ProjectedColumn> projected = node.getAllProjectedColumns();
        EdmEntityType entityType = node.getEdmEntityType();
        
        for (ProjectedColumn column: projected) {

            String propertyName = Symbol.getShortName(column.getExpression());
            if (entityType.getKeyPredicateNames().contains(propertyName)) {
                // no reason to update the identity keys
                continue;
            }
            
            Object value = rs.getObject(column.getOrdinal());
            
            try {
                SingletonPrimitiveType type = (SingletonPrimitiveType) column.getEdmType();
                if (column.isCollection()) {
                    Property previousProperty = entity.getProperty(propertyName);
                    Property property = buildPropery(propertyName, type, column.getPrecision(), column.getScale(),
                            column.isCollection(), value);
                    
                    property = mergeProperties(propertyName, type, previousProperty, property);
                    entity.getProperties().remove(previousProperty);
                    entity.addProperty(property);
                }                
            } catch (IOException e) {
                throw new SQLException(e);
            } catch (TeiidProcessingException e) {
                throw new SQLException(e);
            }
        }
    }    

    private static Property mergeProperties(String propName,
            SingletonPrimitiveType type, Property one, Property two) {
        ArrayList<Object> values = new ArrayList<Object>();
        values.addAll(one.asCollection());
        values.addAll(two.asCollection());
        return createCollection(propName, type, values);
    }

    private static void buildStreamLink(LinkedHashMap<String, Link> streamProperties,
            Object value, String propName) {
        if (value != null) {
            // read link 
            Link streamLink = new Link();
            streamLink.setTitle(propName);
            if (value instanceof SQLXML) {
                streamLink.setType("application/xml");
            }
            else if (value instanceof Clob) {
                streamLink.setType("application/json");
            }
            else if (value instanceof Blob) {
                streamLink.setType("application/octet-stream");
            }
            streamLink.setRel("http://docs.oasis-open.org/odata/ns/mediaresource/"+propName);
            streamProperties.put(propName, streamLink);            
        }
    }

    static Property buildPropery(String propName,
            SingletonPrimitiveType type, Integer precision, Integer scale, boolean isArray, Object value) throws TeiidProcessingException,
            SQLException, IOException {

        if (value instanceof Array) {
            value = ((Array) value).getArray();

            int length = java.lang.reflect.Array.getLength(value);
            ArrayList<Object> values = new ArrayList<Object>();
            for (int i = 0; i < length; i++) {
                Object o = java.lang.reflect.Array.get(value, i);
                if (o != null && o.getClass().isArray()) {
                    throw new TeiidNotImplementedException(ODataPlugin.Event.TEIID16029, 
                            ODataPlugin.Util.gs(ODataPlugin.Event.TEIID16029, propName));
                }
                Object p = getPropertyValue(type, precision, scale, isArray,  o);
                values.add(p);
            }
            return createCollection(propName, type, values);
        }
        if (isArray) {
            ArrayList<Object> values = new ArrayList<Object>();
            values.add(getPropertyValue(type, precision, scale, isArray, value));
            return createCollection(propName, type, values);
        }
        return createPrimitive(propName, type, getPropertyValue(type, precision, scale, isArray, value));
    }

    private static Property createPrimitive(final String name,
            EdmPrimitiveType type, final Object value) {
        return new Property(type.getFullQualifiedName().getFullQualifiedNameAsString(), name, ValueType.PRIMITIVE,
                value);
    }

    private static Property createCollection(final String name,
            EdmPrimitiveType type, final ArrayList<Object> values) {
        return new Property(type.getFullQualifiedName().getFullQualifiedNameAsString(), name,
                ValueType.COLLECTION_PRIMITIVE, values);
    }

/*    static Object getPropertyValue(SingletonPrimitiveType expectedType, boolean isArray,
            Object value, String invalidCharacterReplacement)
            throws TransformationException, SQLException, IOException {
        if (value == null) {
            return null;
        }
                
        Class<?> sourceType = DataTypeManager.getRuntimeType(value.getClass());
        Class<?> targetType = DataTypeManager.getDataTypeClass(ODataTypeManager.teiidType(expectedType, isArray));
        if (sourceType != targetType) {
            Transform t = DataTypeManager.getTransform(sourceType, targetType);
            if (t == null && BlobType.class == targetType) {
                if (sourceType == ClobType.class) {
                    return ClobType.getString((Clob) value).getBytes();
                }
                if (sourceType == SQLXML.class) {
                    return ((SQLXML) value).getString().getBytes();
                }
            }
            value = t != null ? t.transform(value, targetType) : value;
            value = replaceInvalidCharacters(expectedType, value, invalidCharacterReplacement);
            return value;
        }
        value = replaceInvalidCharacters(expectedType, value, invalidCharacterReplacement);
        return value;
    }
*/

    static Object getPropertyValue(SingletonPrimitiveType expectedType, Integer precision, Integer scale, boolean isArray,
            Object value)
            throws TransformationException, SQLException, IOException {
    	if (value == null) {
    		return null;
    	}
    	value = getPropertyValueInternal(expectedType, isArray, value);
    	if (value instanceof BigDecimal) {
    		BigDecimal bigDecimalValue = (BigDecimal)value;
    		
    		//if precision is set, then try to set an appropriate scale to pass the facet check
    		if (precision != null) {
	    		final int digits = bigDecimalValue.scale() >= 0
	    		          ? Math.max(bigDecimalValue.precision(), bigDecimalValue.scale())
	    		              : bigDecimalValue.precision() - bigDecimalValue.scale();
	    		
	            if (bigDecimalValue.scale() > (scale == null ? 0 : scale) || (digits > precision)) {
	            	bigDecimalValue = bigDecimalValue.setScale(Math.min(digits > precision ? bigDecimalValue.scale() - digits + precision : bigDecimalValue.scale(), scale == null ? 0 : scale), RoundingMode.HALF_UP);
	            }
    		}

    		value = bigDecimalValue;
    	}
    	return value;
    }
    
    private static Object getPropertyValueInternal(SingletonPrimitiveType expectedType, boolean isArray,
            Object value)
            throws TransformationException, SQLException, IOException {
        Class<?> sourceType = DataTypeManager.getRuntimeType(value.getClass());
        if (sourceType.isAssignableFrom(expectedType.getDefaultType())) {
            return value;
        }
        
        if (expectedType instanceof EdmDate && sourceType == Date.class) {
            return value;
        } else if (expectedType instanceof EdmDateTimeOffset && sourceType == Timestamp.class){
            return value;
        } else if (expectedType instanceof EdmTimeOfDay && sourceType == Time.class){
            return value;
        } else if (expectedType instanceof EdmBinary) {
            // there could be memory implications here, should have been modeled as EdmStream
            LogManager.logDetail(LogConstants.CTX_ODATA, "Possible OOM when inlining the stream based values"); //$NON-NLS-1$
            if (sourceType == ClobType.class) {
                return ClobType.getString((Clob) value).getBytes();
            }
            if (sourceType == SQLXML.class) {
                return ((SQLXML) value).getString().getBytes();
            }
            if (sourceType == BlobType.class) {
                return ObjectConverterUtil.convertToByteArray(((Blob)value).getBinaryStream());
            }
            if (value instanceof Serializable) {
                ByteArrayOutputStream bos = new ByteArrayOutputStream();
                ObjectOutputStream oos = new ObjectOutputStream(bos);
                oos.writeObject(value);
                oos.close();
                bos.close();
                return bos.toByteArray(); 
            }
        }

        Class<?> targetType = DataTypeManager.getDataTypeClass(ODataTypeManager.teiidType(expectedType, isArray));
        if (sourceType != targetType) {
            Transform t = DataTypeManager.getTransform(sourceType, targetType);
            value = t != null ? t.transform(value, targetType) : value;
        }
        return value;
    }    

    @Override
    public long size() {
        return getEntities().size();
    }

    @Override
    public void setCount(long count) {
        this.collectionCount = (int)count;
    }

    @Override
    public void setNextToken(String token) {
        this.nextToken = token;
    }
    
    @Override
    public String getNextToken() {
        return this.nextToken;
    }
    
    private int skipped = 0;
    private int topCount = 0;
    private int collectionCount = 0;
    
    private boolean processOptions(int skip, int top, Entity expandEntity) {
        this.collectionCount++;
        
        if (skip > 0 && this.skipped < skip) {
            this.skipped++;
            return false;
        }
        
        if (top > 0 ) {
            if (this.topCount < top) {
                this.topCount++;                
                return true;
            } else {
                return false;
            }
        }
        return true;
    }

    @Override
    public Integer getCount() {
        return this.collectionCount;
    }    
}
