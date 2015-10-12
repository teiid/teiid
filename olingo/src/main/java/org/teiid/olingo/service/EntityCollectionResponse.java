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
import java.util.LinkedHashMap;
import java.util.List;

import org.apache.olingo.commons.api.data.Entity;
import org.apache.olingo.commons.api.data.EntityCollection;
import org.apache.olingo.commons.api.data.Link;
import org.apache.olingo.commons.api.data.Property;
import org.apache.olingo.commons.api.data.ValueType;
import org.apache.olingo.commons.api.edm.EdmEntityType;
import org.apache.olingo.commons.api.edm.EdmPrimitiveType;
import org.apache.olingo.commons.core.Encoder;
import org.apache.olingo.commons.core.edm.primitivetype.EdmBinary;
import org.apache.olingo.commons.core.edm.primitivetype.EdmDate;
import org.apache.olingo.commons.core.edm.primitivetype.EdmDateTimeOffset;
import org.apache.olingo.commons.core.edm.primitivetype.EdmStream;
import org.apache.olingo.commons.core.edm.primitivetype.EdmString;
import org.apache.olingo.commons.core.edm.primitivetype.EdmTimeOfDay;
import org.apache.olingo.commons.core.edm.primitivetype.SingletonPrimitiveType;
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
import org.teiid.olingo.ODataTypeManager;
import org.teiid.olingo.ProjectedColumn;
import org.teiid.query.sql.symbol.Symbol;

public class EntityCollectionResponse extends EntityCollection implements QueryResponse {
    private final String invalidCharacterReplacement;
    private String nextToken;
    private Entity currentEntity;
    private DocumentNode documentNode;

    public EntityCollectionResponse(String invalidCharacterReplacement,
            DocumentNode resource) {
        this.invalidCharacterReplacement = invalidCharacterReplacement;
        this.documentNode = resource;
    }
    
    @Override
    public void addRow(ResultSet rs) throws SQLException {
        Entity entity = null;
        boolean add = true;
        
        if (this.currentEntity == null) {
            entity = createEntity(rs, this.documentNode, this.invalidCharacterReplacement);
            this.currentEntity = entity;
        } else {
            if(isSameRow(rs, this.documentNode, this.currentEntity)) {
                entity = this.currentEntity;
                add = false;
            } else {
                entity = createEntity(rs, this.documentNode, this.invalidCharacterReplacement);
                this.currentEntity = entity;            
            }
        }
        
        if (this.documentNode.getExpands() != null && !this.documentNode.getExpands().isEmpty()) {
            // right now it can do only one expand, when more than one this logic
            // need to be re-done.
            for (DocumentNode resource : this.documentNode.getExpands()) {
                ExpandDocumentNode expandNode = (ExpandDocumentNode)resource;
                Entity expandEntity = createEntity(rs, expandNode, this.invalidCharacterReplacement);
                
                Link link = entity.getNavigationLink(expandNode.getNavigationName());
                if (expandNode.isCollection()) {
                    if (link.getInlineEntitySet() == null) {
                        link.setInlineEntitySet(new EntityCollection());
                    }
                    link.getInlineEntitySet().getEntities().add(expandEntity);
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
            updateEntity(rs, entity, this.documentNode, this.invalidCharacterReplacement);            
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

    static Entity createEntity(ResultSet rs, DocumentNode node, String invalidChar)
            throws SQLException {
        
        List<ProjectedColumn> projected = node.getAllProjectedColumns();
        EdmEntityType entityType = node.getEdmEntityType();
        
        LinkedHashMap<String, Link> streamProperties = new LinkedHashMap<String, Link>();
        
        Entity entity = new Entity();
        entity.setType(entityType.getFullQualifiedName().getFullQualifiedNameAsString());
        
        for (ProjectedColumn column: projected) {

            /*
            if (!column.isVisible()) {
                continue;
            }*/
            
            String propertyName = Symbol.getShortName(column.getExpression());
            Object value = rs.getObject(column.getOrdinal());
            
            try {
                SingletonPrimitiveType type = (SingletonPrimitiveType) column.getEdmType();
                if (type instanceof EdmStream) {
                    buildStreamLink(streamProperties, value, propertyName);
                }
                else {
                    Property property = buildPropery(propertyName, type,
                            column.isCollection(), value,
                            invalidChar);
                    entity.addProperty(property);
                }
            } catch (IOException e) {
                throw new SQLException(e);
            } catch (TransformationException e) {
                throw new SQLException(e);
            }
        }
            
        // Build the navigation and Stream Links
        try {
            String id = buildId(entity, entityType);
            entity.setId(new URI(id));
            
            // build stream properties
            for (String name:streamProperties.keySet()) {
                Link link = streamProperties.get(name);
                link.setHref(id+"/"+name);
                entity.getMediaEditLinks().add(link);
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
        }            
        
        return entity;
    }    
    
    private static void updateEntity(ResultSet rs, Entity entity, DocumentNode node, String invalidChar)
            throws SQLException {
        
        List<ProjectedColumn> projected = node.getAllProjectedColumns();
        EdmEntityType entityType = node.getEdmEntityType();
        
        for (ProjectedColumn column: projected) {

            String propertyName = Symbol.getShortName(column.getExpression());
            if (entityType.getKeyPredicateNames().contains(propertyName)) {
                // no reason to update the identity keys
                continue;
            }
            
            Object value = rs.getObject(propertyName);
            
            try {
                SingletonPrimitiveType type = (SingletonPrimitiveType) column.getEdmType();
                if (column.isCollection()) {
                    Property previousProperty = entity.getProperty(propertyName);
                    Property property = buildPropery(propertyName, type,
                            column.isCollection(), value,
                            invalidChar);
                    
                    property = mergeProperties(propertyName, type, previousProperty, property);
                    entity.getProperties().remove(previousProperty);
                    entity.addProperty(property);
                }                
            } catch (IOException e) {
                throw new SQLException(e);
            } catch (TransformationException e) {
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
            SingletonPrimitiveType type, boolean isArray, Object value,
            String invalidCharacterReplacement) throws TransformationException,
            SQLException, IOException {

        if (value instanceof Array) {
            value = ((Array) value).getArray();

            int length = java.lang.reflect.Array.getLength(value);
            ArrayList<Object> values = new ArrayList<Object>();
            for (int i = 0; i < length; i++) {
                Object o = java.lang.reflect.Array.get(value, i);
                if (o != null && o.getClass().isArray()) {
                    throw new TransformationException(ODataPlugin.Event.TEIID16029, ODataPlugin.Util.gs(ODataPlugin.Event.TEIID16029, propName));
                }
                Object p = getPropertyValue(type, isArray,  o, invalidCharacterReplacement);
                values.add(p);
            }
            return createCollection(propName, type, values);
        }
        if (isArray) {
            ArrayList<Object> values = new ArrayList<Object>();
            values.add(getPropertyValue(type, isArray, value, invalidCharacterReplacement));
            return createCollection(propName, type, values);
        }
        return createPrimitive(propName, type, getPropertyValue(type, isArray, value, invalidCharacterReplacement));
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
    
    static Object getPropertyValue(SingletonPrimitiveType expectedType, boolean isArray,
            Object value, String invalidCharacterReplacement)
            throws TransformationException, SQLException, IOException {
        if (value == null) {
            return null;
        }
                
        Class<?> sourceType = DataTypeManager.getRuntimeType(value.getClass());
        if (sourceType.isAssignableFrom(expectedType.getDefaultType())) {
            return replaceInvalidCharacters(expectedType, value, invalidCharacterReplacement);
        }
        
        if (expectedType instanceof EdmDate && sourceType == Date.class) {
            return value;
        } else if (expectedType instanceof EdmDateTimeOffset && sourceType == Timestamp.class){
            return value;
        } else if (expectedType instanceof EdmTimeOfDay && sourceType == Time.class){
            return value;
        } else if (expectedType instanceof EdmBinary) {
            // there could be memory implications here, should have been modeled as EdmStream
            LogManager.logDetail(LogConstants.CTX_ODATA, "Possible OOM when inlining the stream based values");
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
        value = replaceInvalidCharacters(expectedType, value, invalidCharacterReplacement);
        return value;
    }    
    static Object replaceInvalidCharacters(EdmPrimitiveType expectedType,
            Object value, String invalidCharacterReplacement) {
        if (!(expectedType instanceof EdmString)  || invalidCharacterReplacement == null) {
            return value;
        }
        if (value instanceof Character) {
            value = value.toString();
        }
        String s = (String) value;
        StringBuilder result = null;
        for (int i = 0; i < s.length(); i++) {
            char c = s.charAt(i);
            if (c <= 0x0020 && c != ' ' && c != '\n' && c != '\t' && c != '\r') {
                if (result == null) {
                    result = new StringBuilder();
                    result.append(s.substring(0, i));
                }
                result.append(invalidCharacterReplacement);
            } else if (result != null) {
                result.append(c);
            }
        }
        if (result == null) {
            return value;
        }
        return result.toString();
    }

    @Override
    public long size() {
        return getEntities().size();
    }

    @Override
    public void setCount(long count) {
        super.setCount((int) count);
    }

    @Override
    public void setNextToken(String token) {
        this.nextToken = token;
    }
    
    @Override
    public String getNextToken() {
        return this.nextToken;
    }
    
    public static String buildId(Entity entity, EdmEntityType type) {
        String location = type.getName() + "(";
        int i = 0;
        boolean usename = type.getKeyPredicateNames().size() > 1;

        for (String key : type.getKeyPredicateNames()) {
          if (i > 0) {
            location += ",";
          }
          i++;
          if (usename) {
            location += (key + "=");
          }
          Property p = entity.getProperty(key);
          if (p.getType().equals("Edm.String")) {
            location = location + "'" + Encoder.encode(p.getValue().toString()) + "'";
          } else {
            location = location + p.getValue().toString();
          }
        }
        location += ")";
        return location;
    }
}
