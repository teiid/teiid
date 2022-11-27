/*
 * Copyright Red Hat, Inc. and/or its affiliates
 * and other contributors as indicated by the @author tags and
 * the COPYRIGHT.txt file distributed with this work.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
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
import org.apache.olingo.commons.api.edm.EdmStructuredType;
import org.apache.olingo.commons.core.edm.primitivetype.AbstractGeospatialType;
import org.apache.olingo.commons.core.edm.primitivetype.EdmBinary;
import org.apache.olingo.commons.core.edm.primitivetype.EdmDate;
import org.apache.olingo.commons.core.edm.primitivetype.EdmDateTimeOffset;
import org.apache.olingo.commons.core.edm.primitivetype.EdmStream;
import org.apache.olingo.commons.core.edm.primitivetype.EdmTimeOfDay;
import org.apache.olingo.commons.core.edm.primitivetype.SingletonPrimitiveType;
import org.apache.olingo.server.core.responses.EntityResponse;
import org.teiid.api.exception.query.FunctionExecutionException;
import org.teiid.core.TeiidProcessingException;
import org.teiid.core.TeiidRuntimeException;
import org.teiid.core.types.BinaryType;
import org.teiid.core.types.BlobType;
import org.teiid.core.types.ClobType;
import org.teiid.core.types.DataTypeManager;
import org.teiid.core.types.DataTypeManager.DefaultDataClasses;
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

    interface Row {
        Object getObject(int column) throws SQLException;
        Object[] getArray(int columnIndex) throws SQLException;
    }

    private String nextToken;
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
    public void addRow(ResultSet rs) throws SQLException {
        Entity entity = createEntity(rs, this.documentNode, this.baseURL, this);

        processExpands(asRow(rs), entity, this.documentNode);
        getEntities().add(entity);
    }

    private void processExpands(Row vals, Entity entity, DocumentNode node)
            throws SQLException {
        if (node.getExpands() == null || node.getExpands().isEmpty()) {
            return;
        }
        for (ExpandDocumentNode expandNode : node.getExpands()) {
            Object[] expandedVals = vals.getArray(expandNode.getColumnIndex());
            if (expandedVals == null) {
                continue;
            }
            for (Object o : expandedVals) {
                Object[] expandedVal = (Object[])o;
                Entity expandEntity = createEntity(expandedVal, expandNode, this.baseURL, this);

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

                processExpands(asRow(expandedVal), expandEntity, expandNode);
            }
        }
    }

    static Entity createEntity(final Object[] vals, DocumentNode node, String baseURL, EntityCollectionResponse response)
            throws SQLException {
        return createEntity(asRow(vals), node, baseURL, response);
    }

    static Entity createEntity(final ResultSet vals, DocumentNode node, String baseURL, EntityCollectionResponse response)
            throws SQLException {
        return createEntity(asRow(vals), node, baseURL, response);
    }

    static Entity createEntity(Row row, DocumentNode node, String baseURL, EntityCollectionResponse response)
            throws SQLException {

        List<ProjectedColumn> projected = node.getAllProjectedColumns();
        EdmStructuredType entityType = node.getEdmStructuredType();

        LinkedHashMap<String, Link> streamProperties = new LinkedHashMap<String, Link>();

        Entity entity = new Entity();
        entity.setType(entityType.getFullQualifiedName().getFullQualifiedNameAsString());
        boolean allNulls = true;
        for (ProjectedColumn column: projected) {

            String propertyName = Symbol.getShortName(column.getExpression());
            Object value = row.getObject(column.getOrdinal());
            if (value != null) {
                allNulls = false;
            }
            try {
                SingletonPrimitiveType type = column.getEdmType();
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
            } catch (IOException | TeiidProcessingException e) {
                throw new SQLException(e);
            }
        }

        if (allNulls) {
            return null;
        }

        // Build the navigation and Stream Links
        try {
            if (!(entityType instanceof EdmEntityType)) {
                if (!streamProperties.isEmpty()) {
                    //won't work without an id
                    throw new TeiidRuntimeException();
                }
                return entity;
            }
            String id = EntityResponse.buildLocation(baseURL, entity, entityType.getName(), (EdmEntityType)entityType);
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

    private static Row asRow(final ResultSet vals) {
        return new Row() {
            @Override
            public Object getObject(int column) throws SQLException {
                return vals.getObject(column);
            }
            @Override
            public Object[] getArray(int columnIndex) throws SQLException {
                Array array = vals.getArray(columnIndex);
                if (array == null) {
                    return null;
                }
                return (Object[]) array.getArray();
            }
        };
    }

    private static Row asRow(final Object[] vals) {
        return new Row() {
            @Override
            public Object getObject(int column) throws SQLException {
                return vals[column - 1];
            }
            @Override
            public Object[] getArray(int columnIndex) {
                return (Object[]) vals[columnIndex - 1];
            }
        };
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
        return new Property(type.getFullQualifiedName().getFullQualifiedNameAsString(), name,
                (type instanceof AbstractGeospatialType?ValueType.GEOSPATIAL:ValueType.PRIMITIVE),
                value);
    }

    private static Property createCollection(final String name,
            EdmPrimitiveType type, final ArrayList<Object> values) {
        return new Property(type.getFullQualifiedName().getFullQualifiedNameAsString(), name,
                (type instanceof AbstractGeospatialType?ValueType.COLLECTION_GEOSPATIAL:ValueType.COLLECTION_PRIMITIVE),
                values);
    }

    static Object getPropertyValue(SingletonPrimitiveType expectedType, Integer precision, Integer scale, boolean isArray,
            Object value)
            throws TransformationException, SQLException, IOException, FunctionExecutionException {
        if (value == null) {
            return null;
        }
        value = getPropertyValueInternal(expectedType, isArray, value);
        value = ODataTypeManager.rationalizePrecision(precision, scale, value);
        return value;
    }

    /**
     *
     * @param expectedType
     * @param isArray
     * @param value
     * @return
     * @see DocumentNode#addProjectedColumn(org.teiid.query.sql.symbol.Expression, org.apache.olingo.commons.api.edm.EdmType, org.apache.olingo.commons.api.edm.EdmProperty, boolean)
     * for the convention around geometry representation, currently there are none but if we
     * need the srid it needs to be associated with the value via ewkb or passed in here based upon the property metadata.
     */
    private static Object getPropertyValueInternal(SingletonPrimitiveType expectedType, boolean isArray,
            Object value)
            throws TransformationException, SQLException, IOException, FunctionExecutionException {
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
            if (sourceType == BinaryType.class) {
                if (value instanceof BinaryType) {
                    return ((BinaryType)value).getBytesDirect();
                }
                return value;
            }
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
        } else if (expectedType instanceof AbstractGeospatialType && sourceType == DefaultDataClasses.BLOB) {
            return ODataTypeManager.convertToODataValue(((Blob)value).getBinaryStream(), false);
        }

        Class<?> targetType = DataTypeManager.getDataTypeClass(ODataTypeManager.teiidType(expectedType, isArray));
        if (sourceType != targetType) {
            Transform t = DataTypeManager.getTransform(sourceType, targetType);
            value = DataTypeManager.convertToRuntimeType(value, true);
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

        if (top > -1 ) {
            if (this.topCount < top) {
                this.topCount++;
                return true;
            }
            return false;
        }
        return true;
    }

    @Override
    public Integer getCount() {
        return this.collectionCount;
    }
}
