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
package org.teiid.olingo;

import java.io.IOException;
import java.sql.Array;
import java.sql.Clob;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLXML;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;

import org.apache.olingo.commons.api.data.Entity;
import org.apache.olingo.commons.api.data.Property;
import org.apache.olingo.commons.api.data.ValueType;
import org.apache.olingo.commons.api.edm.EdmElement;
import org.apache.olingo.commons.api.edm.EdmEntitySet;
import org.apache.olingo.commons.api.edm.EdmEntityType;
import org.apache.olingo.commons.api.edm.EdmPrimitiveTypeKind;
import org.apache.olingo.commons.api.edm.EdmProperty;
import org.apache.olingo.commons.core.data.EntityImpl;
import org.apache.olingo.commons.core.data.EntitySetImpl;
import org.apache.olingo.commons.core.data.PropertyImpl;
import org.apache.olingo.server.core.edm.provider.EdmPropertyImpl;
import org.teiid.core.TeiidException;
import org.teiid.core.types.BlobType;
import org.teiid.core.types.ClobType;
import org.teiid.core.types.DataTypeManager;
import org.teiid.core.types.Transform;
import org.teiid.core.types.TransformationException;

public class EntityList extends EntitySetImpl implements QueryResponse {
    private final String invalidCharacterReplacement;
    private final HashMap<String, EdmElement> propertyTypes;
    private final Collection<ProjectedColumn> projectedColumns;

    public EntityList(String invalidCharacterReplacement, EdmEntitySet edmEntitySet,
            Collection<ProjectedColumn> projectedColumns) {
        this.invalidCharacterReplacement = invalidCharacterReplacement;
        this.propertyTypes = new HashMap<String, EdmElement>();
        this.projectedColumns = projectedColumns;

        EdmEntityType entityType = edmEntitySet.getEntityType();
        Iterator<String> propIter = entityType.getPropertyNames().iterator();
        while (propIter.hasNext()) {
            String prop = propIter.next();
            this.propertyTypes.put(prop, entityType.getProperty(prop));
        }
    }

    @Override
    public void addRow(ResultSet rs) throws SQLException, TeiidException {
        getEntities().add(getEntity(rs));
    }

    private Entity getEntity(ResultSet rs) throws SQLException, TeiidException {
        HashMap<String, Property> properties = new HashMap<String, Property>();
        for (int i = 0; i < rs.getMetaData().getColumnCount(); i++) {
            Object value = rs.getObject(i + 1);
            String propName = rs.getMetaData().getColumnLabel(i + 1);
            EdmElement element = this.propertyTypes.get(propName);
            if (!(element instanceof EdmProperty) && !((EdmProperty) element).isPrimitive()) {
                throw new TeiidException(ODataPlugin.Event.TEIID16024,
                        ODataPlugin.Util.gs(ODataPlugin.Event.TEIID16024));
            }
            EdmPropertyImpl edmProperty = (EdmPropertyImpl) element;
            Property property;
            try {
                property = buildPropery(propName, edmProperty.getTypeInfo()
.getPrimitiveTypeKind(), value,
                        invalidCharacterReplacement);
                properties.put(rs.getMetaData().getColumnLabel(i + 1), property);
            } catch (IOException e) {
                throw new TeiidException(e);
            }
        }

        // TODO: need to define key and navigation

        /*
         * OEntityKey key = OEntityKey.infer(entitySet, new
         * ArrayList<Property<?>>(properties.values()));
         *
         * ArrayList<Link> links = new ArrayList<Link>();
         *
         * for (EdmNavigationProperty
         * navProperty:entitySet.getType().getNavigationProperties()) {
         * links.add
         * (OLinks.relatedEntity(navProperty.getRelationship().getName(),
         * navProperty.getToRole().getRole(), key.toKeyString())); }
         */

        // properties can contain more than what is requested in project to
        // build links
        // filter those columns out.
        EntityImpl entity = new EntityImpl();
        for (ProjectedColumn entry : this.projectedColumns) {
            if (entry.isVisible()) {
                entity.addProperty(properties.get(entry.getName()));
            }
        }
        return entity;
    }

    static PropertyImpl buildPropery(String propName,
            EdmPrimitiveTypeKind type, Object value,
            String invalidCharacterReplacement) throws TransformationException,
            SQLException, IOException {

        if (value instanceof Array) {
            value = ((Array) value).getArray();

            int length = java.lang.reflect.Array.getLength(value);
            ArrayList values = new ArrayList();
            for (int i = 0; i < length; i++) {
                Object o = java.lang.reflect.Array.get(value, i);
                Object p = getPropertyValue(type, o, invalidCharacterReplacement);
                values.add(p);
            }
            return createCollection(propName, type, values);
        }
        return createPrimitive(propName, type, getPropertyValue(type, value, invalidCharacterReplacement));
    }

    private static PropertyImpl createPrimitive(final String name,
            EdmPrimitiveTypeKind type, final Object value) {
        return new PropertyImpl(type.getFullQualifiedName().getFullQualifiedNameAsString(), name, ValueType.PRIMITIVE,
                value);
    }

    private static PropertyImpl createCollection(final String name,
            EdmPrimitiveTypeKind type, final Object... values) {
        return new PropertyImpl(type.getFullQualifiedName().getFullQualifiedNameAsString(), name,
                ValueType.COLLECTION_PRIMITIVE, Arrays.asList(values));
    }

    static Object getPropertyValue(EdmPrimitiveTypeKind expectedType,
            Object value, String invalidCharacterReplacement)
            throws TransformationException, SQLException, IOException {
        if (value == null) {
            return null;
        }
        Class<?> sourceType = DataTypeManager.getRuntimeType(value.getClass());
        Class<?> targetType = DataTypeManager.getDataTypeClass(ODataTypeManager.teiidType(expectedType));
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

    static Object replaceInvalidCharacters(EdmPrimitiveTypeKind expectedType,
            Object value, String invalidCharacterReplacement) {
        if (expectedType != EdmPrimitiveTypeKind.String || invalidCharacterReplacement == null) {
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
    public void setNext(long row) {
        // TODO: set next URI..
    }
}
