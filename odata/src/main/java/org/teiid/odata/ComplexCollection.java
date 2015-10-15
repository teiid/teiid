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
import java.util.List;

import org.odata4j.core.OCollection;
import org.odata4j.core.OCollections;
import org.odata4j.core.OComplexObject;
import org.odata4j.core.OComplexObjects;
import org.odata4j.core.OProperty;
import org.odata4j.edm.EdmComplexType;
import org.odata4j.edm.EdmEntitySet;
import org.odata4j.edm.EdmProperty;
import org.odata4j.edm.EdmSimpleType;
import org.odata4j.edm.EdmType;
import org.odata4j.exceptions.NotAcceptableException;
import org.odata4j.producer.CollectionResponse;
import org.odata4j.producer.InlineCount;
import org.odata4j.producer.QueryInfo;
import org.teiid.core.types.TransformationException;
import org.teiid.odata.DocumentNode.ProjectedColumn;

public class ComplexCollection extends ArrayList<OComplexObject> implements
        CollectionResponse<OComplexObject>, EntityCollector<OComplexObject> {
    private static final long serialVersionUID = -337908504068541458L;
    private EdmComplexType type;
    private QueryInfo queryInfo;
    private int count = 0;
    private String skipToken;
    private DocumentNode documentNode;
    
    public ComplexCollection(EdmComplexType type,
            DocumentNode node, QueryInfo queryInfo) {
        this.type = type;
        this.queryInfo = queryInfo;
        this.documentNode = node;
    }
    
    private EdmType getType(String name) {
        for (EdmProperty property : this.type.getProperties()) {
            if (property.getName().equals(name)) {
                property.getType();
            }
        }
        return EdmSimpleType.STRING;
    }
    
    @Override
    public OComplexObject addRow(Object previous, ResultSet rs, String invalidCharacterReplacement)
            throws TransformationException, SQLException, IOException {
        List<OProperty<?>> row = new ArrayList<OProperty<?>>();
        if (this.documentNode.getProjectedColumns().isEmpty()) {
            for (int i = 0; i < rs.getMetaData().getColumnCount(); i++) {
                Object value = rs.getObject(i+1);
                String propName = rs.getMetaData().getColumnLabel(i+1);
                OProperty<?> property = LocalClient.buildPropery(propName,
                        getType(propName), value,
                        invalidCharacterReplacement);
                row.add(property);                                            
            }
            
        } else {
            for (ProjectedColumn pc : this.documentNode.getProjectedColumns().values()) {
                OProperty<?> property = LocalClient.buildPropery(pc.name(),
                        pc.type(), rs.getObject(pc.ordinal()),
                        invalidCharacterReplacement);
                row.add(property);                            
            }
        }
        OComplexObject erow = OComplexObjects.create(this.type, row);
        add(erow);
        return erow;
    }

    @Override
    public void lastRow(Object last) {
        // no-op as it added during the addRow.
    }

    @Override
    public boolean isSameRow(Object previous, Object current) {
        return false;
    }
    
    @Override
    public void setInlineCount(int count) {
       this.count = count;
    }

    @Override
    public void setSkipToken(String skipToken) {
        this.skipToken = skipToken;
    }

    @Override
    public OCollection<OComplexObject> getCollection() {
        OCollection.Builder<OComplexObject> resultRows = OCollections.newBuilder(this.type);
        for (OComplexObject oco:this) {
            resultRows.add(oco);
        }
        return resultRows.build();        
    }

    @Override
    public String getCollectionName() {
        String collectionName = this.type.getFullyQualifiedTypeName();
        collectionName = collectionName.replace("(", "_"); //$NON-NLS-1$ //$NON-NLS-2$
        collectionName = collectionName.replace(")", "_"); //$NON-NLS-1$ //$NON-NLS-2$
        return collectionName;
    }

    @Override
    public Integer getInlineCount() {
        if (queryInfo.inlineCount == InlineCount.ALLPAGES) {
            return count;
        }
        return null;
    }

    @Override
    public String getSkipToken() {
        return this.skipToken;
    }

    @Override
    public EdmEntitySet getEntitySet() {
        throw new NotAcceptableException("Only Complex types");
    }
}
