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
package org.teiid.translator.odata4;

import java.io.StringWriter;
import java.net.URI;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.apache.olingo.client.api.serialization.ODataSerializerException;
import org.apache.olingo.client.core.ConfigurationImpl;
import org.apache.olingo.client.core.serialization.JsonSerializer;
import org.apache.olingo.client.core.uri.URIBuilderImpl;
import org.apache.olingo.commons.api.data.ComplexValue;
import org.apache.olingo.commons.api.data.Entity;
import org.apache.olingo.commons.api.data.Property;
import org.apache.olingo.commons.api.data.ValueType;
import org.apache.olingo.commons.api.format.ContentType;
import org.teiid.metadata.Column;
import org.teiid.metadata.ForeignKey;
import org.teiid.metadata.RuntimeMetadata;
import org.teiid.metadata.Table;
import org.teiid.translator.TranslatorException;

public class ODataUpdateQuery extends ODataQuery {
    private Map<String, Object> keys = new LinkedHashMap<String, Object>();
    private Map<String, Object> expandKeys = new LinkedHashMap<String, Object>();
    private List<Property> payloadProperties = new ArrayList<Property>();
    private String method = "POST"; //$NON-NLS-1$
    
    public ODataUpdateQuery(ODataExecutionFactory executionFactory, RuntimeMetadata metadata) {
        super(executionFactory, metadata);
    }

    public String buildInsertURL(String serviceRoot) throws TranslatorException {
        handleMissingEntitySet();
        URIBuilderImpl uriBuilder = new URIBuilderImpl(new ConfigurationImpl(), serviceRoot);
        uriBuilder.appendEntitySetSegment(this.entitySetTable.getName());
        if (this.keys.size() == 1) {
            uriBuilder.appendKeySegment(this.keys.values().iterator().next());
        } else if (!this.keys.isEmpty()){
            uriBuilder.appendKeySegment(this.keys);
        }
        
        this.method = "POST";
        if (!this.complexTables.isEmpty()) {
            if (this.complexTables.get(0).isCollection()) {
                this.method = "PUT";
            } else {
                this.method = "PATCH";
            }
            uriBuilder.appendPropertySegment(this.complexTables.get(0).getName());
        }
        if (!this.expandTables.isEmpty()) {
            if (!this.expandTables.get(0).isCollection()) {
                this.method = "PUT";
            }
            uriBuilder.appendPropertySegment(this.expandTables.get(0).getName());
        }
//        
//        if (this.expandKeys.size() == 1) {
//            uriBuilder.appendKeySegment(this.expandKeys.values().iterator().next());
//        } else if (!this.expandKeys.isEmpty()){
//            uriBuilder.appendKeySegment(this.expandKeys);
//        }

        URI uri = uriBuilder.build();
        return uri.toString();
    }

    public String getPayload() throws TranslatorException {
        JsonSerializer serializer = new JsonSerializer(false, ContentType.APPLICATION_JSON);
        StringWriter writer = new StringWriter();
        
        try {
            if (!this.complexTables.isEmpty()) {
                Table table = this.complexTables.get(0).getTable();
                Property complexProperty = new Property();
                complexProperty.setName(getName(table));
                complexProperty.setType(table.getProperty(ODataMetadataProcessor.NAME_IN_SCHEMA, false));

                ComplexValue value = new ComplexValue();
                for (Property p:this.payloadProperties) {
                    value.getValue().add(p);
                }
                complexProperty.setValue(ValueType.COMPLEX, value);
                serializer.write(writer, complexProperty);                
            } else if (!this.expandTables.isEmpty()) {
                Table table = this.expandTables.get(0).getTable();
                Entity entity = new Entity();
                entity.setType(table.getProperty(
                        ODataMetadataProcessor.NAME_IN_SCHEMA, false));            
                for (Property p:this.payloadProperties) {
                    entity.addProperty(p);
                }
                serializer.write(writer, entity);
            } else {
                Entity entity = new Entity();
                entity.setType(this.entitySetTable.getTable().getProperty(
                        ODataMetadataProcessor.NAME_IN_SCHEMA, false));            
                for (Property p:this.payloadProperties) {
                    entity.addProperty(p);
                }
                serializer.write(writer, entity);
            }
        } catch (ODataSerializerException e) {
            throw new TranslatorException(e);
        }
        return writer.toString();
    }

    private void buildKeyColumns(Table parentTable, Table childTable,
            String columnName, Object value) {
        if(parentTable.equals(childTable)) {
            return;
        }

        for (ForeignKey fk:childTable.getForeignKeys()) {
            if (fk.getReferenceTableName().equals(parentTable.getName())) {
                List<Column> columns = fk.getColumns();
                for (int i = 0; i < columns.size(); i++) {
                    Column fkColumn = columns.get(i);
                    if (fkColumn.getName().equals(columnName)) {
                        this.keys.put(fk.getReferenceColumns().get(i), value);
                    }
                }
            }
        }
        
        if (!this.expandTables.isEmpty()) {
            if (childTable.getPrimaryKey() != null) {
                for (Column column:childTable.getPrimaryKey().getColumns()) {
                    if (column.isSelectable()) {
                        if (column.getName().equals(columnName)) {
                            this.expandKeys.put(columnName, value);
                        }
                    }
                }
            }
        }
    }
    
    private String getName(Table table) {
        if (table.getNameInSource() != null) {
            return table.getNameInSource();
        }
        return table.getName();
    }    
    
    public void addPayloadProperty(Table parentTable, Column column, String type, Object value) {
        buildKeyColumns(parentTable, (Table)column.getParent(), column.getName(), value);
        if (column.isSelectable()) {
            this.payloadProperties.add(new Property(type, column.getName(), ValueType.PRIMITIVE, value));
        }
    }

    public String getMethod() {
        return this.method;
    }   
}
