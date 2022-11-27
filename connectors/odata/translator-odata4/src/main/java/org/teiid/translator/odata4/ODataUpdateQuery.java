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
package org.teiid.translator.odata4;

import java.io.StringWriter;
import java.net.URI;
import java.util.ArrayList;
import java.util.Arrays;
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
import org.teiid.language.Condition;
import org.teiid.metadata.Column;
import org.teiid.metadata.ForeignKey;
import org.teiid.metadata.RuntimeMetadata;
import org.teiid.metadata.Table;
import org.teiid.translator.TranslatorException;
import org.teiid.translator.document.DocumentNode;

public class ODataUpdateQuery extends ODataQuery {
    private Map<String, Object> keys = new LinkedHashMap<String, Object>();
    private Map<String, Object> expandKeys = new LinkedHashMap<String, Object>();
    private List<Property> payloadProperties = new ArrayList<Property>();
    private Condition condition;

    public ODataUpdateQuery(ODataExecutionFactory executionFactory, RuntimeMetadata metadata) {
        super(executionFactory, metadata);
    }

    public String buildInsertURL(String serviceRoot) throws TranslatorException {
        URIBuilderImpl uriBuilder = new URIBuilderImpl(new ConfigurationImpl(), serviceRoot);
        uriBuilder.appendEntitySetSegment(this.rootDocument.getName());
        if (this.keys.size() == 1) {
            uriBuilder.appendKeySegment(this.keys.values().iterator().next());
        } else if (!this.keys.isEmpty()){
            uriBuilder.appendKeySegment(this.keys);
        }

        if (!this.complexTables.isEmpty()) {
            uriBuilder.appendPropertySegment(this.complexTables.get(0).getName());
        }
        if (!this.expandTables.isEmpty()) {
            uriBuilder.appendPropertySegment(this.expandTables.get(0).getName());
        }

        URI uri = uriBuilder.build();
        return uri.toString();
    }

    public String getInsertMethod() {
        String method = "POST";
        if (!this.complexTables.isEmpty()) {
            if (this.complexTables.get(0).isCollection()) {
                method = "PUT";
            } else {
                method = "PATCH";
            }
        }
        if (!this.expandTables.isEmpty()) {
            if (!this.expandTables.get(0).isCollection()) {
                method = "PUT";
            }
        }
        return method;
    }


    public String getPayload(Entity parentEntity) throws TranslatorException {
        JsonSerializer serializer = new JsonSerializer(false, ContentType.JSON_FULL_METADATA);
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
                if (this.complexTables.get(0).isCollection()) {
                    complexProperty.setValue(ValueType.COLLECTION_COMPLEX, Arrays.asList(value));
                } else {
                    complexProperty.setValue(ValueType.COMPLEX, value);
                }
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
                entity.setType(this.rootDocument.getTable().getProperty(
                        ODataMetadataProcessor.NAME_IN_SCHEMA, false));
                for (Property p:this.payloadProperties) {
                    entity.addProperty(p);
                }
                // for updates
                if (parentEntity != null) {
                    // add all the key properties.
                    List<Column> keys = this.rootDocument.getTable().getPrimaryKey().getColumns();
                    for (Column key: keys) {
                        entity.addProperty(parentEntity.getProperty(key.getName()));
                    }
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
                    if (!isPseudo(column)) {
                        if (column.getName().equals(columnName)) {
                            this.expandKeys.put(columnName, value);
                        }
                    }
                }
            }
        }
    }

    private boolean isPseudo(Column column) {
        String property = column.getProperty(ODataMetadataProcessor.PSEUDO, false);
        return property != null;
    }

    private String getName(Table table) {
        if (table.getNameInSource() != null) {
            return table.getNameInSource();
        }
        return table.getName();
    }

    public void addInsertProperty(Column column, String type, Object value) {
        buildKeyColumns(getRootDocument().getTable(), (Table)column.getParent(), column.getName(), value);
        addUpdateProperty(column, type, value);
    }

    public void addUpdateProperty(Column column, String type, Object value) {
        boolean collection = (value instanceof List<?>);
        if (!isPseudo(column)) {
            this.payloadProperties.add(new Property(type, column.getName(),
                    collection ? ValueType.COLLECTION_PRIMITIVE
                            : ValueType.PRIMITIVE, value));
        }
    }

    public void setCondition(Condition where) {
        this.condition = where;
    }

    public String buildUpdateSelectionURL(String serviceRoot) throws TranslatorException {
        URIBuilderImpl uriBuilder = new URIBuilderImpl(new ConfigurationImpl(), serviceRoot);
        uriBuilder.appendEntitySetSegment(this.rootDocument.getName());

        List<String> selection = this.rootDocument.getIdentityColumns();
        uriBuilder.select(selection.toArray(new String[selection.size()]));

        if (!this.expandTables.isEmpty()) {
            ODataDocumentNode use = this.expandTables.get(0);
            List<String> keys = use.getIdentityColumns();
            for (String key:keys) {
                use.appendSelect(key);
            }
            uriBuilder.expandWithOptions(use.getName(), use.getOptions());
        }

        String filter = processFilter(condition);
        if (filter != null) {
            uriBuilder.filter(filter);
        }
        URI uri = uriBuilder.build();
        return uri.toString();
    }

    public String getUpdateMethod() {
        String method = "PATCH";
        if (!this.complexTables.isEmpty()) {
            if (this.complexTables.get(0).isCollection()) {
                method = "PUT";
            } else {
                method = "PATCH";
            }
        }
        if (!this.expandTables.isEmpty()) {
            if (!this.expandTables.get(0).isCollection()) {
                method = "PUT";
            }
        }
        return method;
    }

    public String buildUpdateURL(String serviceRoot, List<?> row) {
        URIBuilderImpl uriBuilder = new URIBuilderImpl(new ConfigurationImpl(), serviceRoot);
        uriBuilder.appendEntitySetSegment(this.rootDocument.getName());

        List<String> selection = this.rootDocument.getIdentityColumns();
        if (selection.size() == 1) {
            uriBuilder.appendKeySegment(row.get(0));
        } else if (!selection.isEmpty()) {
            LinkedHashMap<String, Object> keys = new LinkedHashMap<String, Object>();
            for (int i = 0; i < selection.size(); i++) {
                keys.put(selection.get(i), row.get(i));
            }
            uriBuilder.appendKeySegment(keys);
        }

        if (!this.complexTables.isEmpty()) {
            uriBuilder.appendPropertySegment(this.complexTables.get(0).getName());
        }
        if (!this.expandTables.isEmpty()) {
            uriBuilder.appendPropertySegment(this.expandTables.get(0).getName());

            // add keys if present
            DocumentNode use = this.expandTables.get(0);
            List<String> expandSelection = use.getIdentityColumns();
            LinkedHashMap<String, Object> keys = new LinkedHashMap<String, Object>();
            for (int i = 0; i < expandSelection.size(); i++) {
                keys.put(expandSelection.get(i), row.get(selection.size()+i));
            }
            if (!keys.isEmpty()) {
                uriBuilder.appendKeySegment(keys);
            }
        }

        URI uri = uriBuilder.build();
        return uri.toString();
    }
}
