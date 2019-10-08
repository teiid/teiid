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

import java.net.URI;
import java.net.URISyntaxException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import org.apache.olingo.commons.api.edm.EdmEntityType;
import org.apache.olingo.commons.api.edm.EdmNavigationProperty;
import org.apache.olingo.commons.api.edm.EdmProperty;
import org.apache.olingo.commons.api.edm.EdmStructuredType;
import org.apache.olingo.server.api.OData;
import org.apache.olingo.server.api.ServiceMetadata;
import org.apache.olingo.server.api.uri.UriInfo;
import org.apache.olingo.server.api.uri.UriParameter;
import org.apache.olingo.server.api.uri.UriResourceEntitySet;
import org.apache.olingo.server.api.uri.UriResourceNavigation;
import org.apache.olingo.server.core.RequestURLHierarchyVisitor;
import org.apache.olingo.server.core.uri.parser.UriParserException;
import org.apache.olingo.server.core.uri.validator.UriValidationException;
import org.teiid.core.TeiidException;
import org.teiid.metadata.Column;
import org.teiid.metadata.ForeignKey;
import org.teiid.metadata.MetadataStore;
import org.teiid.metadata.Table;
import org.teiid.odata.api.SQLParameter;
import org.teiid.olingo.common.ODataTypeManager;
import org.teiid.query.sql.lang.Criteria;
import org.teiid.query.sql.lang.Update;
import org.teiid.query.sql.symbol.Constant;
import org.teiid.query.sql.symbol.ElementSymbol;
import org.teiid.query.sql.symbol.GroupSymbol;
import org.teiid.query.sql.symbol.Reference;

public class ReferenceUpdateSQLBuilder  extends RequestURLHierarchyVisitor {
    private final MetadataStore metadata;
    private String baseURI;
    private ServiceMetadata serviceMetadata;
    private OData odata;

    private ScopedTable updateTable;
    private ScopedTable referenceTable;
    private boolean collection;
    private final ArrayList<SQLParameter> params = new ArrayList<SQLParameter>();

    static class ScopedTable extends DocumentNode {
        private ForeignKey fk;

        public ScopedTable (Table table, EdmEntityType type, List<UriParameter> keys) {
            setTable(table);
            setEdmStructuredType(type);
            setGroupSymbol(new GroupSymbol(table.getFullName()));
            setKeyPredicates(keys);
        }
        public ForeignKey getFk() {
            return fk;
        }
        public void setFk(ForeignKey fk) {
            this.fk = fk;
        }
    }

    public ReferenceUpdateSQLBuilder(MetadataStore metadata, String baseURI,
            ServiceMetadata serviceMetadata, OData odata) {
        this.metadata = metadata;
        this.baseURI = baseURI;
        this.serviceMetadata = serviceMetadata;
        this.odata = odata;
    }

    @Override
    public void visit(UriResourceEntitySet info) {
        Table table = DocumentNode.findTable(info.getEntitySet(), this.metadata);
        EdmEntityType type = info.getEntitySet().getEntityType();
        List<UriParameter> keys = info.getKeyPredicates();
        this.updateTable = new ScopedTable(table, type, keys);
    }

    @Override
    public void visit(UriResourceNavigation info) {
        EdmNavigationProperty property = info.getProperty();

        this.referenceTable = new ScopedTable(DocumentNode.findTable(property.getType(),
                this.metadata), property.getType(),
                info.getKeyPredicates());

        if (property.isCollection()) {
            ForeignKey fk = DocumentNode.joinFK(referenceTable, this.updateTable, property);
            referenceTable.setFk(fk);

            ScopedTable temp = this.updateTable;
            this.updateTable = referenceTable;
            this.referenceTable = temp;
            this.collection = true;
        }
        else {
            ForeignKey fk = DocumentNode.joinFK(this.updateTable, referenceTable, property);
            this.updateTable.setFk(fk);
        }
    }


    public Update updateReference(URI referenceId, boolean prepared, boolean delete) throws SQLException {
        try {
            if (referenceId != null) {
                UriInfo uriInfo = ODataSQLBuilder.buildUriInfo(referenceId, this.baseURI, this.serviceMetadata, this.odata);
                UriResourceEntitySet uriEnitytSet = (UriResourceEntitySet)uriInfo.asUriInfoResource().getUriResourceParts().get(0);
                if (this.collection) {
                    this.updateTable.setKeyPredicates(uriEnitytSet.getKeyPredicates());
                }
                else {
                    this.referenceTable.setKeyPredicates(uriEnitytSet.getKeyPredicates());
                }
            }
        } catch (UriParserException e) {
            throw new SQLException(e);
        } catch (URISyntaxException e) {
            throw new SQLException(e);
        } catch (UriValidationException e) {
            throw new SQLException(e);
        }

        try {
            Update update = new Update();
            update.setGroup(this.updateTable.getGroupSymbol());

            List<String> columnNames = DocumentNode.getColumnNames(this.updateTable.getFk().getColumns());
            for (int i = 0; i < columnNames.size(); i++) {
                Column column = this.updateTable.getFk().getColumns().get(i);
                String columnName = columnNames.get(i);
                ElementSymbol symbol = new ElementSymbol(columnName, this.updateTable.getGroupSymbol());

                EdmStructuredType entityType = this.updateTable.getEdmStructuredType();
                EdmProperty edmProperty = entityType.getStructuralProperty(columnName);

                // reference table keys will be null for delete scenario
                Object value = null;
                if (!delete) {
                    UriParameter parameter = getParameter(this.updateTable.getFk().getReferenceColumns().get(i),
                            this.referenceTable.getKeyPredicates());
                    value = ODataTypeManager.parseLiteral(edmProperty, column.getJavaType(), parameter.getText());
                }

                if (prepared) {
                    update.addChange(symbol, new Reference(i++));
                    this.params.add(ODataSQLBuilder.asParam(edmProperty, value));
                }
                else {
                    update.addChange(symbol, new Constant(ODataSQLBuilder.asParam(edmProperty, value).getValue()));
                }
            }

            Criteria criteria = DocumentNode.buildEntityKeyCriteria(
                    this.updateTable, null, this.metadata, this.odata, null, null);
            update.setCriteria(criteria);

            return update;
        } catch (TeiidException e) {
            throw new SQLException(e);
        }
    }

    private UriParameter getParameter(String name, List<UriParameter> keys) {
        for (UriParameter parameter:keys) {
            String propertyName = parameter.getName();
            if (propertyName.equals(name)) {
                return parameter;
            }
        }
        return null;
    }

    public List<SQLParameter> getParameters(){
        return this.params;
    }
}
