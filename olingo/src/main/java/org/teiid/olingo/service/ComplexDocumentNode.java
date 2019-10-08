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

import java.util.ArrayList;
import java.util.List;

import org.apache.olingo.commons.api.edm.EdmComplexType;
import org.apache.olingo.commons.api.edm.EdmOperation;
import org.apache.olingo.commons.api.edm.EdmReturnType;
import org.apache.olingo.commons.api.edm.FullQualifiedName;
import org.apache.olingo.commons.core.edm.EdmPropertyImpl;
import org.teiid.metadata.Column;
import org.teiid.metadata.MetadataStore;
import org.teiid.metadata.Procedure;
import org.teiid.metadata.Schema;
import org.teiid.olingo.service.ProcedureSQLBuilder.ProcedureReturn;
import org.teiid.olingo.service.TeiidServiceHandler.UniqueNameGenerator;
import org.teiid.query.sql.lang.SPParameter;
import org.teiid.query.sql.lang.StoredProcedure;
import org.teiid.query.sql.lang.SubqueryFromClause;
import org.teiid.query.sql.symbol.ElementSymbol;
import org.teiid.query.sql.symbol.Expression;
import org.teiid.query.sql.symbol.GroupSymbol;
import org.teiid.query.sql.symbol.Reference;

public class ComplexDocumentNode extends DocumentNode {
    private ProcedureReturn procedureReturn;
    private Procedure procedure;

    public static ComplexDocumentNode buildComplexDocumentNode(
            EdmOperation edmOperation,
            MetadataStore metadata,
            UniqueNameGenerator nameGenerator) {

        ComplexDocumentNode resource = new ComplexDocumentNode();

        FullQualifiedName fqn = edmOperation.getFullQualifiedName();
        String withoutVDB = fqn.getNamespace().substring(fqn.getNamespace().lastIndexOf('.')+1);
        Schema schema = metadata.getSchema(withoutVDB);

        Procedure procedure =  schema.getProcedure(edmOperation.getName());

        StoredProcedure storedQuery = new StoredProcedure();
        storedQuery.setProcedureName(procedure.getFullName());
        for (int i = 0; i < procedure.getParameters().size(); i++) {
            storedQuery.setParameter(new SPParameter(i+1, new Reference(i)));
        }

        String group = nameGenerator.getNextGroup();
        SubqueryFromClause sfc = new SubqueryFromClause(group, storedQuery);

        resource.setGroupSymbol(new GroupSymbol(group));
        resource.setFromClause(sfc);
        resource.procedure = procedure;
        return resource;
    }

    @Override
    public List<String> getKeyColumnNames(){
        return new ArrayList<String>();
    }

    public void setProcedureReturn(ProcedureReturn pp) {
        this.procedureReturn = pp;
    }

    public ProcedureReturn getProcedureReturn() {
        return procedureReturn;
    }

    @Override
    protected void addAllColumns(boolean onlyPK) {
        for (final Column column : procedure.getResultSet().getColumns()) {
            if (column.isSelectable()) {
                EdmReturnType returnType = procedureReturn.getReturnType();
                EdmComplexType complexType = (EdmComplexType)returnType.getType();
                EdmPropertyImpl edmProperty = (EdmPropertyImpl)complexType.getProperty(column.getName());
                addProjectedColumn(new ElementSymbol(column.getName(), getGroupSymbol()), edmProperty.getType(), edmProperty, edmProperty.isCollection());
            }
        }
    }

    @Override
    protected void addProjectedColumn(final String columnName,
            final Expression expr) {
        EdmReturnType returnType = procedureReturn.getReturnType();
        EdmComplexType complexType = (EdmComplexType)returnType.getType();
        EdmPropertyImpl edmProperty = (EdmPropertyImpl)complexType.getProperty(columnName);
        addProjectedColumn(expr, edmProperty.getType(), edmProperty, edmProperty.isCollection());
    }


    public String getName() {
        return procedure.getName();
    }

    public ContextColumn getColumnByName(String name) {
        for (final Column column : procedure.getResultSet().getColumns()) {
            if (column.getName().equals(name)) {
                return new TableContextColumn(column);
            }
        }
        return null;
    }

    public String getFullName() {
        return procedure.getFullName();
    }
}
