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

import java.util.ArrayList;
import java.util.List;

import org.apache.olingo.commons.api.edm.EdmComplexType;
import org.apache.olingo.commons.api.edm.EdmOperation;
import org.apache.olingo.commons.api.edm.EdmReturnType;
import org.apache.olingo.commons.api.edm.FullQualifiedName;
import org.apache.olingo.commons.core.edm.EdmPropertyImpl;
import org.apache.olingo.server.api.OData;
import org.apache.olingo.server.api.uri.UriInfo;
import org.teiid.core.TeiidProcessingException;
import org.teiid.metadata.Column;
import org.teiid.metadata.MetadataStore;
import org.teiid.metadata.Procedure;
import org.teiid.metadata.Schema;
import org.teiid.olingo.service.ODataSQLBuilder.URLParseService;
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
            MetadataStore metadata, OData odata,
            UniqueNameGenerator nameGenerator, boolean useAlias,
            UriInfo uriInfo, URLParseService parseService)
            throws TeiidProcessingException {
        
        ComplexDocumentNode resource = new ComplexDocumentNode();
        
        FullQualifiedName fqn = edmOperation.getFullQualifiedName();
        String withoutVDB = fqn.getNamespace().substring(fqn.getNamespace().lastIndexOf('.')+1);
        Schema schema = metadata.getSchema(withoutVDB);

        Procedure procedure =  schema.getProcedure(edmOperation.getName());
        
        StoredProcedure storedQuery = new StoredProcedure();
        storedQuery.setProcedureName(procedure.getFullName()); //$NON-NLS-1$
        for (int i = 0; i < procedure.getParameters().size(); i++) {
            storedQuery.setParameter(new SPParameter(i+1, new Reference(i)));
        }
        
        String group = nameGenerator.getNextGroup();
        SubqueryFromClause sfc = new SubqueryFromClause(group, storedQuery); //$NON-NLS-1$
                
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
    
    public Column getColumnByName(String name) {
        for (final Column column : procedure.getResultSet().getColumns()) {
            if (column.getName().equals(name)) {
                return column;
            }
        }
        return null;
    }

    public String getFullName() {
        return procedure.getFullName();
    }    
}
