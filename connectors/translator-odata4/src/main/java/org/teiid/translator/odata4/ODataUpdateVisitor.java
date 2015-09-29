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

import java.util.ArrayList;
import java.util.List;

import org.apache.olingo.commons.api.data.Entity;
import org.apache.olingo.commons.api.data.Property;
import org.apache.olingo.commons.api.data.ValueType;
import org.teiid.language.Condition;
import org.teiid.language.Delete;
import org.teiid.language.Expression;
import org.teiid.language.ExpressionValueSource;
import org.teiid.language.Insert;
import org.teiid.language.Literal;
import org.teiid.language.NamedTable;
import org.teiid.language.Update;
import org.teiid.language.visitor.HierarchyVisitor;
import org.teiid.metadata.Column;
import org.teiid.metadata.RuntimeMetadata;
import org.teiid.metadata.Table;
import org.teiid.translator.TranslatorException;
import org.teiid.translator.odata4.ODataMetadataProcessor.ODataType;

public class ODataUpdateVisitor extends HierarchyVisitor {
    protected enum OperationType {INSERT, UPDATE, DELETE}; 
	protected ArrayList<TranslatorException> exceptions = new ArrayList<TranslatorException>();
	private Condition condition;
	private ODataUpdateQuery odataQuery;
	private Table updatedTable;
	private RuntimeMetadata metadata;
	private OperationType operationType;

    public ODataUpdateVisitor(ODataExecutionFactory ef, RuntimeMetadata metadata) {
	    this.odataQuery = new ODataUpdateQuery(ef, metadata);
	    this.metadata = metadata;
	}
	
    public OperationType getOperationType() {
        return this.operationType;
    }
    
    public ODataUpdateQuery getODataQuery() {
        return this.odataQuery;
    }

    @Override
    public void visit(Insert obj) {
        try {
            this.operationType = OperationType.INSERT;
    		visitNode(obj.getTable());
    		Table table = obj.getTable().getMetadataObject();
    		this.updatedTable = getParentTable(table);
    		if (isComplexType(table) || isNavigationType(table)) {
    		    this.odataQuery.addTable(this.updatedTable);

    		} 
    		// read the properties
            int elementCount = obj.getColumns().size();
            for (int i = 0; i < elementCount; i++) {
                Column column = obj.getColumns().get(i).getMetadataObject();
            	List<Expression> values = ((ExpressionValueSource)obj.getValueSource()).getValues();
            	String type = ODataTypeManager.odataType(column.getRuntimeType())
                        .getFullQualifiedName().getFullQualifiedNameAsString();
            	Object value = ((Literal)values.get(i)).getValue();
            	this.odataQuery.addPayloadProperty(this.updatedTable, column, type, value);
            }
        } catch (TranslatorException e) {
            this.exceptions.add(e);
        } 
	}	
	
    @Override
    public void visit(Update obj) {
        try {
            this.operationType = OperationType.UPDATE;
    		visitNode(obj.getTable());
    		this.condition = obj.getWhere();
    		this.updatedTable = getParentTable(obj.getTable().getMetadataObject());
		
            Entity entity = new Entity();
            int elementCount = obj.getChanges().size();
            for (int i = 0; i < elementCount; i++) {
            	Column column = obj.getChanges().get(i).getSymbol().getMetadataObject();
            	Literal value = (Literal)obj.getChanges().get(i).getValue();
                String type = ODataTypeManager.odataType(column.getRuntimeType())
                        .getFullQualifiedName().getFullQualifiedNameAsString();
                entity.addProperty(new Property(type, 
                        column.getName(), 
                        ValueType.PRIMITIVE, 
                        ODataTypeManager.convertToODataInput(value, type)));
            }
        } catch (TranslatorException e) {
            this.exceptions.add(e);
        }
	}

	@Override
    public void visit(Delete obj) {
		try {
		    this.operationType = OperationType.DELETE;
            visitNode(obj.getTable());
            this.condition = obj.getWhere();
            this.updatedTable = getParentTable(obj.getTable().getMetadataObject());
        } catch (TranslatorException e) {
            this.exceptions.add(e);
        }
	}

    @Override
    public void visit(NamedTable obj) {
        this.odataQuery.addTable(obj.getMetadataObject());
    }
    
    private Table getParentTable(Table table) throws TranslatorException {
        if (isComplexType(table) || isNavigationType(table)) {
            String parentTable = table.getProperty(ODataMetadataProcessor.MERGE, false);
            return metadata.getTable(parentTable);
        }
        return table;
    }
    
    private boolean isComplexType(Table table) {
        ODataType type = ODataType.valueOf(table.getProperty(ODataMetadataProcessor.ODATA_TYPE, false));
        return type == ODataType.COMPLEX || type == ODataType.COMPLEX_COLLECTION;
    }
    
    private boolean isNavigationType(Table table) {
        ODataType type = ODataType.valueOf(table.getProperty(ODataMetadataProcessor.ODATA_TYPE, false));
        return type == ODataType.NAVIGATION || type == ODataType.NAVIGATION_COLLECTION;
    }
}
