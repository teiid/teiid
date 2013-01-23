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
package org.teiid.translator.odata;

import java.util.List;

import org.odata4j.edm.EdmDataServices;
import org.teiid.language.Call;
import org.teiid.metadata.Column;
import org.teiid.metadata.RuntimeMetadata;
import org.teiid.metadata.Table;
import org.teiid.translator.*;

public class ODataProcedureExecution extends BaseQueryExecution implements ProcedureExecution {
	private ODataProcedureVisitor visitor;
	private Class<?>[] expectedColumnTypes;
	private String[] columnNames;
	private String[] embeddedColumnNames;	
	
	public ODataProcedureExecution(Call command, ODataExecutionFactory translator,  ExecutionContext executionContext, RuntimeMetadata metadata, WSConnection connection, EdmDataServices edsMetadata) throws TranslatorException {
		super(translator, executionContext, metadata, connection, edsMetadata);
		
		this.visitor = new ODataProcedureVisitor(translator, metadata);
		this.visitor.visitNode(command);
		
		if (this.visitor.hasCollectionReturn()) {
			Table table = this.metadata.getTable(this.visitor.getTableName());
			int colCount = table.getColumns().size();
			this.expectedColumnTypes = new Class[colCount];
			this.columnNames = new String[colCount];
			this.embeddedColumnNames = new String[colCount];	
			
			for (int i = 0; i < colCount; i++) {
				Column column = table.getColumns().get(i);
	    		this.columnNames[i] = column.getName();
	    		this.embeddedColumnNames[i] = null;
	    		this.expectedColumnTypes[i] = column.getJavaType();
	    		String entityType = column.getProperty(ODataMetadataProcessor.ENTITY_TYPE, false);
	    		if (entityType != null) {
	    			String parentEntityType = column.getParent().getProperty(ODataMetadataProcessor.ENTITY_TYPE, false);
	    			if (!parentEntityType.equals(entityType)) {
	    				// this is embedded column
	    				this.columnNames[i] = entityType;
	    				this.embeddedColumnNames[i] = column.getNameInSource();
	    			}
	    		}				
			}
		}
	}

	@Override
	public List<?> getOutputParameterValues() throws TranslatorException {
		return null;
	}

	@Override
	public void execute() throws TranslatorException {
		String URI = this.visitor.buildURL();
		if (this.visitor.hasCollectionReturn()) {
			execute(this.visitor.getMethod(), URI, this.visitor.getEntityName());
		}
		else {
			//TODO:
			throw new TranslatorException();
		}
	}

	@Override
	public List<?> next() throws TranslatorException, DataNotAvailableException {
		// Feed based response
		if (this.visitor.hasCollectionReturn() && this.response != null ) {
			return this.response.getNextRow(this.columnNames, this.embeddedColumnNames, this.expectedColumnTypes);
		}
		return null;
	}

	@Override
	public void close() {
	}

	@Override
	public void cancel() throws TranslatorException {
	}
}
