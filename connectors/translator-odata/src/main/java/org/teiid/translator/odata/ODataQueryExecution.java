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

import java.io.IOException;
import java.sql.Blob;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.List;

import org.odata4j.edm.EdmDataServices;
import org.teiid.core.util.ObjectConverterUtil;
import org.teiid.language.QueryExpression;
import org.teiid.metadata.Column;
import org.teiid.metadata.RuntimeMetadata;
import org.teiid.translator.*;
import org.teiid.translator.ws.BinaryWSProcedureExecution;

public class ODataQueryExecution extends BaseQueryExecution implements ResultSetExecution {
    
	private ODataSQLVisitor visitor;
	private int countResponse = -1;
	private Class<?>[] expectedColumnTypes;
	private String[] columnNames;
	private String[] embeddedColumnNames;
	
	public ODataQueryExecution(ODataExecutionFactory translator,
			QueryExpression command, ExecutionContext executionContext,
			RuntimeMetadata metadata, WSConnection connection, EdmDataServices edsMetadata) throws TranslatorException {
		super(translator, executionContext, metadata, connection, edsMetadata);
		
		this.visitor = new ODataSQLVisitor(this.translator, metadata);
    	this.visitor.visitNode(command);
    	if (!this.visitor.exceptions.isEmpty()) {
    		throw visitor.exceptions.get(0);
    	}  
    	this.expectedColumnTypes = command.getColumnTypes();
    	int colCount = this.visitor.getSelect().length;
    	this.columnNames = new String[colCount];
    	this.embeddedColumnNames = new String[colCount];
    	for (int i = 0; i < colCount; i++) {
    		Column column = this.visitor.getSelect()[i];
    		this.columnNames[i] = column.getName();
    		this.embeddedColumnNames[i] = null;
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

	@Override
	public void execute() throws TranslatorException {
		String URI = this.visitor.buildURL();

		if (this.visitor.isCount()) {
			String[] headers = new String[] {"text/xml", "text/plain"}; //$NON-NLS-1$ //$NON-NLS-2$
			BinaryWSProcedureExecution execution = executeRequest("GET", URI, headers); //$NON-NLS-1$
			Blob blob = (Blob)execution.getOutputParameterValues().get(0);
			try {
				this.countResponse = Integer.parseInt(ObjectConverterUtil.convertToString(blob.getBinaryStream()));
			} catch (IOException e) {
				throw new TranslatorException(e);
			} catch (SQLException e) {
				throw new TranslatorException(e);
			}			
		}
		else {
			execute("GET", URI, visitor.getEnityTable().getName()); //$NON-NLS-1$
		}
	}
	
	@Override
	public List<?> next() throws TranslatorException, DataNotAvailableException {
		if (visitor.isCount() && this.countResponse != -1) {
			int count = this.countResponse;
			this.countResponse = -1;
			return Arrays.asList(count);
		}

		// Feed based response
		if (this.response != null ) {
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
