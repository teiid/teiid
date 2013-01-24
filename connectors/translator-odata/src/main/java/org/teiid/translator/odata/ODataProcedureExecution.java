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

import java.io.InputStreamReader;
import java.sql.Blob;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.List;

import javax.ws.rs.core.Response.Status;

import org.odata4j.core.ODataConstants;
import org.odata4j.core.ODataVersion;
import org.odata4j.core.OObject;
import org.odata4j.core.OSimpleObject;
import org.odata4j.edm.EdmDataServices;
import org.odata4j.format.FormatParser;
import org.odata4j.format.FormatParserFactory;
import org.odata4j.format.FormatType;
import org.odata4j.format.Settings;
import org.teiid.language.Call;
import org.teiid.metadata.Column;
import org.teiid.metadata.RuntimeMetadata;
import org.teiid.metadata.Table;
import org.teiid.translator.*;
import org.teiid.translator.ws.BinaryWSProcedureExecution;

public class ODataProcedureExecution extends BaseQueryExecution implements ProcedureExecution {
	private ODataProcedureVisitor visitor;
	private Class<?>[] expectedColumnTypes;
	private String[] columnNames;
	private String[] embeddedColumnNames;	
	private Object returnValue;
	private ODataEntitiesResponse response;
	
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
	public void execute() throws TranslatorException {
		String URI = this.visitor.buildURL();
		if (this.visitor.hasCollectionReturn()) {
			this.response = executeWithReturnEntity(this.visitor.getMethod(), URI, null, this.visitor.getEntityName(),Status.OK, Status.NO_CONTENT);
			if (this.response != null && this.response.hasError()) {
				throw this.response.getError();
			}
		}
		else {
			try {
				String[] headers = FormatType.ATOM.getAcceptableMediaTypes();
				
				BinaryWSProcedureExecution execution = executeDirect(this.visitor.getMethod(), URI, null, headers);
				if (execution.getResponseCode() != Status.OK.getStatusCode()) {
					throw buildError(execution);
				}
				
				Blob blob = (Blob)execution.getOutputParameterValues().get(0);
				ODataVersion version = getDataServiceVersion((String)execution.getResponseHeader(ODataConstants.Headers.DATA_SERVICE_VERSION));				

				// if the procedure is not void
				if (this.visitor.getReturnType() != null) {
					FormatParser<? extends OObject> parser = FormatParserFactory.getParser(OSimpleObject.class,
							FormatType.ATOM, new Settings(version, this.edsMetadata, this.visitor.getProcedureName(),
					            null, // entitykey
					            true, // isResponse
					            ODataTypeManager.odataType(this.visitor.getReturnType())));
					
					OSimpleObject object = (OSimpleObject)parser.parse(new InputStreamReader(blob.getBinaryStream()));
					this.returnValue = this.translator.retrieveValue(object.getValue(), this.visitor.getReturnTypeClass());
				}
			} catch (SQLException e) {
				throw new TranslatorException(e);
			}
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
	public List<?> getOutputParameterValues() throws TranslatorException {
		return Arrays.asList(this.returnValue);
	}	

	@Override
	public void close() {
	}

	@Override
	public void cancel() throws TranslatorException {
	}
}
