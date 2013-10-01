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

import java.io.StringWriter;
import java.util.ArrayList;
import java.util.List;

import javax.ws.rs.core.Response.Status;

import org.odata4j.core.OEntities;
import org.odata4j.core.OEntity;
import org.odata4j.core.OProperty;
import org.odata4j.edm.EdmDataServices;
import org.odata4j.edm.EdmEntitySet;
import org.odata4j.format.Entry;
import org.odata4j.format.FormatWriter;
import org.odata4j.format.FormatWriterFactory;
import org.teiid.GeneratedKeys;
import org.teiid.language.Command;
import org.teiid.metadata.RuntimeMetadata;
import org.teiid.metadata.Schema;
import org.teiid.metadata.Table;
import org.teiid.translator.DataNotAvailableException;
import org.teiid.translator.ExecutionContext;
import org.teiid.translator.TranslatorException;
import org.teiid.translator.UpdateExecution;
import org.teiid.translator.WSConnection;
import org.teiid.translator.ws.BinaryWSProcedureExecution;

public class ODataUpdateExecution extends BaseQueryExecution implements UpdateExecution {
	private ODataUpdateVisitor visitor;
	private ODataEntitiesResponse response;
	
	public ODataUpdateExecution(Command command, ODataExecutionFactory translator,
			ExecutionContext executionContext, RuntimeMetadata metadata,
			WSConnection connection) throws TranslatorException {
		super(translator, executionContext, metadata, connection);
		
		this.visitor = new ODataUpdateVisitor(translator, metadata);
		this.visitor.visitNode(command);
		
		if (!this.visitor.exceptions.isEmpty()) {
			throw this.visitor.exceptions.get(0);
		}
	}

	@Override
	public void close() {
	}

	@Override
	public void cancel() throws TranslatorException {
	}

	@Override
	public void execute() throws TranslatorException {
		if (this.visitor.getMethod().equals("DELETE")) { //$NON-NLS-1$
			// DELETE
			BinaryWSProcedureExecution execution = executeDirect(this.visitor.getMethod(), this.visitor.buildURL(), null, getDefaultHeaders());
			if (execution.getResponseCode() != Status.OK.getStatusCode() && (execution.getResponseCode() != Status.NO_CONTENT.getStatusCode())) {
				throw buildError(execution);
			}
		}
		else if(this.visitor.getMethod().equals("PUT")) { //$NON-NLS-1$
			// UPDATE
			Schema schema = visitor.getTable().getParent();
			EdmDataServices edm = new TeiidEdmMetadata(schema.getName(), ODataEntitySchemaBuilder.buildMetadata( schema));
			BinaryWSProcedureExecution execution = executeDirect("GET", this.visitor.buildURL(), null, getDefaultHeaders()); //$NON-NLS-1$
			if (execution.getResponseCode() == Status.OK.getStatusCode()) {
				String etag = getHeader(execution, "ETag"); //$NON-NLS-1$
				String payload = buildPayload(this.visitor.getTable().getName(), this.visitor.getPayload(), edm);
				this.response = executeWithReturnEntity(this.visitor.getMethod(), this.visitor.buildURL(), payload, this.visitor.getTable().getName(), edm, etag, Status.OK, Status.NO_CONTENT);
				if (this.response != null) {
					if (this.response.hasError()) {
						throw this.response.getError();
					}
				}
			}
		}
		else if (this.visitor.getMethod().equals("POST")) { //$NON-NLS-1$
			// INSERT
			Schema schema = visitor.getTable().getParent();
			EdmDataServices edm = new TeiidEdmMetadata(schema.getName(), ODataEntitySchemaBuilder.buildMetadata( schema));
			String payload = buildPayload(this.visitor.getTable().getName(), this.visitor.getPayload(), edm);
			this.response = executeWithReturnEntity(this.visitor.getMethod(), this.visitor.buildURL(), payload, this.visitor.getTable().getName(), edm, null, Status.CREATED);
			if (this.response != null) {
				if (this.response.hasError()) {
					throw this.response.getError();
				}
			}
		}
	}

	private String buildPayload(String entitySet, final List<OProperty<?>> props, EdmDataServices edm) {
		final EdmEntitySet ees = edm.getEdmEntitySet(entitySet);
		
	    Entry entry =  new Entry() {
	        public String getUri() {
	          return null;
	        }
	        public OEntity getEntity() {
	          return OEntities.createRequest(ees, props, null);
	        }
	      };		
		
		StringWriter sw = new StringWriter();
		FormatWriter<Entry> fw = FormatWriterFactory.getFormatWriter(Entry.class, null, "ATOM", null); //$NON-NLS-1$
		fw.write(null, sw, entry);
		return sw.toString();
	}	
	
	@Override
	public int[] getUpdateCounts() throws DataNotAvailableException, TranslatorException {
		if (this.visitor.getMethod().equals("DELETE")) { //$NON-NLS-1$
			//DELETE
			return (this.response != null)?new int[]{1}:new int[]{0};
		}
		else if(this.visitor.getMethod().equals("PUT")) { //$NON-NLS-1$
			// UPDATE; 
			// conflicting implementation found where some sent 200 with content; other with 204 no-content 
			return (this.response != null)?new int[]{1}:new int[]{0};
		}
		else if (this.visitor.getMethod().equals("POST")) { //$NON-NLS-1$
			//INSERT
			if (this.response != null && this.response.hasRow()) {
	            if (this.executionContext.getCommandContext().isReturnAutoGeneratedKeys()) {
	            	addAutoGeneretedKeys();
	            }				
				return new int[]{1};
			}
		}
		return new int[] {0};
	}
	
	private void addAutoGeneretedKeys() {
		OEntity entity = this.response.getResultsIter().next().getEntity();
		Table table = this.visitor.getEnityTable();
		
		int cols = table.getPrimaryKey().getColumns().size();
		Class<?>[] columnDataTypes = new Class<?>[cols];
		String[] columnNames = new String[cols];
		//this is typically expected to be an int/long, but we'll be general here.  we may eventual need the type logic off of the metadata importer
        for (int i = 0; i < cols; i++) {
        	columnDataTypes[i] = table.getPrimaryKey().getColumns().get(i).getJavaType();
        	columnNames[i] = table.getPrimaryKey().getColumns().get(i).getName();
        }
        GeneratedKeys generatedKeys = this.executionContext.getCommandContext().returnGeneratedKeys(columnNames, columnDataTypes);
        List<Object> vals = new ArrayList<Object>(columnDataTypes.length);
        for (int i = 0; i < columnDataTypes.length; i++) {
        	OProperty<?> prop = entity.getProperty(columnNames[i]);
            Object value = this.translator.retrieveValue(prop.getValue(), columnDataTypes[i]);
            vals.add(value); 
        }
        generatedKeys.addKey(vals);
	}
}
