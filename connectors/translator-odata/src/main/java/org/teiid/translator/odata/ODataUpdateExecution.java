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

import javax.ws.rs.core.Response.Status;

import org.odata4j.edm.EdmDataServices;
import org.odata4j.format.FormatType;
import org.teiid.language.Command;
import org.teiid.metadata.RuntimeMetadata;
import org.teiid.translator.*;
import org.teiid.translator.ws.BinaryWSProcedureExecution;

public class ODataUpdateExecution extends BaseQueryExecution implements UpdateExecution {
	private ODataUpdateVisitor visitor;
	private ODataEntitiesResponse response;
	
	public ODataUpdateExecution(Command command, ODataExecutionFactory translator,
			ExecutionContext executionContext, RuntimeMetadata metadata,
			WSConnection connection, EdmDataServices edsMetadata) throws TranslatorException {
		super(translator, executionContext, metadata, connection, edsMetadata);
		
		this.visitor = new ODataUpdateVisitor(translator, metadata, edsMetadata);
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
			BinaryWSProcedureExecution execution = executeDirect(this.visitor.getMethod(), this.visitor.buildURL(), null, FormatType.ATOM.getAcceptableMediaTypes());
			if (execution.getResponseCode() != Status.OK.getStatusCode() || (execution.getResponseCode() != Status.NO_CONTENT.getStatusCode())) {
				throw buildError(execution);
			}
		}
		else if(this.visitor.getMethod().equals("PATCH")) { //$NON-NLS-1$
			this.response = executeWithReturnEntity(this.visitor.getMethod(), this.visitor.buildURL(), this.visitor.getPayload(), this.visitor.getEntityName(), Status.OK, Status.NO_CONTENT);
			if (this.response != null) {
				if (this.response.hasError()) {
					throw this.response.getError();
				}
				
				if (!this.response.hasRow()) {
					this.response = null;	
				}
			}
		}
		else if (this.visitor.getMethod().equals("POST")) { //$NON-NLS-1$
			this.response = executeWithReturnEntity(this.visitor.getMethod(), this.visitor.buildURL(), this.visitor.getPayload(), this.visitor.getEntityName(), Status.CREATED);
			if (this.response != null) {
				if (this.response.hasError()) {
					throw this.response.getError();
				}
				
				if (!this.response.hasRow()) {
					this.response = null;	
				}
			}
		}
	}

	@Override
	public int[] getUpdateCounts() throws DataNotAvailableException,
			TranslatorException {
		if (this.response != null || this.visitor.getMethod().equals("DELETE")) { //$NON-NLS-1$
			return new int[] {1};
		}
		return new int[] {0};
	}
}
