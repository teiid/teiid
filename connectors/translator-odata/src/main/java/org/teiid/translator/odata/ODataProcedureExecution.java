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

import org.odata4j.core.ODataVersion;
import org.odata4j.core.OObject;
import org.odata4j.core.OSimpleObject;
import org.odata4j.edm.EdmDataServices;
import org.odata4j.format.FormatParser;
import org.odata4j.format.FormatParserFactory;
import org.odata4j.format.FormatType;
import org.odata4j.format.Settings;
import org.teiid.language.Call;
import org.teiid.metadata.RuntimeMetadata;
import org.teiid.metadata.Schema;
import org.teiid.translator.DataNotAvailableException;
import org.teiid.translator.ExecutionContext;
import org.teiid.translator.ProcedureExecution;
import org.teiid.translator.TranslatorException;
import org.teiid.translator.WSConnection;
import org.teiid.translator.ws.BinaryWSProcedureExecution;

public class ODataProcedureExecution extends BaseQueryExecution implements ProcedureExecution {
	private ODataProcedureVisitor visitor;
	private Object returnValue;
	private ODataEntitiesResponse response;
	private Class<?>[] expectedColumnTypes;

	public ODataProcedureExecution(Call command, ODataExecutionFactory translator,  ExecutionContext executionContext, RuntimeMetadata metadata, WSConnection connection) throws TranslatorException {
		super(translator, executionContext, metadata, connection);

		this.visitor = new ODataProcedureVisitor(translator, metadata);
		this.visitor.visitNode(command);
		
		if (!this.visitor.exceptions.isEmpty()) {
			throw this.visitor.exceptions.get(0);
		}
		
		this.expectedColumnTypes = command.getResultSetColumnTypes();
	}

	@Override
	public void execute() throws TranslatorException {
		String URI = this.visitor.buildURL();
		Schema schema = visitor.getProcedure().getParent();
		EdmDataServices edm = new TeiidEdmMetadata(schema.getName(), ODataEntitySchemaBuilder.buildMetadata( schema));
		if (this.visitor.hasCollectionReturn()) {
			if (this.visitor.isReturnComplexType()) {
				// complex return
				this.response = executeWithComplexReturn(this.visitor.getMethod(), URI, null, this.visitor.getReturnEntityTypeName(), edm, null, Status.OK, Status.NO_CONTENT);
			}
			else {
				// entity type return
				this.response = executeWithReturnEntity(this.visitor.getMethod(), URI, null, this.visitor.getTable().getName(), edm, null, Status.OK, Status.NO_CONTENT);
			}
			if (this.response != null && this.response.hasError()) {
				throw this.response.getError();
			}
		}
		else {
			try {
				BinaryWSProcedureExecution execution = executeDirect(this.visitor.getMethod(), URI, null, getDefaultHeaders());
				if (execution.getResponseCode() != Status.OK.getStatusCode()) {
					throw buildError(execution);
				}

				Blob blob = (Blob)execution.getOutputParameterValues().get(0);
				ODataVersion version = getODataVersion(execution);

				// if the procedure is not void
				if (this.visitor.getReturnType() != null) {
					FormatParser<? extends OObject> parser = FormatParserFactory.getParser(OSimpleObject.class,
							FormatType.ATOM, new Settings(version, edm, this.visitor.getProcedure().getName(),
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
			return this.response.getNextRow(this.visitor.getReturnColumns(), this.expectedColumnTypes);
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
