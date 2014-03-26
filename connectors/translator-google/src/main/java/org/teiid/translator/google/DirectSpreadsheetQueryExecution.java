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
package org.teiid.translator.google;

import java.util.Collections;
import java.util.Iterator;
import java.util.List;

import org.teiid.core.util.StringUtil;
import org.teiid.language.Argument;
import org.teiid.language.Literal;
import org.teiid.language.visitor.SQLStringVisitor;
import org.teiid.logging.LogConstants;
import org.teiid.logging.LogManager;
import org.teiid.resource.adapter.google.GoogleSpreadsheetConnection;
import org.teiid.resource.adapter.google.common.SheetRow;
import org.teiid.translator.DataNotAvailableException;
import org.teiid.translator.ExecutionContext;
import org.teiid.translator.ProcedureExecution;
import org.teiid.translator.TranslatorException;

public class DirectSpreadsheetQueryExecution implements ProcedureExecution {
	private static final String WORKSHEET = "worksheet"; //$NON-NLS-1$
	private static final String QUERY = "query"; //$NON-NLS-1$
	private static final String OFFEST = "offset"; //$NON-NLS-1$
	private static final String LIMIT = "limit"; //$NON-NLS-1$
	
	private GoogleSpreadsheetConnection connection;
	private Iterator<SheetRow> rowIterator;
	private ExecutionContext executionContext;
	private List<Argument> arguments;
	
	private String query;
	private boolean returnsArray;

	public DirectSpreadsheetQueryExecution(String query, List<Argument> arguments, ExecutionContext executionContext, GoogleSpreadsheetConnection connection, boolean returnsArray) {
		this.executionContext = executionContext;
		this.connection = connection;
		this.arguments = arguments;
		this.query = query;
		this.returnsArray = returnsArray;
	}

	@Override
	public void close() {
		LogManager.logDetail(LogConstants.CTX_CONNECTOR, SpreadsheetExecutionFactory.UTIL.getString("close_query")); //$NON-NLS-1$
	}

	@Override
	public void cancel() throws TranslatorException {
		LogManager.logDetail(LogConstants.CTX_CONNECTOR, SpreadsheetExecutionFactory.UTIL.getString("cancel_query")); //$NON-NLS-1$
		this.rowIterator = null;
	}

	@Override
	public void execute() throws TranslatorException {
		String worksheet = null;
		Integer limit = null;
		Integer offset = null;
		String toQuery = query;
		
		List<String> parts = StringUtil.tokenize(query, ';');
		for (String var : parts) {
			int index = var.indexOf('=');
			if (index == -1) {
				continue;
			}
			String key = var.substring(0, index).trim();
			String value = var.substring(index+1).trim();
			
			if (key.equalsIgnoreCase(WORKSHEET)) {
				worksheet = value;
			}
			else if (key.equalsIgnoreCase(QUERY)) {
				StringBuilder buffer = new StringBuilder();
				SQLStringVisitor.parseNativeQueryParts(value, arguments, buffer, new SQLStringVisitor.Substitutor() {
					
					@Override
					public void substitute(Argument arg, StringBuilder builder, int index) {
						Literal argumentValue = arg.getArgumentValue();
						SpreadsheetSQLVisitor visitor = new SpreadsheetSQLVisitor();
						visitor.visit(argumentValue);
						builder.append(visitor.getTranslatedSQL());
					}
				});
				toQuery = buffer.toString();
			}
			else if (key.equalsIgnoreCase(LIMIT)) {
				limit = Integer.parseInt(value);
			}
			else if (key.equalsIgnoreCase(OFFEST)) {
				offset = Integer.parseInt(value);
			}
		}
		
		this.rowIterator = this.connection.executeQuery(worksheet, toQuery, offset, limit, executionContext.getBatchSize()).iterator();
	}

	@Override
	public List<?> next() throws TranslatorException, DataNotAvailableException {	
		if (this.rowIterator != null && this.rowIterator.hasNext()) {
			List<?> result = rowIterator.next().getRow();
			if (returnsArray) {
				return Collections.singletonList((Object)result.toArray());
			}
			return result;
		}
		this.rowIterator = null;
		return null;
	}

	@Override
	public List<?> getOutputParameterValues() throws TranslatorException {
		return null;
	}
}
