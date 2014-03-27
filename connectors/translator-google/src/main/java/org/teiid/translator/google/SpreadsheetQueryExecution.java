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

import java.util.Iterator;
import java.util.List;

import org.teiid.language.Select;
import org.teiid.logging.LogConstants;
import org.teiid.logging.LogManager;
import org.teiid.resource.adapter.google.GoogleSpreadsheetConnection;
import org.teiid.resource.adapter.google.common.SheetRow;
import org.teiid.translator.DataNotAvailableException;
import org.teiid.translator.ExecutionContext;
import org.teiid.translator.ResultSetExecution;
import org.teiid.translator.TranslatorException;

public class SpreadsheetQueryExecution implements ResultSetExecution {

	private Select query;
	private GoogleSpreadsheetConnection connection;
	private Iterator<SheetRow> rowIterator;
	private ExecutionContext executionContext;

	public SpreadsheetQueryExecution(Select query,
			GoogleSpreadsheetConnection connection, ExecutionContext executionContext) {
		this.executionContext = executionContext;
		this.connection = connection;
		this.query = query;
	}

	@Override
	public void close() {
		LogManager.logDetail(LogConstants.CTX_CONNECTOR, SpreadsheetExecutionFactory.UTIL.getString("close_query")); //$NON-NLS-1$
	}

	@Override
	public void cancel() throws TranslatorException {
		LogManager.logDetail(LogConstants.CTX_CONNECTOR, SpreadsheetExecutionFactory.UTIL.getString("cancel_query")); //$NON-NLS-1$

	}

	@Override
	public void execute() throws TranslatorException {
		SpreadsheetSQLVisitor visitor = new SpreadsheetSQLVisitor();
		visitor.translateSQL(query);		
		rowIterator = connection.executeQuery(visitor.getWorksheetTitle(), visitor.getTranslatedSQL(), visitor.getOffsetValue(),visitor.getLimitValue(), executionContext.getBatchSize()).iterator();
		
	}

	@Override
	public List<?> next() throws TranslatorException, DataNotAvailableException {		
		if (rowIterator.hasNext()) {
			return rowIterator.next().getRow();
		}
		return null;
	}

}
