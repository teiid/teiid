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

import org.teiid.language.Command;
import org.teiid.metadata.RuntimeMetadata;
import org.teiid.resource.adapter.google.GoogleSpreadsheetConnection;
import org.teiid.resource.adapter.google.common.SpreadsheetOperationException;
import org.teiid.resource.adapter.google.common.UpdateResult;
import org.teiid.resource.adapter.google.metadata.SpreadsheetInfo;
import org.teiid.resource.adapter.google.metadata.Worksheet;
import org.teiid.translator.DataNotAvailableException;
import org.teiid.translator.ExecutionContext;
import org.teiid.translator.TranslatorException;
import org.teiid.translator.UpdateExecution;

public abstract class AbstractSpreadsheetExecution implements UpdateExecution {
	protected GoogleSpreadsheetConnection connection;
	protected RuntimeMetadata metadata;
	protected ExecutionContext context;
	protected Command command;
	protected UpdateResult result;

	public AbstractSpreadsheetExecution(Command command, GoogleSpreadsheetConnection connection, ExecutionContext context,RuntimeMetadata metadata) {
		super();
		this.connection = connection;
		this.metadata = metadata;
		this.context = context;
		this.command = command;
	}

	@Override
	public void close() {
	}

	@Override
	public void cancel() throws TranslatorException {
	}

	@Override
	public int[] getUpdateCounts() throws DataNotAvailableException, TranslatorException {
		if (result.getExpectedNumberOfRows() != result.getActualNumberOfRows()) {
			if (result.getExpectedNumberOfRows() > result.getActualNumberOfRows()) {
				context.addWarning(new SpreadsheetOperationException(SpreadsheetExecutionFactory.UTIL.gs("partial_update", result.getExpectedNumberOfRows(), result.getActualNumberOfRows()))); //$NON-NLS-1$
			} else { 
				throw new SpreadsheetOperationException(SpreadsheetExecutionFactory.UTIL.gs("unexpected_updatecount", result.getExpectedNumberOfRows(), result.getActualNumberOfRows())); //$NON-NLS-1$
			}
		}
		return new int[]{result.getActualNumberOfRows()};
	}
    
	 void checkHeaders(String worksheetTitle) throws TranslatorException{
		SpreadsheetInfo info=connection.getSpreadsheetInfo();
		Worksheet worksheet=info.getWorksheetByName(worksheetTitle);
		if(worksheet==null){
			throw new SpreadsheetOperationException(SpreadsheetExecutionFactory.UTIL.gs("missing_worksheet", worksheetTitle)); //$NON-NLS-1$
		}
		if(!worksheet.isHeaderEnabled()){
			throw new TranslatorException(SpreadsheetExecutionFactory.UTIL.gs("headers_required")); //$NON-NLS-1$
		}
	}
	
	public GoogleSpreadsheetConnection getConnection(){
		return connection;
	}
}
