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
import org.teiid.language.Delete;
import org.teiid.language.Insert;
import org.teiid.language.Update;
import org.teiid.logging.LogConstants;
import org.teiid.logging.LogManager;
import org.teiid.metadata.RuntimeMetadata;
import org.teiid.resource.adapter.google.GoogleSpreadsheetConnection;
import org.teiid.resource.adapter.google.common.UpdateResult;
import org.teiid.resource.adapter.google.metadata.SpreadsheetInfo;
import org.teiid.translator.ExecutionContext;
import org.teiid.translator.TranslatorException;
import org.teiid.translator.google.visitor.SpreadsheetDeleteVisitor;
import org.teiid.translator.google.visitor.SpreadsheetInsertVisitor;
import org.teiid.translator.google.visitor.SpreadsheetUpdateVisitor;
/**
 * Execution of INSERT, DELETE and UPDATE commands
 * 
 * @author felias
 *
 */
public class SpreadsheetUpdateExecution extends AbstractSpreadsheetExecution {

	public SpreadsheetUpdateExecution(Command command, GoogleSpreadsheetConnection connection, ExecutionContext context, RuntimeMetadata metadata) {
		super(command, connection, context, metadata);
		this.command = command;
		this.connection = connection;
	}

	@Override
	public void cancel() throws TranslatorException {
		LogManager.logDetail(LogConstants.CTX_CONNECTOR, SpreadsheetExecutionFactory.UTIL.getString("cancel_query")); //$NON-NLS-1$
	}

	@Override
	public void execute() throws TranslatorException {

		if (command instanceof org.teiid.language.Delete) {
			result = executeDelete();
		} else if (command instanceof org.teiid.language.Insert) {
			result = executeInsert();
		} else if (command instanceof org.teiid.language.Update) {
			result = executeUpdate();
		}
	}

	private UpdateResult executeUpdate() throws TranslatorException {
		SpreadsheetInfo info = connection.getSpreadsheetInfo();
		SpreadsheetUpdateVisitor updateVisitor = new SpreadsheetUpdateVisitor(info);
		updateVisitor.visit((Update) command);
		checkHeaders(updateVisitor.getWorksheetTitle());
		result = connection.executeListFeedUpdate(updateVisitor.getWorksheetKey(), updateVisitor.getCriteriaQuery(), updateVisitor.getChanges());
		return result;
	}

	private UpdateResult executeInsert() throws TranslatorException {
		SpreadsheetInfo info = connection.getSpreadsheetInfo();
		SpreadsheetInsertVisitor visitor = new SpreadsheetInsertVisitor(info);
		visitor.visit((Insert) command);
		checkHeaders(visitor.getWorksheetTitle());
		result = connection.executeRowInsert(visitor.getWorksheetKey(), visitor.getColumnNameValuePair());
		return result;
	}

	private UpdateResult executeDelete() throws TranslatorException {
		SpreadsheetInfo info = connection.getSpreadsheetInfo();
		SpreadsheetDeleteVisitor visitor = new SpreadsheetDeleteVisitor(info);
		visitor.visit((Delete) command);
		checkHeaders(visitor.getWorksheetTitle());
		result = connection.deleteRows(visitor.getWorksheetKey(), visitor.getCriteriaQuery());
		return result;
	}
}
