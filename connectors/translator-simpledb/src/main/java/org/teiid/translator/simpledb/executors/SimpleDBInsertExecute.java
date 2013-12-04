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

package org.teiid.translator.simpledb.executors;

import java.util.Map;

import org.teiid.language.Command;
import org.teiid.language.Insert;
import org.teiid.resource.adpter.simpledb.SimpleDBConnection;
import org.teiid.translator.DataNotAvailableException;
import org.teiid.translator.TranslatorException;
import org.teiid.translator.UpdateExecution;
import org.teiid.translator.simpledb.visitors.SimpleDBInsertVisitor;

public class SimpleDBInsertExecute implements UpdateExecution{

	private Command command;
	private SimpleDBConnection connection;
	private int updatedCount=0;
	
	public SimpleDBInsertExecute(Command command, SimpleDBConnection connection) {
		this.command = command;
		this.connection = connection;
	}
	
	@Override
	public void close() {
		
	}

	@Override
	public void cancel() throws TranslatorException {
		
	}

	@Override
	public void execute() throws TranslatorException {
		Insert insert = (Insert) command;
		Map<String, String> columnsMap = SimpleDBInsertVisitor.getColumnsValuesMap(insert);
		updatedCount = connection.getAPIClass().performInsert(SimpleDBInsertVisitor.getDomainName(insert), columnsMap.get("itemName()"), columnsMap);
	}

	@Override
	public int[] getUpdateCounts() throws DataNotAvailableException, TranslatorException {
		return new int[] { updatedCount };
	}

}
