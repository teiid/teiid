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

import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

import org.teiid.language.Command;
import org.teiid.language.Delete;
import org.teiid.resource.adpter.simpledb.SimpleDBConnection;
import org.teiid.translator.DataNotAvailableException;
import org.teiid.translator.TranslatorException;
import org.teiid.translator.UpdateExecution;
import org.teiid.translator.simpledb.visitors.SimpleDBDeleteVisitor;

public class SimpleDBDeleteExecute implements UpdateExecution {
	
	private Command command;
	private SimpleDBConnection connection;
	private int updatedCount=0;
	
	public SimpleDBDeleteExecute(Command command, SimpleDBConnection connection) {
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
		Delete delete = (Delete) command;
		SimpleDBDeleteVisitor visitor = new SimpleDBDeleteVisitor(delete, connection.getAPIClass());
		if (visitor.hasWhere()){
			if (visitor.isSimpleDelete()){
				connection.getAPIClass().performDelete(visitor.getTableName(), visitor.getItemName());
				updatedCount = 1;
			}else{
				for (String itemName : visitor.getItemNames()) {
					connection.getAPIClass().performDelete(visitor.getTableName(), itemName);
				}
				updatedCount = visitor.getItemNames().size();
			}
		}else{
			Iterator<List<String>> result = connection.getAPIClass().performSelect("SELECT itemName() FROM "+visitor.getTableName(), Arrays.asList("itemName()"));
			while (result.hasNext()) {
				String itemName = result.next().get(0);
				connection.getAPIClass().performDelete(visitor.getTableName(), itemName);
				updatedCount++;
			}
		}

	}

	@Override
	public int[] getUpdateCounts() throws DataNotAvailableException, TranslatorException {
		return new int[] { updatedCount };
	}

}
