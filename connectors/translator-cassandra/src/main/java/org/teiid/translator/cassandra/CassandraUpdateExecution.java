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
package org.teiid.translator.cassandra;

import org.teiid.language.Command;
import org.teiid.metadata.RuntimeMetadata;
import org.teiid.translator.DataNotAvailableException;
import org.teiid.translator.ExecutionContext;
import org.teiid.translator.TranslatorException;
import org.teiid.translator.UpdateExecution;

public class CassandraUpdateExecution implements UpdateExecution {
	
	private CassandraConnection connection;
	private ExecutionContext executionContext;
	private RuntimeMetadata metadata;
	private Command command;
	private int updateCount = 1;
	
	public CassandraUpdateExecution(Command command,
			ExecutionContext executionContext, RuntimeMetadata metadata,
			CassandraConnection connection) {
		this.command = command;
		this.executionContext = executionContext;
		this.metadata = metadata;
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
		CassandraSQLVisitor visitor = new CassandraSQLVisitor();
		visitor.translateSQL(this.command);
		try {
			connection.executeQuery(visitor.getTranslatedSQL());
		} catch(Throwable t) {
			throw new TranslatorException(t);
		}
	}

	@Override
	public int[] getUpdateCounts() throws DataNotAvailableException,
			TranslatorException {
		return new int[] {this.updateCount};
	}

}
