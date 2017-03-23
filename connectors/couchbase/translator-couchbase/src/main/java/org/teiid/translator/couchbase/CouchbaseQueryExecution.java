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
package org.teiid.translator.couchbase;

import java.util.List;

import org.teiid.couchbase.CouchbaseConnection;
import org.teiid.language.QueryExpression;
import org.teiid.language.Select;
import org.teiid.metadata.RuntimeMetadata;
import org.teiid.translator.DataNotAvailableException;
import org.teiid.translator.ExecutionContext;
import org.teiid.translator.ResultSetExecution;
import org.teiid.translator.TranslatorException;

public class CouchbaseQueryExecution extends CouchbaseExecution implements ResultSetExecution {
    
	private Select command;
	private CouchbaseExecutionFactory executionFactory;

	private Class<?>[] expectedTypes;

	public CouchbaseQueryExecution(
			CouchbaseExecutionFactory executionFactory,
			QueryExpression command, ExecutionContext executionContext,
			RuntimeMetadata metadata, CouchbaseConnection connection) {
		super(executionContext, metadata, connection);
		this.command = (Select)command;
		this.executionFactory = executionFactory;
		this.expectedTypes = command.getColumnTypes();
	}

	@Override
	public void execute() throws TranslatorException {
		
	}

	@Override
	public List<?> next() throws TranslatorException, DataNotAvailableException {

		return null;
	}

	@Override
	public void close() {

	}

	@Override
	public void cancel() throws TranslatorException {
		close();
	}
}
