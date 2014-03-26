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

package org.teiid.translator.simpledb;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;

import javax.resource.cci.ConnectionFactory;

import org.teiid.language.*;
import org.teiid.metadata.RuntimeMetadata;
import org.teiid.resource.adpter.simpledb.SimpleDBConnection;
import org.teiid.translator.*;

@Translator(name = "simpledb", description = "Translator for SimpleDB")
public class SimpleDBExecutionFactory extends ExecutionFactory<ConnectionFactory, SimpleDBConnection> {

	@Override
	public UpdateExecution createUpdateExecution(final Command command, ExecutionContext executionContext,
			RuntimeMetadata metadata, final SimpleDBConnection connection) throws TranslatorException {
		if (command instanceof Insert) {
			return new SimpleDBInsertExecute(command, connection);
		} else if (command instanceof Delete) {
			return new SimpleDBDeleteExecute(command, connection);
		} else if (command instanceof Update) {
			return new SimpleDBUpdateExecute(command, connection);
		} else {
			throw new TranslatorException("Just INSERT, DELETE and UPDATE are supported"); //$NON-NLS-1$
		}
	}

	@Override
	public ResultSetExecution createResultSetExecution(final QueryExpression command,
			ExecutionContext executionContext, RuntimeMetadata metadata, final SimpleDBConnection connection)
			throws TranslatorException {
		return new ResultSetExecution() {
			Iterator<List<String>> listIterator;

			@Override
			public void execute() throws TranslatorException {
				List<String> columns = new ArrayList<String>();
				for (DerivedColumn column : ((Select) command).getDerivedColumns()) {
					columns.add(SimpleDBSQLVisitor.getSQLString(column));
				}
				listIterator = connection.getAPIClass().performSelect(SimpleDBSQLVisitor.getSQLString(command),
						columns);

			}

			@Override
			public void close() {

			}

			@Override
			public void cancel() throws TranslatorException {

			}

			@Override
			public List<?> next() {
				try {
					return listIterator.next();
				} catch (NoSuchElementException ex) {
					return null;
				}
			}
		};
	}

	@Override
    public MetadataProcessor<SimpleDBConnection> getMetadataProcessor(){
	    return new SimpleDBMetadataProcessor();
	}

	@Override
	public boolean supportsCompareCriteriaEquals() {
		return true;
	}

	@Override
	public boolean supportsCompareCriteriaOrdered() {
		return true;
	}

	@Override
	public boolean supportsInCriteria() {
		return true;
	}

	@Override
	public boolean supportsIsNullCriteria() {
		return true;
	}

	@Override
	public boolean supportsRowLimit() {
		return true;
	}

	@Override
	public boolean supportsNotCriteria() {
		return true;
	}

	@Override
	public boolean supportsOrCriteria() {
		return true;
	}

	@Override
	public boolean supportsLikeCriteria() {
		return true;
	}
}
