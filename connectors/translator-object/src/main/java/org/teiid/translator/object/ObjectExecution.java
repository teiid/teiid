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

package org.teiid.translator.object;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

import org.teiid.language.Select;
import org.teiid.logging.LogConstants;
import org.teiid.logging.LogManager;
import org.teiid.metadata.RuntimeMetadata;
import org.teiid.translator.DataNotAvailableException;
import org.teiid.translator.ResultSetExecution;
import org.teiid.translator.TranslatorException;

/**
 * Execution of the SELECT Command
 */
public class ObjectExecution implements ResultSetExecution {

	private Select query;
	private Object connection;
	private ObjectExecutionFactory config;

	private Iterator<Object> resultsIt = null;

	public ObjectExecution(Select query, RuntimeMetadata metadata,
			ObjectExecutionFactory factory, Object connection) {
		this.query = query;
		this.connection = connection;
		this.config = factory;
	}

	@Override
	public void execute() throws TranslatorException {

		LogManager.logTrace(LogConstants.CTX_CONNECTOR,
				"ObjectExecution command: " + query.toString()); //$NON-NLS-1$

		SelectProjections projections = SelectProjections.create(config);
		projections.parse(query);

		List<Object> results = executeQuery(projections);

		if (results != null && results.size() > 0) {
			LogManager.logDetail(LogConstants.CTX_CONNECTOR,
					"ObjectExecution number of returned objects is : " + results.size()); //$NON-NLS-1$

		} else {
			LogManager.logDetail(LogConstants.CTX_CONNECTOR,
					"ObjectExecution number of objects returned is : 0"); //$NON-NLS-1$

			results = Collections.emptyList();
		}

		this.resultsIt = results.iterator();
	}

	protected List<Object> executeQuery(SelectProjections projections)
			throws TranslatorException {

		SearchStrategy is = this.config.getSearchStrategy();
		LogManager
				.logTrace(
						LogConstants.CTX_CONNECTOR,
						"ObjectExecution calling search strategy : " + is.getClass().getName()); //$NON-NLS-1$

		return is.performSearch((Select) query, projections, this.config,
				this.connection);

	}

	@Override
	public List<Object> next() throws TranslatorException,
			DataNotAvailableException {
		// create and return one row at a time for your resultset.
		if (resultsIt.hasNext()) {
			List<Object> r = new ArrayList<Object>(1);
			r.add(resultsIt.next());
			return r;
		}
		return null;
	}

	@Override
	public void close() {
		this.query = null;
		this.connection = null;
		this.config = null;
		this.resultsIt = null;
	}

	@Override
	public void cancel() throws TranslatorException {
	}

}
