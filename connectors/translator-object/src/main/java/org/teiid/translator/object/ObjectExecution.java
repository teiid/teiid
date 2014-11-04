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

import javax.script.CompiledScript;
import javax.script.ScriptContext;
import javax.script.ScriptException;
import javax.script.SimpleScriptContext;

import org.teiid.language.ColumnReference;
import org.teiid.language.DerivedColumn;
import org.teiid.language.NamedTable;
import org.teiid.language.Select;
import org.teiid.logging.LogConstants;
import org.teiid.logging.LogManager;
import org.teiid.metadata.AbstractMetadataRecord;
import org.teiid.metadata.RuntimeMetadata;
import org.teiid.query.eval.TeiidScriptEngine;
import org.teiid.translator.DataNotAvailableException;
import org.teiid.translator.ExecutionContext;
import org.teiid.translator.ResultSetExecution;
import org.teiid.translator.TranslatorException;

/**
 * Execution of the SELECT Command
 */
public class ObjectExecution implements ResultSetExecution {

	private static final String OBJECT_NAME = "o"; //$NON-NLS-1$
	protected Select query;
	protected ObjectConnection connection;
	private ArrayList<CompiledScript> projects;
	private ScriptContext sc = new SimpleScriptContext();
	private static TeiidScriptEngine scriptEngine = new TeiidScriptEngine();
	private Iterator<Object> resultsIt = null;
	private ObjectExecutionFactory factory;
	private ExecutionContext executionContext;

	public ObjectExecution(Select query, RuntimeMetadata metadata,
			ObjectExecutionFactory factory, ObjectConnection connection, ExecutionContext executionContext) throws TranslatorException {
		this.factory = factory;
		this.query = query;
		this.connection = connection;
		this.executionContext = executionContext;

		projects = new ArrayList<CompiledScript>(query.getDerivedColumns().size());
		for (DerivedColumn dc : query.getDerivedColumns()) {
			ColumnReference cr = (ColumnReference) dc.getExpression();
			String name = null;
			if (cr.getMetadataObject() != null) {
				name = getNameInSource(cr.getMetadataObject());
			} else {
				name = cr.getName();
			}
				if (name.equalsIgnoreCase("this")) { //$NON-NLS-1$
					projects.add(null);
				} else {
					try {
						projects.add(scriptEngine.compile(OBJECT_NAME + "." + name)); //$NON-NLS-1$
					} catch (ScriptException e) {
						throw new TranslatorException(e);
					}
				}
		}
	}

	@Override
	public void execute() throws TranslatorException {

		LogManager.logTrace(LogConstants.CTX_CONNECTOR,
				"ObjectExecution command:", query.toString(), "using connection:", connection.getClass().getName()); //$NON-NLS-1$ //$NON-NLS-2$

		String nameInSource = ((NamedTable)query.getFrom().get(0)).getMetadataObject().getNameInSource();
	    
	    List<Object> results = factory.search(query, nameInSource, connection, executionContext);
	    
		if (results != null && results.size() > 0) {
			LogManager.logDetail(LogConstants.CTX_CONNECTOR,
					"ObjectExecution number of returned objects is :", results.size()); //$NON-NLS-1$

		} else {
			LogManager.logDetail(LogConstants.CTX_CONNECTOR,
					"ObjectExecution number of objects returned is : 0"); //$NON-NLS-1$

			results = Collections.emptyList();
		}

		this.resultsIt = results.iterator();
	}

	@Override
	public List<Object> next() throws TranslatorException,
			DataNotAvailableException {
		// create and return one row at a time for your resultset.
		if (resultsIt.hasNext()) {
			List<Object> r = new ArrayList<Object>(projects.size());
			Object o = resultsIt.next();
			sc.setAttribute(OBJECT_NAME, o, ScriptContext.ENGINE_SCOPE);
			for (CompiledScript cs : this.projects) {
				if (cs == null) {
					r.add(o);
					continue;
				}
				try {
					r.add(cs.eval(sc));
				} catch (ScriptException e) {
					throw new TranslatorException(e);
				}
			}
			return r;
		}
		return null;
	}

	@Override
	public void close() {
		this.query = null;
		this.connection = null;
		this.resultsIt = null;
	}

	@Override
	public void cancel()  {
	}
	
	private static String getNameInSource(AbstractMetadataRecord c) {
		String name = c.getNameInSource();
		if (name == null || name.trim().isEmpty()) {
			return c.getName();
		}
		return name;
	}
	
}
