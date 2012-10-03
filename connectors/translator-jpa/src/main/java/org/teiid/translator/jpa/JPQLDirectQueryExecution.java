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
package org.teiid.translator.jpa;

import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

import javax.persistence.EntityManager;
import javax.persistence.Query;

import org.teiid.language.Argument;
import org.teiid.language.Command;
import org.teiid.language.Literal;
import org.teiid.language.visitor.SQLStringVisitor;
import org.teiid.logging.LogConstants;
import org.teiid.logging.LogManager;
import org.teiid.metadata.RuntimeMetadata;
import org.teiid.translator.DataNotAvailableException;
import org.teiid.translator.ExecutionContext;
import org.teiid.translator.ProcedureExecution;
import org.teiid.translator.TranslatorException;

public class JPQLDirectQueryExecution extends JPQLBaseExecution implements ProcedureExecution{
	private Iterator<?> resultsIterator;
	private List<Argument> arguments;
	private boolean returnsArray = true;
	private String query;

	@SuppressWarnings("unused")
	public JPQLDirectQueryExecution(List<Argument> arguments, Command command, ExecutionContext executionContext, RuntimeMetadata metadata, EntityManager em, String query, boolean returnsArray) {
		super(executionContext, metadata, em);
		this.arguments = arguments;
		this.returnsArray = returnsArray;
		this.query = query;
	}

	@Override
	public void execute() throws TranslatorException {
		if (query.length() < 7) {
			throw new TranslatorException(JPAPlugin.Util.gs(JPAPlugin.Event.TEIID14008));
		}
		String firstToken = query.substring(0, 7);
		
		String jpql = query.substring(7);
		LogManager.logTrace(LogConstants.CTX_CONNECTOR, "JPA Source-Query:", jpql); //$NON-NLS-1$

		if (firstToken.equalsIgnoreCase("search;")) { // //$NON-NLS-1$
			StringBuilder buffer = new StringBuilder();
			SQLStringVisitor.parseNativeQueryParts(jpql, arguments, buffer, new SQLStringVisitor.Substitutor() {
				
				@Override
				public void substitute(Argument arg, StringBuilder builder, int index) {
					Literal argumentValue = arg.getArgumentValue();
					builder.append(argumentValue);
				}
			});
			jpql = buffer.toString();
			Query queryCommand = this.enityManager.createQuery(jpql);
			List<?> results = queryCommand.getResultList();
			this.resultsIterator = results.iterator();
		}		
		else if (firstToken.equalsIgnoreCase("create;")) { // //$NON-NLS-1$
			Object entity = arguments.get(0).getArgumentValue().getValue();
			this.enityManager.merge(entity);
			this.resultsIterator = Arrays.asList(1).iterator();
		}
		else if (firstToken.equalsIgnoreCase("update;") || firstToken.equalsIgnoreCase("delete;")) { // //$NON-NLS-1$ //$NON-NLS-2$
			Query queryCmd = this.enityManager.createQuery(jpql);
			this.resultsIterator = Arrays.asList(queryCmd.executeUpdate()).iterator();
		} else {
			throw new TranslatorException(JPAPlugin.Util.gs(JPAPlugin.Event.TEIID14008));
		}
	}

	@Override
	public List<?> next() throws TranslatorException, DataNotAvailableException {
		if (this.resultsIterator != null && this.resultsIterator.hasNext()) {
			Object obj = this.resultsIterator.next();
			if (obj instanceof Object[]) {
				if (returnsArray) {
					return Arrays.asList(obj);
				}
				return Arrays.asList((Object[])obj);
			}
			if (returnsArray) {
				return Collections.singletonList(new Object[] {obj});
			}
			return Arrays.asList(obj);
		}
		this.resultsIterator = null;
		return null;
	}
	
	@Override
	public void close() {
		// no close
		this.resultsIterator = null;

	}

	@Override
	public void cancel() throws TranslatorException {
		// no cancel
	}

	@Override
	public List<?> getOutputParameterValues() throws TranslatorException {
		return null;
	}
}
