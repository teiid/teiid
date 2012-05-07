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
import java.util.Iterator;
import java.util.List;

import javax.persistence.EntityManager;
import javax.persistence.Query;

import org.teiid.language.QueryExpression;
import org.teiid.language.Select;
import org.teiid.logging.LogConstants;
import org.teiid.logging.LogManager;
import org.teiid.metadata.RuntimeMetadata;
import org.teiid.translator.DataNotAvailableException;
import org.teiid.translator.ExecutionContext;
import org.teiid.translator.ResultSetExecution;
import org.teiid.translator.TranslatorException;

public class JPQLQueryExecution extends JPQLBaseExecution implements ResultSetExecution {
	private QueryExpression command;
	private Iterator resultsIterator;
	private JPA2ExecutionFactory executionFactory;

	public JPQLQueryExecution(JPA2ExecutionFactory executionFactory, QueryExpression command, ExecutionContext executionContext, RuntimeMetadata metadata, EntityManager em) {
		super(executionContext, metadata, em);
		this.command = command;
		this.executionFactory = executionFactory;
	}

	@Override
	public void execute() throws TranslatorException {
		String jpql = JPQLSelectVisitor.getJPQLString((Select)this.command, this.executionFactory, this.metadata);
		
		LogManager.logTrace(LogConstants.CTX_CONNECTOR, "JPA Source-Query:", jpql); //$NON-NLS-1$
		
		Query query = this.enityManager.createQuery(jpql);
		List results = query.getResultList();
		this.resultsIterator = results.iterator();
	}

	@Override
	public List<?> next() throws TranslatorException, DataNotAvailableException {
		if (this.resultsIterator != null && this.resultsIterator.hasNext()) {
			Object obj = this.resultsIterator.next();
			if (obj instanceof Object[]) {
				return Arrays.asList((Object[])obj);
			}
			return Arrays.asList(obj);
		}
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
}
