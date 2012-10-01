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
package org.teiid.translator.ldap;

import java.util.ArrayList;
import java.util.List;

import javax.naming.directory.SearchControls;
import javax.naming.ldap.LdapContext;

import org.teiid.language.Argument;
import org.teiid.translator.DataNotAvailableException;
import org.teiid.translator.ExecutionContext;
import org.teiid.translator.ProcedureExecution;
import org.teiid.translator.TranslatorException;

public class LDAPDirectSearchQueryExecution extends LDAPSyncQueryExecution implements ProcedureExecution {
	
	private List<Argument> arguments;
	
	public LDAPDirectSearchQueryExecution(List<Argument> arguments, LDAPExecutionFactory factory, ExecutionContext executionContext, LdapContext connection) {
		super(null, factory, executionContext, connection);
		this.arguments = arguments;
	}
	
	@Override
	public void execute() throws TranslatorException {
		String query = (String)arguments.get(0).getArgumentValue().getValue();
		IQueryToLdapSearchParser parser = new IQueryToLdapSearchParser(this.executionFactory);
		LDAPSearchDetails details = parser.buildRequest(query);
		// Create and configure the new search context.
		LdapContext context =  createSearchContext(details.getContextName());
		
		// build search controls
		SearchControls controls = new SearchControls();
		controls.setSearchScope(details.getSearchScope());
		controls.setTimeLimit(details.getTimeLimit());
		controls.setCountLimit(details.getCountLimit());
		controls.setReturningAttributes(details.getAttributes());
		
		this.delegate = new LDAPQueryExecution(context, details, controls, this.executionFactory, this.executionContext);
		this.delegate.execute();		
	}

	@Override
	public List<?> next() throws TranslatorException, DataNotAvailableException {
		List<?> vals = super.next();
		if (vals == null) {
			return null;
		}
		List<Object[]> row = new ArrayList<Object[]>(1);
		row.add(vals.toArray(new Object[vals.size()]));
		return row;
	}
	
	@Override
	public List<?> getOutputParameterValues() throws TranslatorException {
		return null;
	}
}
