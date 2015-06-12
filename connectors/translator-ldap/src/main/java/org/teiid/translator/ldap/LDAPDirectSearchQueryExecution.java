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
import org.teiid.language.visitor.SQLStringVisitor;
import org.teiid.translator.DataNotAvailableException;
import org.teiid.translator.ExecutionContext;
import org.teiid.translator.ProcedureExecution;
import org.teiid.translator.TranslatorException;

public class LDAPDirectSearchQueryExecution extends LDAPSyncQueryExecution implements ProcedureExecution {
	
	private String query;
	private boolean returnsArray = true;
	
	public LDAPDirectSearchQueryExecution(List<Argument> arguments, LDAPExecutionFactory factory, ExecutionContext executionContext, LdapContext connection, String query, boolean returnsArray) {
		super(null, factory, executionContext, connection);
		//perform substitution
		StringBuilder sb = new StringBuilder();
		SQLStringVisitor.parseNativeQueryParts(query.substring(7), arguments, sb, new SQLStringVisitor.Substitutor() {
			
			@Override
			public void substitute(Argument arg, StringBuilder builder, int index) {
				builder.append(IQueryToLdapSearchParser.escapeReservedChars(IQueryToLdapSearchParser.getLiteralString(arg.getArgumentValue())));
			}
		});
		this.query = sb.toString();
		this.returnsArray = returnsArray;
	}
	
	@Override
	public void execute() throws TranslatorException {
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
		if (returnsArray) {
			List<Object[]> row = new ArrayList<Object[]>(1);
			row.add(vals.toArray(new Object[vals.size()]));
			return row;
		}
		return vals;
	}
	
	@Override
	public List<?> getOutputParameterValues() throws TranslatorException {
		return null;
	}
}
