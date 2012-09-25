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
import java.util.StringTokenizer;

import javax.naming.directory.SearchControls;
import javax.naming.ldap.LdapContext;

import org.teiid.language.Argument;
import org.teiid.metadata.Column;
import org.teiid.metadata.Datatype;
import org.teiid.translator.DataNotAvailableException;
import org.teiid.translator.ExecutionContext;
import org.teiid.translator.ProcedureExecution;
import org.teiid.translator.TranslatorException;
import org.teiid.translator.TypeFacility;

public class LDAPDirectSearchQueryExecution extends LDAPSyncQueryExecution implements ProcedureExecution {

	private static final String ATTRIBUTES = "attributes"; //$NON-NLS-1$
	private static final String COUNT_LIMIT = "count-limit"; //$NON-NLS-1$
	private static final String TIMEOUT = "timeout";//$NON-NLS-1$
	private static final String SEARCH_SCOPE = "search-scope";//$NON-NLS-1$
	private static final String CRITERIA = "filter";//$NON-NLS-1$
	private static final String CONTEXT_NAME = "context-name";//$NON-NLS-1$
	
	private List<Argument> arguments;
	
	public LDAPDirectSearchQueryExecution(List<Argument> arguments, LDAPExecutionFactory factory, ExecutionContext executionContext, LdapContext connection) {
		super(null, factory, executionContext, connection);
		this.arguments = arguments;
	}
	
	@Override
	public void execute() throws TranslatorException {
		this.delegate = buildRequest();
		this.delegate.execute();		
	}

	private LDAPQueryExecution buildRequest() throws TranslatorException {
		String query = (String)arguments.get(0).getArgumentValue().getValue();
		
		ArrayList<String> attributes = new ArrayList<String>();
		ArrayList<Column> columns = new ArrayList<Column>();
		String contextName = null;
		String criteria = ""; //$NON-NLS-1$
		String searchScope = this.executionFactory.getSearchDefaultScope().name();
		int timeLimit = 0;
		long countLimit = 0;
		
		StringTokenizer st = new StringTokenizer(query, ";"); //$NON-NLS-1$
		while (st.hasMoreTokens()) {
			String var = st.nextToken();
			int index = var.indexOf('=');
			if (index == -1) {
				continue;
			}
			String key = var.substring(0, index).trim().toLowerCase();
			String value = var.substring(index+1).trim();
			
			if (key.equalsIgnoreCase(CONTEXT_NAME)) {
				contextName = value;
			}
			else if (key.equalsIgnoreCase(CRITERIA)) {
				criteria = value;
			}
			else if (key.equalsIgnoreCase(SEARCH_SCOPE)) {
				searchScope = value;
			}
			else if (key.equalsIgnoreCase(TIMEOUT)) {
				timeLimit = Integer.parseInt(value);
			}
			else if (key.equalsIgnoreCase(COUNT_LIMIT)) {
				countLimit = Long.parseLong(value);
			}
			else if (key.equalsIgnoreCase(ATTRIBUTES)) {
				StringTokenizer attrTokens = new StringTokenizer(value, ","); //$NON-NLS-1$
				while(attrTokens.hasMoreElements()) {
					String name = attrTokens.nextToken().trim();
					attributes.add(name);
					
					Column column = new Column();
					column.setName(name);
					Datatype type = new Datatype();
					type.setName(TypeFacility.RUNTIME_NAMES.OBJECT);
					type.setJavaClassName(Object.class.getCanonicalName());
					column.setDatatype(type, true);
					columns.add(column);
				}
			}
		}
		
		int searchScopeInt = buildSearchScope(searchScope);
		
		// build search controls
		SearchControls controls = new SearchControls();
		controls.setSearchScope(searchScopeInt);
		controls.setTimeLimit(timeLimit);
		controls.setCountLimit(countLimit);
		controls.setReturningAttributes(attributes.toArray(new String[attributes.size()]));
		
		LDAPSearchDetails searchDetails = new LDAPSearchDetails(contextName, searchScopeInt, criteria, null, countLimit,  columns);

		// Create and configure the new search context.
		LdapContext context =  createSearchContext(contextName);
		return new LDAPQueryExecution(context, searchDetails, controls, this.executionFactory, this.executionContext);
	}

	private int buildSearchScope(String searchScope) {
		int searchScopeInt = 0;
		// this could be one of OBJECT_SCOPE, ONELEVEL_SCOPE, SUBTREE_SCOPE
		if (searchScope.equalsIgnoreCase("OBJECT_SCOPE")) { //$NON-NLS-1$
			searchScopeInt = SearchControls.OBJECT_SCOPE;
		}
		else if (searchScope.equalsIgnoreCase("ONELEVEL_SCOPE")) {//$NON-NLS-1$
			searchScopeInt = SearchControls.ONELEVEL_SCOPE;
		}
		else if (searchScope.equalsIgnoreCase("SUBTREE_SCOPE")) {//$NON-NLS-1$
			searchScopeInt =  SearchControls.SUBTREE_SCOPE;
		}
		return searchScopeInt;
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
