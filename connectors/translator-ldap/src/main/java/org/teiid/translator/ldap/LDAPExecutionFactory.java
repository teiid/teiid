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

import javax.naming.ldap.LdapContext;
import javax.resource.cci.ConnectionFactory;

import org.teiid.language.Command;
import org.teiid.language.QueryExpression;
import org.teiid.language.Select;
import org.teiid.metadata.RuntimeMetadata;
import org.teiid.translator.ExecutionContext;
import org.teiid.translator.ExecutionFactory;
import org.teiid.translator.ResultSetExecution;
import org.teiid.translator.Translator;
import org.teiid.translator.TranslatorException;
import org.teiid.translator.TranslatorProperty;
import org.teiid.translator.UpdateExecution;


/** 
 * LDAP translator.  This is responsible for initializing 
 * a connection factory, and obtaining connections to LDAP.
 */
@Translator(name="ldap", description="A translator for LDAP directory")
public class LDAPExecutionFactory extends ExecutionFactory<ConnectionFactory, LdapContext> {

	public enum SearchDefaultScope {
		SUBTREE_SCOPE,
		OBJECT_SCOPE,
		ONELEVEL_SCOPE
	}
	
	private String searchDefaultBaseDN;
	private boolean restrictToObjectClass;
	private SearchDefaultScope searchDefaultScope = SearchDefaultScope.ONELEVEL_SCOPE;
	
	public LDAPExecutionFactory() {
		this.setMaxInCriteriaSize(1000);
		this.setMaxDependentInPredicates(25); //no spec limit on query size, AD is 10MB for the query
	}
	
    @TranslatorProperty(display="Default Search Base DN", description="Default Base DN for LDAP Searches")
	public String getSearchDefaultBaseDN() {
		return searchDefaultBaseDN;
	}
	
	public void setSearchDefaultBaseDN(String searchDefaultBaseDN) {
		this.searchDefaultBaseDN = searchDefaultBaseDN;
	}
	
	@TranslatorProperty(display="Restrict Searches To Named Object Class", description="Restrict Searches to objectClass named in the Name field for a table", advanced=true)
	public boolean isRestrictToObjectClass() {
		return restrictToObjectClass;
	}
	
	public void setRestrictToObjectClass(boolean restrictToObjectClass) {
		this.restrictToObjectClass = restrictToObjectClass;
	}

	@TranslatorProperty(display="Default Search Scope", description="Default Scope for LDAP Searches")
	public SearchDefaultScope getSearchDefaultScope() {
		return searchDefaultScope;
	}
	
	public void setSearchDefaultScope(SearchDefaultScope searchDefaultScope) {
		this.searchDefaultScope = searchDefaultScope;
	}    
	
	@Override
	public ResultSetExecution createResultSetExecution(QueryExpression command,ExecutionContext executionContext, RuntimeMetadata metadata, LdapContext context)
			throws TranslatorException {
		return new LDAPSyncQueryExecution((Select)command, this, context);
	}
	
	@Override
	public UpdateExecution createUpdateExecution(Command command,ExecutionContext executionContext, RuntimeMetadata metadata, LdapContext context)
			throws TranslatorException {
		return new LDAPUpdateExecution(command, context);
	}	
	
	@Override
    public boolean supportsCompareCriteriaEquals() {
		return true;
	}

    @Override
	public boolean supportsInCriteria() {
		return true;
	}

	@Override
	public boolean supportsLikeCriteria() {
		return true;
	}

	@Override
	public boolean supportsOrCriteria() {
		return true;
	}

	@Override
	public boolean supportsRowLimit() {
		// GHH 20080408 - turned this on, because I fixed issue
		// in nextBatch that was causing this to fail
		return true;
	}

	@Override
	public boolean supportsRowOffset() {
		// TODO This might actually be possible in future releases,
		// when using virtual list views/Sun. note that this requires the ability
		// to set the count limit, as well as an offset, so setCountLimit::searchControls
		// won't do it alone.
		return false;
	}
	
	@Override
	public boolean supportsCompareCriteriaOrdered() {
		return true;
	}
	
	@Override
	public boolean supportsNotCriteria() {
		return true;
	}	
}
