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

import java.util.List;

import javax.naming.ldap.LdapContext;
import javax.resource.cci.ConnectionFactory;

import org.teiid.language.Argument;
import org.teiid.language.Call;
import org.teiid.language.Command;
import org.teiid.language.QueryExpression;
import org.teiid.language.Select;
import org.teiid.language.visitor.SQLStringVisitor;
import org.teiid.metadata.MetadataFactory;
import org.teiid.metadata.RuntimeMetadata;
import org.teiid.translator.ExecutionContext;
import org.teiid.translator.ExecutionFactory;
import org.teiid.translator.ProcedureExecution;
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

	public static final String DN_PREFIX = MetadataFactory.LDAP_URI + "dn_prefix"; //$NON-NLS-1$
	public static final String RDN_TYPE = MetadataFactory.LDAP_URI + "rdn_type"; //$NON-NLS-1$
	public static final String UNWRAP = MetadataFactory.LDAP_URI + "unwrap"; //$NON-NLS-1$
	
	public enum SearchDefaultScope {
		SUBTREE_SCOPE,
		OBJECT_SCOPE,
		ONELEVEL_SCOPE
	}
	
	private String searchDefaultBaseDN;
	private boolean restrictToObjectClass;
	private SearchDefaultScope searchDefaultScope = SearchDefaultScope.ONELEVEL_SCOPE;
	private boolean usePagination;
	private boolean exceptionOnSizeLimitExceeded;
	
	public LDAPExecutionFactory() {
		this.setMaxInCriteriaSize(1000);
		this.setMaxDependentInPredicates(25); //no spec limit on query size, AD is 10MB for the query
		this.setSupportsInnerJoins(true);
		this.setSupportedJoinCriteria(SupportedJoinCriteria.KEY);
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
		return new LDAPSyncQueryExecution((Select)command, this, executionContext, context);
	}
	
	@Override
	public UpdateExecution createUpdateExecution(Command command,ExecutionContext executionContext, RuntimeMetadata metadata, LdapContext context)
			throws TranslatorException {
		return new LDAPUpdateExecution(command, context);
	}	
	
	@Override
	public ProcedureExecution createDirectExecution(List<Argument> arguments,Command command, ExecutionContext executionContext,RuntimeMetadata metadata, LdapContext context) throws TranslatorException {
		String query = (String) arguments.get(0).getArgumentValue().getValue();
		if (query.startsWith("search;")) { //$NON-NLS-1$
			return new LDAPDirectSearchQueryExecution(arguments.subList(1, arguments.size()), this, executionContext, context, query, true);
		}
		return new LDAPDirectCreateUpdateDeleteQueryExecution(arguments.subList(1, arguments.size()), this, executionContext, context, query, true);
	}	
	
	@Override
	public ProcedureExecution createProcedureExecution(Call command,
			ExecutionContext executionContext, RuntimeMetadata metadata,
			LdapContext connection) throws TranslatorException {
		String nativeQuery = command.getMetadataObject().getProperty(SQLStringVisitor.TEIID_NATIVE_QUERY, false);
    	if (nativeQuery != null) {
    		if (nativeQuery.startsWith("search;")) { //$NON-NLS-1$
    			return new LDAPDirectSearchQueryExecution(command.getArguments(), this, executionContext, connection, nativeQuery, false);
    		}
    		return new LDAPDirectCreateUpdateDeleteQueryExecution(command.getArguments(), this, executionContext, connection, nativeQuery, false);
    	}
    	throw new TranslatorException("Missing native-query extension metadata."); //$NON-NLS-1$
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

	@TranslatorProperty(display="Use Pagination", description="Use a PagedResultsControl to page through large results.  This is not supported by all directory servers.")
	public boolean usePagination() {
		return usePagination;
	}	
	
	public void setUsePagination(boolean usePagination) {
		this.usePagination = usePagination;
	}
	
	@TranslatorProperty(display="Exception on Size Limit Exceeded", description="Set to true to throw an exception when a SizeLimitExceededException is received and a LIMIT is not properly enforced.")
	public boolean isExceptionOnSizeLimitExceeded() {
		return exceptionOnSizeLimitExceeded;
	}
	
	public void setExceptionOnSizeLimitExceeded(
			boolean exceptionOnSizeLimitExceeded) {
		this.exceptionOnSizeLimitExceeded = exceptionOnSizeLimitExceeded;
	}
	
	@Override
	public boolean supportsOnlyLiteralComparison() {
		return true;
	}
	
	@Override
	public boolean supportsLikeCriteriaEscapeCharacter() {
		return true;
	}

	@Override
	public int getMaxFromGroups() {
		return 2;
	}
	
	@Override
	public boolean useAnsiJoin() {
		return true;
	}
	
	@Override
	public boolean supportsPartialFiltering() {
		return true;
	}
	
}
