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
import javax.resource.ResourceException;
import javax.resource.cci.ConnectionFactory;

import org.teiid.language.Command;
import org.teiid.language.QueryExpression;
import org.teiid.language.Select;
import org.teiid.metadata.RuntimeMetadata;
import org.teiid.translator.BasicExecutionFactory;
import org.teiid.translator.ConnectorCapabilities;
import org.teiid.translator.ConnectorException;
import org.teiid.translator.ExecutionContext;
import org.teiid.translator.ResultSetExecution;
import org.teiid.translator.TranslatorProperty;
import org.teiid.translator.UpdateExecution;


/** 
 * LDAPConnector.  This is responsible for initializing 
 * a connection factory, and obtaining connections to LDAP.
 */
public class LDAPExecutionFactory extends BasicExecutionFactory {

	private String searchDefaultBaseDN;
	private boolean restrictToObjectClass = false;
	private String searchDefaultScope = "SUBTREE_SCOPE"; //$NON-NLS-1$
	
    public Class<? extends ConnectorCapabilities> getDefaultCapabilities() {
    	return LDAPConnectorCapabilities.class;
    }	
    	
    @TranslatorProperty(name="SearchDefaultBaseDN", display="Default Search Base DN", description="Default Base DN for LDAP Searches",advanced=true, defaultValue="")
	public String getSearchDefaultBaseDN() {
		return searchDefaultBaseDN;
	}
	
	public void setSearchDefaultBaseDN(String searchDefaultBaseDN) {
		this.searchDefaultBaseDN = searchDefaultBaseDN;
	}
	
	@TranslatorProperty(name="RestrictToObjectClass", display="Restrict Searches To Named Object Class", description="Restrict Searches to objectClass named in the Name field for a table", advanced=true, defaultValue="false")
	public boolean isRestrictToObjectClass() {
		return restrictToObjectClass;
	}
	
	public void setRestrictToObjectClass(boolean restrictToObjectClass) {
		this.restrictToObjectClass = restrictToObjectClass;
	}

	@TranslatorProperty(name="SearchDefaultScope", display="Default Search Scope", description="Default Scope for LDAP Searches", allowed={"OBJECT_SCOPE","ONELEVEL_SCOPE","SUBTREE_SCOPE"},required=true, defaultValue="SUBTREE_SCOPE")
	public String getSearchDefaultScope() {
		return searchDefaultScope;
	}
	
	public void setSearchDefaultScope(String searchDefaultScope) {
		this.searchDefaultScope = searchDefaultScope;
	}    
	
	@Override
	public ResultSetExecution createResultSetExecution(QueryExpression command,ExecutionContext executionContext, RuntimeMetadata metadata, Object connectionFactory)
			throws ConnectorException {
		try {
			ConnectionFactory cf = (ConnectionFactory)connectionFactory;
			return new LDAPSyncQueryExecution((Select)command, this, (LdapContext)cf.getConnection());
		} catch (ResourceException e) {
			throw new ConnectorException(e);
		}
	}
	
	@Override
	public UpdateExecution createUpdateExecution(Command command,ExecutionContext executionContext, RuntimeMetadata metadata, Object connectionFactory)
			throws ConnectorException {
		try {
			ConnectionFactory cf = (ConnectionFactory)connectionFactory;
			return new LDAPUpdateExecution(command, (LdapContext)cf.getConnection());
		} catch (ResourceException e) {
			throw new ConnectorException(e);
		}
	}	
}
