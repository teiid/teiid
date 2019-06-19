/*
 * Copyright Red Hat, Inc. and/or its affiliates
 * and other contributors as indicated by the @author tags and
 * the COPYRIGHT.txt file distributed with this work.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

/**
 *
 * Please see the user's guide for a full description of capabilties, etc.
 *
 * Description/Assumptions:
 * 1. Table's name in source defines the base DN (or context) for the search.
 * Example: Table.NameInSource=ou=people,dc=gene,dc=com
 * [Optional] The table's name in source can also define a search scope. Append
 * a "?" character as a delimiter to the base DN, and add the search scope string.
 * The following scopes are available:
 * SUBTREE_SCOPE
 * ONELEVEL_SCOPE
 * OBJECT_SCOPE
 * [Default] LDAPConnectorConstants.ldapDefaultSearchScope
 * is the default scope used, if no scope is defined (currently, ONELEVEL_SCOPE).
 *
 * 2. Column's name in source defines the LDAP attribute name.
 * [Default] If no name in source is defined, then we attempt to use the column name
 * as the LDAP attribute name.
 *
 *
 * TODO: Implement paged searches -- the LDAP server must support VirtualListViews.
 * TODO: Implement cancel.
 * TODO: Add Sun/Netscape implementation, AD/OpenLDAP implementation.
 *
 *
 * Note:
 * Greater than is treated as >=
 * Less-than is treater as <=
 * If an LDAP entry has more than one entry for an attribute of interest (e.g. a select item), we only return the
 * first occurrance. The first occurance is not predictably the same each time, either, according to the LDAP spec.
 * If an attribute is not present, we return the empty string. Arguably, we could throw an exception.
 *
 * Sun LDAP won't support Sort Orders for very large datasets. So, we've set the sorting to NONCRITICAL, which
 * allows Sun to ignore the sort order. This will result in the results to come back as unsorted, without any error.
 *
 * Removed support for ORDER BY for two reasons:
 * 1: LDAP appears to have a limit to the number of records that
 * can be server-side sorted. When the limit is reached, two things can happen:
 * a. If sortControl is set to CRITICAL, then the search fails.
 * b. If sortControl is NONCRITICAL, then the search returns, unsorted.
 * We'd like to support ORDER BY, no matter how large the size, so we turn it off,
 * and allow MetaMatrix to do it for us.
 * 2: Supporting ORDER BY appears to negatively effect the query plan
 * when cost analysis is used. We stop using dependent queries, and start
 * using inner joins.
 *
 */

package org.teiid.translator.ldap;

import java.util.ArrayList;
import java.util.List;

import javax.naming.NamingException;
import javax.naming.directory.SearchControls;
import javax.naming.ldap.LdapContext;

import org.teiid.language.Select;
import org.teiid.metadata.Column;
import org.teiid.translator.DataNotAvailableException;
import org.teiid.translator.ExecutionContext;
import org.teiid.translator.ResultSetExecution;
import org.teiid.translator.TranslatorException;



/**
 * LDAPSyncQueryExecution is responsible for executing an LDAP search
 * corresponding to a read-only "select" query from Teiid.
 */
public class LDAPSyncQueryExecution implements ResultSetExecution {

    protected LdapContext ldapConnection;
    private Select query;
    protected LDAPExecutionFactory executionFactory;
    protected ExecutionContext executionContext;
    protected LDAPQueryExecution delegate;

    /**
     * Constructor
     * @param connection the LDAP Context
     */
    public LDAPSyncQueryExecution(Select query, LDAPExecutionFactory factory, ExecutionContext context, LdapContext connection) {
        this.ldapConnection = connection;
        this.query = query;
        this.executionFactory = factory;
        this.executionContext = context;
    }

    /**
     * method to execute the supplied query
     */
    @Override
    public void execute() throws TranslatorException {
        // Parse the IQuery, and translate it into an appropriate LDAP search.
        IQueryToLdapSearchParser parser = new IQueryToLdapSearchParser(this.executionFactory);
        LDAPSearchDetails searchDetails = parser.translateSQLQueryToLDAPSearch(query);

        // Create and configure the new search context.
        LdapContext context =  createSearchContext(searchDetails.getContextName());
        SearchControls ctrls = setSearchControls(searchDetails);

        this.delegate = new LDAPQueryExecution(context, searchDetails, ctrls, this.executionFactory, this.executionContext);
        this.delegate.execute();
    }



    /**
     * Perform a lookup against the initial LDAP context, which
     * sets the context to something appropriate for the search that is about to occur.
     *
     */
    protected LdapContext createSearchContext(String contextName) throws TranslatorException {
        try {
            return (LdapContext) this.ldapConnection.lookup(contextName);
        } catch (NamingException ne) {
            throw new TranslatorException(LDAPPlugin.Event.TEIID12002, ne, LDAPPlugin.Util.gs(LDAPPlugin.Event.TEIID12002, contextName));
        }
    }

    /**
     * Set the search controls
     */
    private SearchControls setSearchControls(LDAPSearchDetails searchDetails) {
        SearchControls ctrls = new SearchControls();
        //ArrayList modelAttrList = searchDetails.getAttributeList();
        ArrayList<Column> modelAttrList = searchDetails.getElementList();
        String[] attrs = new String[modelAttrList.size()];
        for (int i = 0; i < attrs.length; i++) {
            attrs[i] = modelAttrList.get(i).getSourceName();
        }

        ctrls.setSearchScope(searchDetails.getSearchScope());
        ctrls.setReturningAttributes(attrs);

        long limit = searchDetails.getCountLimit();
        if(limit != -1) {
            ctrls.setCountLimit(limit);
        }
        return ctrls;
    }

    @Override
    public List<?> next() throws TranslatorException, DataNotAvailableException {
        return this.delegate.next();
    }

    @Override
    public void cancel() throws TranslatorException {
        if (this.delegate != null) {
            this.delegate.cancel();
        }
    }

    @Override
    public void close() {
        if (this.delegate != null) {
            this.delegate.close();
        }
    }

    // testing
    LDAPQueryExecution getDelegate() {
        return this.delegate;
    }
}
