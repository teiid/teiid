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
