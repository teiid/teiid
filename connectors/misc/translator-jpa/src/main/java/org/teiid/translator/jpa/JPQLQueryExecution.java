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
package org.teiid.translator.jpa;

import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

import javax.persistence.EntityManager;
import javax.persistence.Query;

import org.teiid.language.Limit;
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
        handleLimit(this.command, query);
        List results = query.getResultList();
        this.resultsIterator = results.iterator();
    }

    /**
     * If the query specifies a Limit, apply that to the query as firstResult
     * and maxResults.
     *
     * @param command the teiid query to be executed
     * @param query the JPA query to be executed
     */
    private void handleLimit(QueryExpression command, Query query) {
        Limit limit = command.getLimit();
        if (limit == null) {
            return;
        }
        query.setFirstResult(limit.getRowOffset())
                .setMaxResults(limit.getRowLimit());
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
