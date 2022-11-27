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
package org.teiid.translator.accumulo;

import org.teiid.resource.api.ConnectionFactory;

import org.teiid.language.Command;
import org.teiid.language.QueryExpression;
import org.teiid.language.Select;
import org.teiid.metadata.RuntimeMetadata;
import org.teiid.translator.ExecutionContext;
import org.teiid.translator.ExecutionFactory;
import org.teiid.translator.MetadataProcessor;
import org.teiid.translator.ResultSetExecution;
import org.teiid.translator.Translator;
import org.teiid.translator.TranslatorException;
import org.teiid.translator.TranslatorProperty;
import org.teiid.translator.UpdateExecution;

@Translator(name="accumulo", description="Accumulo Translator, reads and writes the data to Accumulo Key/Value store")
public class AccumuloExecutionFactory extends ExecutionFactory<ConnectionFactory, AccumuloConnection> {
    private int queryThreadsCount = 10;

    public AccumuloExecutionFactory() {
        setTransactionSupport(TransactionSupport.NONE);
        setSourceRequiredForMetadata(true);
    }

    @Override
    public void start() throws TranslatorException {
        super.start();
    }

    @TranslatorProperty(display="Execution Query Threads", description="Number of threads to use on Accumulo for Query", advanced=true)
    public int getQueryThreadsCount() {
        return queryThreadsCount;
    }

    public void setQueryThreadsCount(int queryThreadsCount) {
        this.queryThreadsCount = queryThreadsCount;
    }

    @Override
    public ResultSetExecution createResultSetExecution(QueryExpression command,
            ExecutionContext executionContext, RuntimeMetadata metadata,
            AccumuloConnection connection) throws TranslatorException {
        return new AccumuloQueryExecution(this, (Select) command, executionContext, metadata, connection);
    }

    @Override
    public UpdateExecution createUpdateExecution(Command command, ExecutionContext executionContext, RuntimeMetadata metadata, AccumuloConnection connection) throws TranslatorException {
        return new AccumuloUpdateExecution(this, command, executionContext, metadata, connection);
    }

    @Override
    public MetadataProcessor<AccumuloConnection> getMetadataProcessor() {
        return new AccumuloMetadataProcessor();
    }

    @Override
    public boolean supportsAggregatesCountStar() {
        return true;
    }

    @Override
    public boolean supportsCompareCriteriaEquals() {
        return true;
    }

    @Override
    public boolean supportsNotCriteria() {
        return true;
    }

    @Override
    public boolean supportsCompareCriteriaOrdered() {
        return true;
    }

    @Override
    public boolean supportsInCriteria() {
        return true;
    }

    @Override
    public boolean supportsOnlyLiteralComparison() {
        return true;
    }

    @Override
    public boolean supportsIsNullCriteria() {
        return false;
    }

    @Override
    public boolean supportsOrCriteria() {
        return true;
    }

    @Override
    public boolean supportsBulkUpdate() {
        return true;
    }

    public boolean returnsSingleUpdateCount() {
        return true;
    }
}
