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

package org.teiid.translator.simpledb;

import java.util.List;

import org.teiid.resource.api.ConnectionFactory;

import org.teiid.language.Argument;
import org.teiid.language.Command;
import org.teiid.language.Delete;
import org.teiid.language.Insert;
import org.teiid.language.QueryExpression;
import org.teiid.language.Select;
import org.teiid.language.Update;
import org.teiid.metadata.RuntimeMetadata;
import org.teiid.translator.ExecutionContext;
import org.teiid.translator.ExecutionFactory;
import org.teiid.translator.MetadataProcessor;
import org.teiid.translator.ProcedureExecution;
import org.teiid.translator.ResultSetExecution;
import org.teiid.translator.Translator;
import org.teiid.translator.TranslatorException;
import org.teiid.translator.TypeFacility;
import org.teiid.translator.UpdateExecution;
import org.teiid.translator.simpledb.api.SimpleDBConnection;

@Translator(name = "simpledb", description = "Translator for Amazon SimpleDB")
public class SimpleDBExecutionFactory extends ExecutionFactory<ConnectionFactory, SimpleDBConnection> {
    public static final String INTERSECTION = "INTERSECTION"; //$NON-NLS-1$
    public static final String ASTRING = "ASTRING"; //$NON-NLS-1$
    public static final String EVERY = "EVERY"; //$NON-NLS-1$
    public static final String SIMPLEDB = "SIMPLEDB"; //$NON-NLS-1$

    public SimpleDBExecutionFactory() {
        setSupportsOrderBy(true);
        setSupportsDirectQueryProcedure(false);
        setSourceRequiredForMetadata(true);
        setTransactionSupport(TransactionSupport.NONE);
    }

    @Override
    public void start() throws TranslatorException {
        super.start();
        addPushDownFunction(SIMPLEDB, EVERY, TypeFacility.RUNTIME_NAMES.STRING, TypeFacility.RUNTIME_NAMES.STRING+"[]"); //$NON-NLS-1$
        addPushDownFunction(SIMPLEDB, INTERSECTION, TypeFacility.RUNTIME_NAMES.BOOLEAN, TypeFacility.RUNTIME_NAMES.STRING+"[]", TypeFacility.RUNTIME_NAMES.STRING, TypeFacility.RUNTIME_NAMES.STRING); //$NON-NLS-1$
        addPushDownFunction(SIMPLEDB, INTERSECTION, TypeFacility.RUNTIME_NAMES.BOOLEAN, TypeFacility.RUNTIME_NAMES.STRING+"[]", TypeFacility.RUNTIME_NAMES.STRING, TypeFacility.RUNTIME_NAMES.STRING, TypeFacility.RUNTIME_NAMES.STRING); //$NON-NLS-1$
        addPushDownFunction(SIMPLEDB, INTERSECTION, TypeFacility.RUNTIME_NAMES.BOOLEAN, TypeFacility.RUNTIME_NAMES.STRING+"[]", TypeFacility.RUNTIME_NAMES.STRING, TypeFacility.RUNTIME_NAMES.STRING, TypeFacility.RUNTIME_NAMES.STRING, TypeFacility.RUNTIME_NAMES.STRING); //$NON-NLS-1$
    }

    @Override
    public UpdateExecution createUpdateExecution(final Command command, ExecutionContext executionContext,
            RuntimeMetadata metadata, final SimpleDBConnection connection) throws TranslatorException {
        if (command instanceof Insert) {
            return new SimpleDBInsertExecute(command, connection);
        } else if (command instanceof Delete) {
            return new SimpleDBDeleteExecute(command, connection);
        } else if (command instanceof Update) {
            return new SimpleDBUpdateExecute(command, connection);
        } else {
            throw new TranslatorException("Just INSERT, DELETE and UPDATE are supported"); //$NON-NLS-1$
        }
    }

    @Override
    public ProcedureExecution createDirectExecution(List<Argument> arguments, Command command, ExecutionContext executionContext, RuntimeMetadata metadata, SimpleDBConnection connection) throws TranslatorException {
        return new SimpleDBDirectQueryExecution(arguments, command, metadata, connection, executionContext);
    }

    @Override
    public ResultSetExecution createResultSetExecution(final QueryExpression command,
            ExecutionContext executionContext, RuntimeMetadata metadata, final SimpleDBConnection connection)
                    throws TranslatorException {
        return new SimpleDBQueryExecution((Select)command, executionContext, metadata, connection);
    }

    @Override
    public MetadataProcessor<SimpleDBConnection> getMetadataProcessor(){
        return new SimpleDBMetadataProcessor();
    }

    @Override
    public boolean supportsCompareCriteriaEquals() {
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
    public boolean supportsIsNullCriteria() {
        return true;
    }

    @Override
    public boolean supportsOnlyLiteralComparison() {
        return true;
    }

    @Override
    public boolean supportsRowLimit() {
        return false; //the simpledb limit clause is a paging hint
    }

    @Override
    public boolean supportsNotCriteria() {
        return true;
    }

    @Override
    public boolean supportsOrCriteria() {
        return true;
    }

    @Override
    public boolean supportsLikeCriteria() {
        return true;
    }

    @Override
    public boolean supportsLikeCriteriaEscapeCharacter() {
        return true;
    }

    @Override
    public boolean supportsAggregatesCountStar() {
        return true;
    }

    @Override
    public boolean supportsArrayType() {
        return true;
    }

    @Override
    public boolean supportsBulkUpdate() {
        return true;
    }

    @Override
    public boolean returnsSingleUpdateCount() {
        return true;
    }
}
