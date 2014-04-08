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

package org.teiid.translator.simpledb;

import java.util.List;

import javax.resource.cci.ConnectionFactory;

import org.teiid.language.*;
import org.teiid.metadata.RuntimeMetadata;
import org.teiid.resource.adpter.simpledb.SimpleDBConnection;
import org.teiid.translator.*;

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
        return true;
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
}
