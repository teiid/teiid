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

package org.teiid.translator.cassandra;

import java.util.List;

import org.teiid.resource.api.ConnectionFactory;

import org.teiid.core.BundleUtil;
import org.teiid.language.Argument;
import org.teiid.language.Call;
import org.teiid.language.Command;
import org.teiid.language.QueryExpression;
import org.teiid.language.visitor.SQLStringVisitor;
import org.teiid.logging.LogConstants;
import org.teiid.logging.LogManager;
import org.teiid.metadata.RuntimeMetadata;
import org.teiid.translator.ExecutionContext;
import org.teiid.translator.ExecutionFactory;
import org.teiid.translator.MetadataProcessor;
import org.teiid.translator.ProcedureExecution;
import org.teiid.translator.ResultSetExecution;
import org.teiid.translator.Translator;
import org.teiid.translator.TranslatorException;
import org.teiid.translator.UpdateExecution;

import com.datastax.driver.core.VersionNumber;


@Translator(name = "cassandra", description = "A translator for Cassandra NoSql database")
public class CassandraExecutionFactory extends ExecutionFactory<ConnectionFactory, CassandraConnection> {
    private static final VersionNumber DEFAULT_VERSION = VersionNumber.parse("1.2.0"); //$NON-NLS-1$
    private static final VersionNumber V2 = VersionNumber.parse("2.0.0"); //$NON-NLS-1$
    private static final VersionNumber V2_2 = VersionNumber.parse("2.2.0"); //$NON-NLS-1$
    public static final BundleUtil UTIL = BundleUtil.getBundleUtil(CassandraExecutionFactory.class);

    public static enum Event implements BundleUtil.Event {
        TEIID22000
    }

    private VersionNumber version;

    @Override
    public void start() throws TranslatorException {
        super.start();
        setTransactionSupport(TransactionSupport.NONE);
        LogManager.logTrace(LogConstants.CTX_CONNECTOR, "Cassandra ExecutionFactory Started"); //$NON-NLS-1$
    }

    @Override
    public ResultSetExecution createResultSetExecution(QueryExpression command,
            ExecutionContext executionContext, RuntimeMetadata metadata,
            CassandraConnection connection) throws TranslatorException {
        return new CassandraQueryExecution(command, connection, executionContext);
    }

    @Override
    public UpdateExecution createUpdateExecution(Command command,
            ExecutionContext executionContext, RuntimeMetadata metadata,
            CassandraConnection connection) throws TranslatorException {
        return new CassandraUpdateExecution(command, executionContext, metadata, connection);
    }

    @Override
    public ProcedureExecution createProcedureExecution(Call command,
            ExecutionContext executionContext, RuntimeMetadata metadata,
            CassandraConnection connection) throws TranslatorException {
        String nativeQuery = command.getMetadataObject().getProperty(SQLStringVisitor.TEIID_NATIVE_QUERY, false);
        if (nativeQuery != null) {
            return new CassandraDirectQueryExecution(nativeQuery, command.getArguments(), command, connection, executionContext, false);
        }
        throw new TranslatorException("Missing native-query extension metadata."); //$NON-NLS-1$
    }

    @Override
    public ProcedureExecution createDirectExecution(List<Argument> arguments,
            Command command, ExecutionContext executionContext,
            RuntimeMetadata metadata, CassandraConnection connection)
            throws TranslatorException {
        return new CassandraDirectQueryExecution((String) arguments.get(0).getArgumentValue().getValue(), arguments.subList(1, arguments.size()), command, connection, executionContext, true);
    }

    @Override
    public MetadataProcessor<CassandraConnection> getMetadataProcessor(){
        return new CassandraMetadataProcessor();
    }

    @Override
    public boolean supportsOrderBy() {
        // Order by is allowed in very restrictive case when this is used as
        // compound primary key's second column where it is defined partioned key
        return false;
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
    public boolean supportsCompareCriteriaOrdered() {
        return true;
    }

    @Override
    public boolean supportsInCriteria() {
        return true;
    }

    @Override
    public boolean supportsRowLimit() {
        return true;
    }

    @Override
    public boolean supportsBulkUpdate() {
        return version.compareTo(V2) >= 0;
    }

    @Override
    public boolean supportsBatchedUpdates() {
        return version.compareTo(V2) >= 0;
    }

    @Override
    public boolean supportsAggregatesSum() {
        return version.compareTo(V2_2) >= 0;
    }

    @Override
    public boolean supportsAggregatesAvg() {
        return version.compareTo(V2_2) >= 0;
    }

    @Override
    public boolean supportsAggregatesMin() {
        return version.compareTo(V2_2) >= 0;
    }

    @Override
    public boolean supportsAggregatesMax() {
        return version.compareTo(V2_2) >= 0;
    }

    @Override
    public boolean returnsSingleUpdateCount() {
        return true;
    }

    @Override
    public void initCapabilities(CassandraConnection connection)
            throws TranslatorException {
        if (connection == null) {
            return;
        }
        this.version = connection.getVersion();
        if (this.version == null) {
             this.version = DEFAULT_VERSION;
        }
    }

    @Override
    public boolean isSourceRequiredForCapabilities() {
        return true;
    }

}
