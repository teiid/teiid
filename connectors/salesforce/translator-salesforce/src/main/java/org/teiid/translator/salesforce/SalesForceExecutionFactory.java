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

package org.teiid.translator.salesforce;

import static org.teiid.translator.TypeFacility.RUNTIME_NAMES.BOOLEAN;
import static org.teiid.translator.TypeFacility.RUNTIME_NAMES.STRING;

import java.util.Arrays;
import java.util.List;

import org.teiid.language.Argument;
import org.teiid.language.Call;
import org.teiid.language.Command;
import org.teiid.language.QueryExpression;
import org.teiid.language.visitor.SQLStringVisitor;
import org.teiid.logging.LogConstants;
import org.teiid.logging.LogManager;
import org.teiid.metadata.Procedure;
import org.teiid.metadata.RuntimeMetadata;
import org.teiid.resource.api.ConnectionFactory;
import org.teiid.translator.ExecutionContext;
import org.teiid.translator.ExecutionFactory;
import org.teiid.translator.MetadataProcessor;
import org.teiid.translator.ProcedureExecution;
import org.teiid.translator.ResultSetExecution;
import org.teiid.translator.Translator;
import org.teiid.translator.TranslatorException;
import org.teiid.translator.TranslatorProperty;
import org.teiid.translator.UpdateExecution;
import org.teiid.translator.salesforce.execution.DeleteExecutionImpl;
import org.teiid.translator.salesforce.execution.DirectQueryExecution;
import org.teiid.translator.salesforce.execution.InsertExecutionImpl;
import org.teiid.translator.salesforce.execution.ProcedureExecutionParentImpl;
import org.teiid.translator.salesforce.execution.QueryExecutionImpl;
import org.teiid.translator.salesforce.execution.UpdateExecutionImpl;

import com.sforce.soap.partner.QueryResult;
import com.sforce.soap.partner.sobject.SObject;

@Translator(name="salesforce", description="A translator for Salesforce")
public class SalesForceExecutionFactory extends ExecutionFactory<ConnectionFactory, SalesforceConnection> {
    public static final int MAX_RETRIEVE = 2000;
    private static final String SALESFORCE = "salesforce"; //$NON-NLS-1$
    private static final String EXCLUDES = "excludes";//$NON-NLS-1$
    private static final String INCLUDES = "includes";//$NON-NLS-1$
    private int maxInsertBatchSize = 2048;
    private boolean supportsGroupBy = true;
    private boolean bulk;
    private boolean hardDelete;
    private int maxFromGroups = 2;

    public SalesForceExecutionFactory() {
        /*there are multiple issues:
           - only one item is supported, which would require a new capability
           - it's case insensitive (which is an inconsistent case for us at best)
           - many types are not comparable or have other special rules, which we try to account for in the
             the metadata processors, but it's not complete
          basic support was added in case a user wants to turn this on though
        */
        setSupportsOrderBy(false);
        setSupportsOuterJoins(true);
        setSupportsInnerJoins(true);
        setTransactionSupport(TransactionSupport.NONE);
        setSupportedJoinCriteria(SupportedJoinCriteria.KEY);
        setMaxDependentInPredicates(1);
        setMaxInCriteriaSize(MAX_RETRIEVE); //retrieve limit
    }

    @Override
    public void start() throws TranslatorException {
        super.start();
        addPushDownFunction(SALESFORCE, INCLUDES, BOOLEAN, STRING, STRING);
        addPushDownFunction(SALESFORCE, EXCLUDES, BOOLEAN, STRING, STRING);
        LogManager.logTrace(LogConstants.CTX_CONNECTOR, "Salesforce ExecutionFactory Started"); //$NON-NLS-1$
    }


    @Override
    public ResultSetExecution createResultSetExecution(QueryExpression command, ExecutionContext executionContext, RuntimeMetadata metadata, SalesforceConnection connection)
            throws TranslatorException {
        return new QueryExecutionImpl(command, connection, metadata, executionContext, this);
    }

    @Override
    public UpdateExecution createUpdateExecution(Command command, ExecutionContext executionContext, RuntimeMetadata metadata, SalesforceConnection connection) throws TranslatorException {
        UpdateExecution result = null;
        if(command instanceof org.teiid.language.Delete) {
            result = new DeleteExecutionImpl(this, command, connection, metadata, executionContext);
        } else if (command instanceof org.teiid.language.Insert) {
            result = new InsertExecutionImpl(this, command, connection, metadata, executionContext);
        } else if (command instanceof org.teiid.language.Update) {
            result = new UpdateExecutionImpl(this, command, connection, metadata, executionContext);
        }
        return result;

    }

    @Override
    public ProcedureExecution createProcedureExecution(Call command,ExecutionContext executionContext, RuntimeMetadata metadata, SalesforceConnection connection)
            throws TranslatorException {
        Procedure metadataObject = command.getMetadataObject();
        String nativeQuery = metadataObject.getProperty(SQLStringVisitor.TEIID_NATIVE_QUERY, false);
        if (nativeQuery != null) {
            return new DirectQueryExecution(command.getArguments(), command, connection, metadata, executionContext, nativeQuery, false);
        }
        return new ProcedureExecutionParentImpl(command, connection, metadata, executionContext);
    }

    @Override
    public ProcedureExecution createDirectExecution(List<Argument> arguments, Command command, ExecutionContext executionContext, RuntimeMetadata metadata, SalesforceConnection connection) throws TranslatorException {
         return new DirectQueryExecution(arguments.subList(1, arguments.size()), command, connection, metadata, executionContext, (String)arguments.get(0).getArgumentValue().getValue(), true);
    }

    @Override
    public MetadataProcessor<SalesforceConnection> getMetadataProcessor(){
        return new SalesForceMetadataProcessor();
    }

    @Override
    public List<String> getSupportedFunctions() {
        return Arrays.asList(INCLUDES, EXCLUDES);
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
    public boolean supportsRowLimit() {
        return true;
    }

    @Override
    public boolean supportsAggregatesCountStar() {
        return true;
    }

    @Override
    public boolean supportsAggregatesCount() {
        return true;
    }

    @Override
    public boolean supportsAggregatesMax() {
        return true;
    }

    @Override
    public boolean supportsAggregatesMin() {
        return true;
    }

    @Override
    public boolean supportsAggregatesSum() {
        return true;
    }

    @Override
    public boolean supportsAggregatesAvg() {
        return true;
    }

    @TranslatorProperty(display="Supports Group By", description="Defaults to true. Set to false to have Teiid process group by aggregations, such as those returning more than 2000 rows which error in SOQL", advanced=true)
    @Override
    public boolean supportsGroupBy() {
        return this.supportsGroupBy ;
    }

    @Override
    public boolean supportsOnlySingleTableGroupBy() {
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
    public boolean supportsCompareCriteriaOrdered() {
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
    public boolean supportsHaving() {
        return true;
    }

    @Override
    public boolean supportsOnlyRelationshipStyleJoins() {
        return true;
    }

    @TranslatorProperty(display="Max From Clause Groups", description="The number of Salesforce tables to join. Defaults to 2.  Set to a larger number to pushdown more joins.", advanced=true)
    public int getMaxFromGroups() {
        return maxFromGroups;
    }

    public void setMaxFromGroups(int maxFromGroups) {
        this.maxFromGroups = maxFromGroups;
    }

    @Override
    public boolean useAnsiJoin() {
        return true;
    }

    @Override
    public boolean supportsBulkUpdate() {
        return true;
    }

    @TranslatorProperty(display="Max Bulk Insert Batch Size", description="The max size of a bulk insert batch.  Default 2048.", advanced=true)
    public int getMaxBulkInsertBatchSize() {
        return maxInsertBatchSize;
    }

    public void setMaxBulkInsertBatchSize(int maxInsertBatchSize) {
        if (maxInsertBatchSize < 1) {
            throw new AssertionError("Max bulk insert batch size must be greater than 0"); //$NON-NLS-1$
        }
        this.maxInsertBatchSize = maxInsertBatchSize;
    }

    public void setSupportsGroupBy(boolean supportsGroupBy) {
        this.supportsGroupBy = supportsGroupBy;
    }

    public QueryResult buildQueryResult(SObject[] objects) {
        QueryResult result = new QueryResult();
        result.setRecords(objects);
        result.setSize(objects.length);
        result.setDone(true);
        return result;
    }

    @Override
    public boolean supportsUpsert() {
        return true;
    }

    @Override
    public boolean supportsSelfJoins() {
        return true;
    }

    @Override
    public boolean supportsAliasedTable() {
        return true;
    }

    @TranslatorProperty(display="Use Bulk By Default", description="Defaults to false. Set to true to use bulk operations when possible without requiring a bulk source hint.", advanced=true)
    public boolean useBulk() {
        return this.bulk;
    }

    @TranslatorProperty(display="Use Hard Delete", description="Defaults to false. Set to true to use hardDelete by default with a bulk delete.", advanced=true)
    public boolean useHardDelete() {
        return this.hardDelete ;
    }

    public void setUseBulk(boolean bulk) {
        this.bulk = bulk;
    }

    public void setUseHardDelete(boolean hardDelete) {
        this.hardDelete = hardDelete;
    }

    @Override
    public boolean supportsOrderByNullOrdering() {
        return true;
    }

    @Override
    public NullOrder getDefaultNullOrder() {
        return NullOrder.FIRST;
    }

}
