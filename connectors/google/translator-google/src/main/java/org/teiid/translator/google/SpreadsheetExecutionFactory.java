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

package org.teiid.translator.google;

import java.util.Arrays;
import java.util.List;

import org.teiid.resource.api.ConnectionFactory;

import org.teiid.core.BundleUtil;
import org.teiid.language.Argument;
import org.teiid.language.Call;
import org.teiid.language.Command;
import org.teiid.language.QueryExpression;
import org.teiid.language.Select;
import org.teiid.language.visitor.SQLStringVisitor;
import org.teiid.logging.LogConstants;
import org.teiid.logging.LogManager;
import org.teiid.metadata.RuntimeMetadata;
import org.teiid.translator.ExecutionContext;
import org.teiid.translator.ExecutionFactory;
import org.teiid.translator.MetadataProcessor;
import org.teiid.translator.ProcedureExecution;
import org.teiid.translator.ResultSetExecution;
import org.teiid.translator.SourceSystemFunctions;
import org.teiid.translator.Translator;
import org.teiid.translator.TranslatorException;
import org.teiid.translator.UpdateExecution;
import org.teiid.translator.google.api.GoogleSpreadsheetConnection;

/**
 * Translator that is used to translate SQL to Google spreadsheet API. Translator uses Google Visualization API and Google Data API.
 *
 * @author felias
 *
 */
@Translator(name="google-spreadsheet", description="A translator for Google Spreadsheet")
public class SpreadsheetExecutionFactory extends ExecutionFactory<ConnectionFactory, GoogleSpreadsheetConnection>{
    public static final BundleUtil UTIL = BundleUtil.getBundleUtil(SpreadsheetExecutionFactory.class);

    public SpreadsheetExecutionFactory() {
        setTransactionSupport(TransactionSupport.NONE);
    }

    @Override
    public void start() throws TranslatorException {
        super.start();
        LogManager.logTrace(LogConstants.CTX_CONNECTOR, "Google Spreadsheet ExecutionFactory Started"); //$NON-NLS-1$
    }

    @Override
    public ResultSetExecution createResultSetExecution(QueryExpression command, ExecutionContext executionContext, RuntimeMetadata metadata, GoogleSpreadsheetConnection connection)
            throws TranslatorException {
        return new SpreadsheetQueryExecution((Select)command, connection, executionContext);
    }

    @Override
    public UpdateExecution createUpdateExecution(Command command, ExecutionContext executionContext, RuntimeMetadata metadata, GoogleSpreadsheetConnection connection) throws TranslatorException {
        UpdateExecution result = new SpreadsheetUpdateExecution(command, connection, executionContext, metadata);
        return result;
    }

    @Override
    public ProcedureExecution createDirectExecution(List<Argument> arguments, Command command, ExecutionContext executionContext, RuntimeMetadata metadata, GoogleSpreadsheetConnection connection) throws TranslatorException {
         return new DirectSpreadsheetQueryExecution((String)arguments.get(0).getArgumentValue().getValue(), arguments.subList(1, arguments.size()), executionContext, connection, true);
    }

    @Override
    public ProcedureExecution createProcedureExecution(Call command,
            ExecutionContext executionContext, RuntimeMetadata metadata,
            GoogleSpreadsheetConnection connection) throws TranslatorException {
        String nativeQuery = command.getMetadataObject().getProperty(SQLStringVisitor.TEIID_NATIVE_QUERY, false);
        if (nativeQuery != null) {
            return new DirectSpreadsheetQueryExecution(nativeQuery, command.getArguments(), executionContext, connection, false);
        }
        throw new TranslatorException("Missing native-query extension metadata."); //$NON-NLS-1$
    }

    @Override
    public MetadataProcessor<GoogleSpreadsheetConnection> getMetadataProcessor(){
        return new GoogleMetadataProcessor();
    }

    @Override
    public boolean supportsCompareCriteriaEquals() {
        return true;
    }

    @Override
    public boolean supportsInCriteria() {
        return false;
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
    public boolean supportsNotCriteria() {
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

    @Override
    public boolean supportsGroupBy() {
        return true;
    }

    @Override
    public boolean supportsOrderBy() {
        return false;
    }

    @Override
    public boolean supportsHaving() {
        return false;
    }

    @Override
    public boolean supportsCompareCriteriaOrdered() {
        return true;
    }

    @Override
    public boolean supportsRowLimit() {
        return true;
    }

    @Override
    public boolean supportsRowOffset() {
        return true;
    }

    @Override
    public List<String> getSupportedFunctions() {
        return Arrays.asList(SourceSystemFunctions.YEAR,
                SourceSystemFunctions.MONTH, SourceSystemFunctions.DAYOFMONTH,
                SourceSystemFunctions.HOUR, SourceSystemFunctions.MINUTE,
                SourceSystemFunctions.SECOND, SourceSystemFunctions.QUARTER,
                SourceSystemFunctions.DAYOFWEEK, SourceSystemFunctions.UCASE,
                SourceSystemFunctions.LCASE);
    }

}
