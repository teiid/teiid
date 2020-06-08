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
import java.util.Iterator;
import java.util.List;

import org.teiid.core.util.StringUtil;
import org.teiid.language.Argument;
import org.teiid.language.Literal;
import org.teiid.language.visitor.SQLStringVisitor;
import org.teiid.logging.LogConstants;
import org.teiid.logging.LogManager;
import org.teiid.translator.DataNotAvailableException;
import org.teiid.translator.ExecutionContext;
import org.teiid.translator.ProcedureExecution;
import org.teiid.translator.TranslatorException;
import org.teiid.translator.google.api.GoogleSpreadsheetConnection;
import org.teiid.translator.google.api.SpreadsheetOperationException;
import org.teiid.translator.google.api.metadata.Worksheet;
import org.teiid.translator.google.api.result.SheetRow;
import org.teiid.translator.google.visitor.SpreadsheetSQLVisitor;

public class DirectSpreadsheetQueryExecution implements ProcedureExecution {
    private static final String WORKSHEET = "worksheet"; //$NON-NLS-1$
    private static final String QUERY = "query"; //$NON-NLS-1$
    private static final String OFFEST = "offset"; //$NON-NLS-1$
    private static final String LIMIT = "limit"; //$NON-NLS-1$

    private GoogleSpreadsheetConnection connection;
    private Iterator<SheetRow> rowIterator;
    private ExecutionContext executionContext;
    private List<Argument> arguments;

    private String query;
    private boolean returnsArray;

    public DirectSpreadsheetQueryExecution(String query, List<Argument> arguments, ExecutionContext executionContext, GoogleSpreadsheetConnection connection, boolean returnsArray) {
        this.executionContext = executionContext;
        this.connection = connection;
        this.arguments = arguments;
        this.query = query;
        this.returnsArray = returnsArray;
    }

    @Override
    public void close() {
        LogManager.logDetail(LogConstants.CTX_CONNECTOR, SpreadsheetExecutionFactory.UTIL.getString("close_query")); //$NON-NLS-1$
    }

    @Override
    public void cancel() throws TranslatorException {
        LogManager.logDetail(LogConstants.CTX_CONNECTOR, SpreadsheetExecutionFactory.UTIL.getString("cancel_query")); //$NON-NLS-1$
        this.rowIterator = null;
    }

    @Override
    public void execute() throws TranslatorException {
        String worksheetName = null;
        Integer limit = null;
        Integer offset = null;
        String toQuery = query;

        List<String> parts = StringUtil.tokenize(query, ';');
        for (String var : parts) {
            int index = var.indexOf('=');
            if (index == -1) {
                continue;
            }
            String key = var.substring(0, index).trim();
            String value = var.substring(index+1).trim();

            if (key.equalsIgnoreCase(WORKSHEET)) {
                worksheetName = value;
            }
            else if (key.equalsIgnoreCase(QUERY)) {
                StringBuilder buffer = new StringBuilder();
                SQLStringVisitor.parseNativeQueryParts(value, arguments, buffer, new SQLStringVisitor.Substitutor() {

                    @Override
                    public void substitute(Argument arg, StringBuilder builder, int index) {
                        Literal argumentValue = arg.getArgumentValue();
                        SpreadsheetSQLVisitor visitor = new SpreadsheetSQLVisitor(connection.getSpreadsheetInfo());
                        visitor.visit(argumentValue);
                        builder.append(visitor.getTranslatedSQL());
                    }
                });
                toQuery = buffer.toString();
            }
            else if (key.equalsIgnoreCase(LIMIT)) {
                limit = Integer.parseInt(value);
            }
            else if (key.equalsIgnoreCase(OFFEST)) {
                offset = Integer.parseInt(value);
            }
        }

        Worksheet worksheet = this.connection.getSpreadsheetInfo().getWorksheetByName(worksheetName);
        if(worksheet==null){
            throw new SpreadsheetOperationException(SpreadsheetExecutionFactory.UTIL.gs("missing_worksheet", worksheetName)); //$NON-NLS-1$
        }

        this.rowIterator = this.connection.executeQuery(worksheet, toQuery, offset, limit, executionContext.getBatchSize()).iterator();
    }

    @Override
    public List<?> next() throws TranslatorException, DataNotAvailableException {
        if (this.rowIterator != null && this.rowIterator.hasNext()) {
            List<?> result = rowIterator.next().getRow();
            if (returnsArray) {
                return Arrays.asList((Object)result.toArray());
            }
            return result;
        }
        this.rowIterator = null;
        return null;
    }

    @Override
    public List<?> getOutputParameterValues() throws TranslatorException {
        return null;
    }
}
