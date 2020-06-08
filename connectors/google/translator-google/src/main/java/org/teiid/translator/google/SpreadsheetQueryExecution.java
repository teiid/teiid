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

import java.util.Iterator;
import java.util.List;

import org.teiid.language.Select;
import org.teiid.logging.LogConstants;
import org.teiid.logging.LogManager;
import org.teiid.translator.DataNotAvailableException;
import org.teiid.translator.ExecutionContext;
import org.teiid.translator.ResultSetExecution;
import org.teiid.translator.TranslatorException;
import org.teiid.translator.google.api.GoogleSpreadsheetConnection;
import org.teiid.translator.google.api.result.SheetRow;
import org.teiid.translator.google.visitor.SpreadsheetSQLVisitor;

/**
 * Execution of SELECT Command
 *
 * @author felias
 *
 */
public class SpreadsheetQueryExecution implements ResultSetExecution {

    private Select query;
    private GoogleSpreadsheetConnection connection;
    private Iterator<SheetRow> rowIterator;
    private ExecutionContext executionContext;

    public SpreadsheetQueryExecution(Select query,
            GoogleSpreadsheetConnection connection, ExecutionContext executionContext) {
        this.executionContext = executionContext;
        this.connection = connection;
        this.query = query;
    }

    @Override
    public void close() {
        LogManager.logDetail(LogConstants.CTX_CONNECTOR, SpreadsheetExecutionFactory.UTIL.getString("close_query")); //$NON-NLS-1$
    }

    @Override
    public void cancel() throws TranslatorException {
        LogManager.logDetail(LogConstants.CTX_CONNECTOR, SpreadsheetExecutionFactory.UTIL.getString("cancel_query")); //$NON-NLS-1$

    }

    @Override
    public void execute() throws TranslatorException {
        SpreadsheetSQLVisitor visitor = new SpreadsheetSQLVisitor(connection.getSpreadsheetInfo());
        visitor.translateSQL(query);
        rowIterator = connection.executeQuery(visitor.getWorksheet(), visitor.getTranslatedSQL(), visitor.getOffsetValue(),visitor.getLimitValue(), executionContext.getBatchSize()).iterator();

    }

    @Override
    public List<?> next() throws TranslatorException, DataNotAvailableException {
        if (rowIterator.hasNext()) {
            return rowIterator.next().getRow();
        }
        return null;
    }

}
