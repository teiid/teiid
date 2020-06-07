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

import org.teiid.language.Command;
import org.teiid.language.Delete;
import org.teiid.language.Insert;
import org.teiid.language.Update;
import org.teiid.logging.LogConstants;
import org.teiid.logging.LogManager;
import org.teiid.metadata.RuntimeMetadata;
import org.teiid.translator.ExecutionContext;
import org.teiid.translator.TranslatorException;
import org.teiid.translator.google.api.GoogleSpreadsheetConnection;
import org.teiid.translator.google.api.metadata.SpreadsheetInfo;
import org.teiid.translator.google.api.result.UpdateResult;
import org.teiid.translator.google.visitor.SpreadsheetDeleteVisitor;
import org.teiid.translator.google.visitor.SpreadsheetInsertVisitor;
import org.teiid.translator.google.visitor.SpreadsheetUpdateVisitor;
/**
 * Execution of INSERT, DELETE and UPDATE commands
 *
 * @author felias
 *
 */
public class SpreadsheetUpdateExecution extends AbstractSpreadsheetExecution {

    public SpreadsheetUpdateExecution(Command command, GoogleSpreadsheetConnection connection, ExecutionContext context, RuntimeMetadata metadata) {
        super(command, connection, context, metadata);
        this.command = command;
        this.connection = connection;
    }

    @Override
    public void cancel() throws TranslatorException {
        LogManager.logDetail(LogConstants.CTX_CONNECTOR, SpreadsheetExecutionFactory.UTIL.getString("cancel_query")); //$NON-NLS-1$
    }

    @Override
    public void execute() throws TranslatorException {

        if (command instanceof org.teiid.language.Delete) {
            result = executeDelete();
        } else if (command instanceof org.teiid.language.Insert) {
            result = executeInsert();
        } else if (command instanceof org.teiid.language.Update) {
            result = executeUpdate();
        }
    }

    private UpdateResult executeUpdate() throws TranslatorException {
        SpreadsheetInfo info = connection.getSpreadsheetInfo();
        SpreadsheetUpdateVisitor updateVisitor = new SpreadsheetUpdateVisitor(info);
        updateVisitor.visit((Update) command);
        checkHeaders(updateVisitor.getWorksheet());
        result = connection.updateRows(updateVisitor.getWorksheet(), updateVisitor.getCriteriaQuery(), updateVisitor.getChanges());
        return result;
    }

    private UpdateResult executeInsert() throws TranslatorException {
        SpreadsheetInfo info = connection.getSpreadsheetInfo();
        SpreadsheetInsertVisitor visitor = new SpreadsheetInsertVisitor(info);
        visitor.visit((Insert) command);
        checkHeaders(visitor.getWorksheet());
        result = connection.executeRowInsert(visitor.getWorksheet(), visitor.getColumnNameValuePair());
        return result;
    }

    private UpdateResult executeDelete() throws TranslatorException {
        SpreadsheetInfo info = connection.getSpreadsheetInfo();
        SpreadsheetDeleteVisitor visitor = new SpreadsheetDeleteVisitor(info);
        visitor.visit((Delete) command);
        checkHeaders(visitor.getWorksheet());
        result = connection.deleteRows(visitor.getWorksheet(), visitor.getCriteriaQuery());
        return result;
    }
}
