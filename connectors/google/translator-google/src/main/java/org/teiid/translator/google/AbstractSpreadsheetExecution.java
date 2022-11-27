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
import org.teiid.metadata.RuntimeMetadata;
import org.teiid.translator.DataNotAvailableException;
import org.teiid.translator.ExecutionContext;
import org.teiid.translator.TranslatorException;
import org.teiid.translator.UpdateExecution;
import org.teiid.translator.google.api.GoogleSpreadsheetConnection;
import org.teiid.translator.google.api.SpreadsheetOperationException;
import org.teiid.translator.google.api.metadata.Worksheet;
import org.teiid.translator.google.api.result.UpdateResult;

public abstract class AbstractSpreadsheetExecution implements UpdateExecution {
    protected GoogleSpreadsheetConnection connection;
    protected RuntimeMetadata metadata;
    protected ExecutionContext context;
    protected Command command;
    protected UpdateResult result;

    public AbstractSpreadsheetExecution(Command command, GoogleSpreadsheetConnection connection, ExecutionContext context,RuntimeMetadata metadata) {
        super();
        this.connection = connection;
        this.metadata = metadata;
        this.context = context;
        this.command = command;
    }

    @Override
    public void close() {
    }

    @Override
    public void cancel() throws TranslatorException {
    }

    @Override
    public int[] getUpdateCounts() throws DataNotAvailableException, TranslatorException {
        if (result.getExpectedNumberOfRows() != result.getActualNumberOfRows()) {
            if (result.getExpectedNumberOfRows() > result.getActualNumberOfRows()) {
                context.addWarning(new SpreadsheetOperationException(SpreadsheetExecutionFactory.UTIL.gs("partial_update", result.getExpectedNumberOfRows(), result.getActualNumberOfRows()))); //$NON-NLS-1$
            } else {
                throw new SpreadsheetOperationException(SpreadsheetExecutionFactory.UTIL.gs("unexpected_updatecount", result.getExpectedNumberOfRows(), result.getActualNumberOfRows())); //$NON-NLS-1$
            }
        }
        return new int[]{result.getActualNumberOfRows()};
    }

     void checkHeaders(Worksheet worksheet) throws TranslatorException{
        if(!worksheet.isHeaderEnabled()){
            throw new TranslatorException(SpreadsheetExecutionFactory.UTIL.gs("headers_required")); //$NON-NLS-1$
        }
    }

    public GoogleSpreadsheetConnection getConnection(){
        return connection;
    }
}
