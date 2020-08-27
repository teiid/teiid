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
package org.teiid.translator.salesforce.execution;

import java.text.SimpleDateFormat;

import org.teiid.core.types.DataTypeManager;
import org.teiid.language.Command;
import org.teiid.metadata.RuntimeMetadata;
import org.teiid.translator.DataNotAvailableException;
import org.teiid.translator.ExecutionContext;
import org.teiid.translator.TranslatorException;
import org.teiid.translator.UpdateExecution;
import org.teiid.translator.salesforce.SalesForceExecutionFactory;
import org.teiid.translator.salesforce.SalesforceConnection;

/**
 *
 * Parent class to the Update, Delete, and Insert execution classes.
 * Provisions the correct impl and contains some common code to
 * get IDs of Salesforce objects.
 *
 */
public abstract class AbstractUpdateExecution implements UpdateExecution {

    private SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSSZ"); //$NON-NLS-1$

    protected SalesForceExecutionFactory executionFactory;
    protected SalesforceConnection connection;
    protected RuntimeMetadata metadata;
    protected ExecutionContext context;
    protected Command command;
    protected int result;

    public AbstractUpdateExecution(SalesForceExecutionFactory ef, Command command,
            SalesforceConnection salesforceConnection,
            RuntimeMetadata metadata, ExecutionContext context) {
        this.executionFactory = ef;
        this.connection = salesforceConnection;
        this.metadata = metadata;
        this.context = context;
        this.command = command;
    }

    @Override
    public void cancel() throws TranslatorException {
    }

    @Override
    public void close() {
    }

    @Override
    public int[] getUpdateCounts() throws DataNotAvailableException,
            TranslatorException {
        return new int[] {result};
    }

    public RuntimeMetadata getMetadata() {
        return metadata;
    }

    public SalesforceConnection getConnection() {
        return connection;
    }

    String getStringValue(Object val, Class<?> type) {
        if (val == null) {
            return null;
        }
        if (type.equals(DataTypeManager.DefaultDataClasses.TIMESTAMP)) {
            return sdf.format(val);
        }
        return val.toString();
    }

}
