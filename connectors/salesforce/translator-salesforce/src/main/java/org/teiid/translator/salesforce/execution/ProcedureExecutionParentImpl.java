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

import java.util.List;

import org.teiid.core.TeiidRuntimeException;
import org.teiid.language.Call;
import org.teiid.metadata.RuntimeMetadata;
import org.teiid.translator.DataNotAvailableException;
import org.teiid.translator.ExecutionContext;
import org.teiid.translator.ProcedureExecution;
import org.teiid.translator.TranslatorException;
import org.teiid.translator.salesforce.SalesforceConnection;


public class ProcedureExecutionParentImpl implements ProcedureExecution, ProcedureExecutionParent {

    public static final String GET_DELETED = "GetDeleted"; //$NON-NLS-1$
    public static final String GET_UPDATED = "GetUpdated"; //$NON-NLS-1$
    private Call command;
    private ExecutionContext executionContext;
    private RuntimeMetadata metadata;
    private SalesforceProcedureExecution execution;
    private SalesforceConnection connection;

    public ProcedureExecutionParentImpl(Call command,
            SalesforceConnection connection, RuntimeMetadata metadata, ExecutionContext executionContext) {
        this.setCommand(command);
        this.setConnection(connection);
        this.setMetadata(metadata);
        this.setExecutionContext(executionContext);
    }

    @Override
    public List<?> getOutputParameterValues() throws TranslatorException {
        return execution.getOutputParameterValues();
    }

    @Override
    public List<?> next() throws TranslatorException, DataNotAvailableException {
        return execution.next();
    }

    @Override
    public void cancel() throws TranslatorException {
        execution.cancel();
    }

    @Override
    public void close() {
        execution.close();
    }

    @Override
    public void execute() throws TranslatorException {
        String name = getCommand().getMetadataObject().getSourceName();
        if (name == null) {
            name = getCommand().getProcedureName();
        }
        if(GET_UPDATED.equalsIgnoreCase(name)) {
            execution = new GetUpdatedExecutionImpl(this);
        } else if(GET_DELETED.equalsIgnoreCase(name)) {
            execution = new GetDeletedExecutionImpl(this);
        } else {
            throw new TeiidRuntimeException("Unknown procedure " + getCommand().getProcedureName() + " with name in source " + name); //$NON-NLS-1$ //$NON-NLS-2$
        }
        execution.execute(this);
    }

    public void setCommand(Call command) {
        this.command = command;
    }

    public Call getCommand() {
        return command;
    }

    private void setConnection(SalesforceConnection connection) {
        this.connection = connection;
    }

    public SalesforceConnection getConnection() {
        return connection;
    }

    private void setExecutionContext(ExecutionContext executionContext) {
        this.executionContext = executionContext;
    }

    public ExecutionContext getExecutionContext() {
        return executionContext;
    }

    private void setMetadata(RuntimeMetadata metadata) {
        this.metadata = metadata;
    }

    public RuntimeMetadata getMetadata() {
        return metadata;
    }
}
