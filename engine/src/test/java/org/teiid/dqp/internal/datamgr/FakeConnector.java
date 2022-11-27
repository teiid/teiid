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

package org.teiid.dqp.internal.datamgr;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.teiid.language.BatchedUpdates;
import org.teiid.language.BulkCommand;
import org.teiid.language.Command;
import org.teiid.metadata.RuntimeMetadata;
import org.teiid.translator.DataNotAvailableException;
import org.teiid.translator.Execution;
import org.teiid.translator.ExecutionContext;
import org.teiid.translator.ExecutionFactory;
import org.teiid.translator.ResultSetExecution;
import org.teiid.translator.TranslatorException;
import org.teiid.translator.UpdateExecution;

public class FakeConnector extends ExecutionFactory<Object, Object> {

    private int connectionCount;
    private int executionCount;
    private int closeCount;
    private boolean returnSingleUpdate;
    private List<Command> commands = new ArrayList<Command>();

    public int getConnectionCount() {
        return connectionCount;
    }

    public int getExecutionCount() {
        return executionCount;
    }

    @Override
    public Execution createExecution(Command command, ExecutionContext executionContext, RuntimeMetadata metadata, Object connection) throws TranslatorException {
        executionCount++;
        commands.add(command);
        FakeExecution result = new FakeExecution(executionContext);
        if (command instanceof BatchedUpdates || (command instanceof BulkCommand && ((BulkCommand)command).getParameterValues() != null)) {
            result.batchOrBulk = true;
        }
        return result;
    }

    @Override
    public Object getConnection(Object factory) throws TranslatorException {
        connectionCount++;
        return factory;
    }

    @Override
    public void closeConnection(Object connection, Object factory) {
        closeCount++;
    }

    public int getCloseCount() {
        return closeCount;
    }

    public final class FakeExecution implements ResultSetExecution, UpdateExecution {
        private int rowCount;
        boolean batchOrBulk;
        ExecutionContext ec;

        public FakeExecution(ExecutionContext ec) {
            this.ec = ec;
        }
        @Override
        public void execute() throws TranslatorException {
            ec.addWarning(new Exception("Some warning")); //$NON-NLS-1$
        }
        @Override
        public List<?> next() throws TranslatorException, DataNotAvailableException {
            if (this.rowCount == 1) {
                return null;
            }
            this.rowCount++;
            return new ArrayList<Object>(Arrays.asList(this.rowCount - 1));
        }
        @Override
        public int[] getUpdateCounts() throws DataNotAvailableException,
                TranslatorException {
            if (batchOrBulk) {
                if (returnSingleUpdate) {
                    return new int[] {2};
                }
                return new int[] {1, 1};
            }
            return new int[] {1};
        }

        @Override
        public void close() {
        }

        @Override
        public void cancel() throws TranslatorException {
        }
    }

    public void setReturnSingleUpdate(boolean returnSingleUpdate) {
        this.returnSingleUpdate = returnSingleUpdate;
    }

    @Override
    public boolean returnsSingleUpdateCount() {
        return returnSingleUpdate;
    }

    public List<Command> getCommands() {
        return commands;
    }

    @Override
    public boolean supportsCompareCriteriaEquals() {
        return true;
    }

    @Override
    public boolean supportsOrCriteria() {
        return true;
    }
}
