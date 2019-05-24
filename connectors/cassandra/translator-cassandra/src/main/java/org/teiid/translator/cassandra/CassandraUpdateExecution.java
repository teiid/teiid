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

import java.nio.ByteBuffer;
import java.sql.Blob;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.teiid.core.types.BinaryType;
import org.teiid.language.BatchedUpdates;
import org.teiid.language.BulkCommand;
import org.teiid.language.Command;
import org.teiid.logging.LogConstants;
import org.teiid.logging.LogManager;
import org.teiid.metadata.RuntimeMetadata;
import org.teiid.translator.DataNotAvailableException;
import org.teiid.translator.ExecutionContext;
import org.teiid.translator.TranslatorException;
import org.teiid.translator.UpdateExecution;

import com.datastax.driver.core.GuavaCompatibility;
import com.datastax.driver.core.ResultSetFuture;

public class CassandraUpdateExecution implements UpdateExecution {

    private CassandraConnection connection;
    private ExecutionContext executionContext;
    private RuntimeMetadata metadata;
    private Command command;
    private int updateCount = 1;
    private ResultSetFuture resultSetFuture;

    public CassandraUpdateExecution(Command command,
            ExecutionContext executionContext, RuntimeMetadata metadata,
            CassandraConnection connection) {
        this.command = command;
        this.executionContext = executionContext;
        this.metadata = metadata;
        this.connection = connection;
    }

    @Override
    public void close() {
        this.resultSetFuture = null;
    }

    @Override
    public void cancel() throws TranslatorException {
        if (this.resultSetFuture != null) {
            this.resultSetFuture.cancel(true);
        }
    }

    @Override
    public void execute() throws TranslatorException {
        internalExecute();
        resultSetFuture.addListener(new Runnable() {
            @Override
            public void run() {
                executionContext.dataAvailable();
            }
        }, GuavaCompatibility.INSTANCE.sameThreadExecutor());
    }

    private void internalExecute() throws TranslatorException {
        if (this.command instanceof BatchedUpdates) {
            handleBatchedUpdates();
            return;
        }
        CassandraSQLVisitor visitor = new CassandraSQLVisitor();
        visitor.translateSQL(this.command);
        String cql = visitor.getTranslatedSQL();
        LogManager.logDetail(LogConstants.CTX_CONNECTOR, "Source-Query:", cql); //$NON-NLS-1$
        this.executionContext.logCommand(cql);
        if (this.command instanceof BulkCommand) {
            BulkCommand bc = (BulkCommand)this.command;
            if (bc.getParameterValues() != null) {
                int count = 0;
                List<Object[]> newValues = new ArrayList<Object[]>();
                Iterator<? extends List<?>> values = bc.getParameterValues();
                while (values.hasNext()) {
                    Object[] bindValues = values.next().toArray();
                    for (int i = 0; i < bindValues.length; i++) {
                        if (bindValues[i] instanceof Blob) {
                            Blob blob = (Blob)bindValues[i];
                            try {
                                if (blob.length() > Integer.MAX_VALUE) {
                                    throw new AssertionError("Blob is too large"); //$NON-NLS-1$
                                }
                                byte[] bytes = ((Blob)bindValues[i]).getBytes(0, (int) blob.length());
                                bindValues[i] = ByteBuffer.wrap(bytes);
                            } catch (SQLException e) {
                                throw new TranslatorException(e);
                            }
                        } else if (bindValues[i] instanceof BinaryType) {
                            bindValues[i] = ByteBuffer.wrap(((BinaryType)bindValues[i]).getBytesDirect());
                        }
                    }
                    newValues.add(bindValues);
                    count++;
                }
                updateCount = count;
                resultSetFuture = connection.executeBatch(cql, newValues);
                return;
            }
        }
        resultSetFuture = connection.executeQuery(cql);
    }

    private void handleBatchedUpdates() {
        BatchedUpdates updates = (BatchedUpdates)this.command;
        List<String> cqlUpdates = new ArrayList<String>();
        for (Command update : updates.getUpdateCommands()) {
            CassandraSQLVisitor visitor = new CassandraSQLVisitor();
            visitor.translateSQL(update);
            String cql = visitor.getTranslatedSQL();
            cqlUpdates.add(cql);
        }
        this.updateCount = cqlUpdates.size();
        resultSetFuture = connection.executeBatch(cqlUpdates);
    }

    @Override
    public int[] getUpdateCounts() throws DataNotAvailableException,
            TranslatorException {
        if (!resultSetFuture.isDone()) {
            throw DataNotAvailableException.NO_POLLING;
        }
        resultSetFuture.getUninterruptibly();
        return new int[] {this.updateCount};
    }

}
