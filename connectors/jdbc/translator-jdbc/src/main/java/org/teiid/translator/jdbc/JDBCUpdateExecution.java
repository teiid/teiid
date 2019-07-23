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

package org.teiid.translator.jdbc;

import java.sql.BatchUpdateException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

import org.teiid.GeneratedKeys;
import org.teiid.language.BatchedUpdates;
import org.teiid.language.BulkCommand;
import org.teiid.language.Command;
import org.teiid.language.Insert;
import org.teiid.logging.LogConstants;
import org.teiid.logging.LogManager;
import org.teiid.metadata.Column;
import org.teiid.translator.DataNotAvailableException;
import org.teiid.translator.ExecutionContext;
import org.teiid.translator.TranslatorBatchException;
import org.teiid.translator.TranslatorException;
import org.teiid.translator.TypeFacility;
import org.teiid.translator.UpdateExecution;


/**
 */
public class JDBCUpdateExecution extends JDBCBaseExecution implements UpdateExecution {

    private int[] result;
    private int maxPreparedInsertBatchSize;
    private boolean atomic = true;

    public JDBCUpdateExecution(Command command, Connection connection, ExecutionContext context, JDBCExecutionFactory env) {
        super(command, connection, context, env);
        this.maxPreparedInsertBatchSize = this.executionFactory.getMaxPreparedInsertBatchSize();
    }

    // ===========================================================================================================================
    // Methods
    // ===========================================================================================================================

    @Override
    public void execute() throws TranslatorException {
        if (command instanceof BatchedUpdates) {
            execute(((BatchedUpdates)command));
        } else {
            // translate command
            TranslatedCommand translatedComm = translateCommand(command);

            executeTranslatedCommand(translatedComm);
        }
    }

    public int[] execute(BatchedUpdates batchedCommand) throws TranslatorException {
        boolean succeeded = false;

        boolean commitType = false;
        if (batchedCommand.isSingleResult()) {
            commitType = getAutoCommit(null);
        }
        Command[] commands = batchedCommand.getUpdateCommands().toArray(new Command[batchedCommand.getUpdateCommands().size()]);
        result = new int[commands.length];

        TranslatedCommand tCommand = null;
        int batchStart = 0;
        int i = 0;
        try {
            // temporarily turn the auto commit off, and set it back to what it was
            // before at the end of the command execution.
            if (commitType) {
                connection.setAutoCommit(false);
            }

            List<TranslatedCommand> executedCmds = new ArrayList<TranslatedCommand>();

            TranslatedCommand previousCommand = null;

            for (; i < commands.length; i++) {
                tCommand = translateCommand(commands[i]);
                if (tCommand.isPrepared()) {
                    PreparedStatement pstmt = null;
                    if (previousCommand != null && previousCommand.isPrepared() && previousCommand.getSql().equals(tCommand.getSql())) {
                        pstmt = (PreparedStatement)statement;
                    } else {
                        if (!executedCmds.isEmpty()) {
                            executeBatch(i, result, executedCmds);
                            batchStart = i;
                        }
                        pstmt = getPreparedStatement(tCommand.getSql());
                    }
                    bind(pstmt, tCommand.getPreparedValues(), null);
                    pstmt.addBatch();
                } else {
                    if (previousCommand != null && previousCommand.isPrepared()) {
                        executeBatch(i, result, executedCmds);
                        batchStart = i;
                        getStatement();
                    }
                    if (statement == null) {
                        getStatement();
                    }
                    statement.addBatch(tCommand.getSql());
                }
                executedCmds.add(tCommand);
                previousCommand = tCommand;
            }
            if (!executedCmds.isEmpty()) {
                executeBatch(commands.length, result, executedCmds);
            }
            succeeded = true;
        } catch (TranslatorException e) {
            if (batchedCommand.isSingleResult()) {
                throw e;
            }
            int size = i + 1;
            //if there is a BatchUpdateException, there are more update counts to accumulate
            if (e.getCause() instanceof BatchUpdateException) {
                BatchUpdateException bue = (BatchUpdateException)e.getCause();
                int[] batchResults = bue.getUpdateCounts();
                for (int j = 0; j < batchResults.length; j++) {
                    result[batchStart + j] = batchResults[j];
                }
                size = batchStart + batchResults.length;
            } else {
                size = batchStart;
            }
            //resize the result and throw exception
            throw new TranslatorBatchException(e, Arrays.copyOf(result, size));
        } catch (SQLException e) {
            if (batchedCommand.isSingleResult()) {
                throw new JDBCExecutionException(JDBCPlugin.Event.TEIID11011, e, tCommand);
            }
            //resize the result and throw exception
            throw new TranslatorBatchException(e, Arrays.copyOf(result, batchStart));
        } finally {
            if (commitType) {
                restoreAutoCommit(!succeeded, null);
            }
        }

        return result;
    }

    private void executeBatch(int commandCount,
                              int[] results,
                              List<TranslatedCommand> commands) throws TranslatorException {
        try {
            int[] batchResults = statement.executeBatch();
            addStatementWarnings();
            for (int j = 0; j < batchResults.length; j++) {
                results[commandCount - 1 - j] = batchResults[batchResults.length - 1 - j];
            }
            commands.clear();
        } catch (SQLException err) {
             throw new JDBCExecutionException(JDBCPlugin.Event.TEIID11012, err, commands.toArray(new TranslatedCommand[commands.size()]));
        }
    }

    /**
     * @param translatedComm
     * @throws TranslatorException
     * @since 4.3
     */
    private void executeTranslatedCommand(TranslatedCommand translatedComm) throws TranslatorException {
        // create statement or PreparedStatement and execute
        String sql = translatedComm.getSql();
        boolean commitType = false;
        boolean succeeded = false;
        try {
            int updateCount = 0;
            String[] keyColumnNames = null;
            if (command instanceof Insert && executionFactory.supportsGeneratedKeys(context, command)) {
                List<Column> cols = context.getGeneratedKeyColumns();
                if (cols != null) {
                    //TODO won't work in scenarios where the teiid name is changed or contains a .
                    keyColumnNames = context.returnGeneratedKeys().getColumnNames();
                }
            }
            if (!translatedComm.isPrepared()) {
                statement = getStatement();
                //handle autoGeneratedKeys
                if (keyColumnNames != null) {
                    if (executionFactory.useColumnNamesForGeneratedKeys()) {
                        updateCount = statement.executeUpdate(sql, keyColumnNames);
                    } else {
                        updateCount = statement.executeUpdate(sql, Statement.RETURN_GENERATED_KEYS);
                    }
                } else {
                    updateCount = statement.executeUpdate(sql);
                }
                result = new int[] {updateCount};
                addStatementWarnings();
            } else {
                PreparedStatement pstatement = null;
                if (statement != null) {
                    statement.close();
                }
                if (keyColumnNames != null) {
                    if (executionFactory.useColumnNamesForGeneratedKeys()) {
                        pstatement = connection.prepareStatement(sql, keyColumnNames);
                    } else {
                        pstatement = connection.prepareStatement(sql, Statement.RETURN_GENERATED_KEYS);
                    }
                } else {
                    pstatement = getPreparedStatement(sql);
                }
                statement = pstatement;
                Iterator<? extends List<?>> vi = null;
                if (command instanceof BulkCommand) {
                    BulkCommand batchCommand = (BulkCommand)command;
                    vi = batchCommand.getParameterValues();
                }

                int k = 0;
                int batchStart = 0;
                if (vi != null) {
                    try {
                        commitType = getAutoCommit(translatedComm);
                        if (commitType) {
                            connection.setAutoCommit(false);
                        }
                        int maxBatchSize = (command instanceof Insert)?maxPreparedInsertBatchSize:Integer.MAX_VALUE;
                        boolean done = false;
                        outer: while (!done) {
                            for (int i = 0; i < maxBatchSize; i++) {
                                if (vi.hasNext()) {
                                    List<?> values = vi.next();
                                    bind(pstatement, translatedComm.getPreparedValues(), values);
                                    k++;
                                } else {
                                    if (i == 0) {
                                        break outer;
                                    }
                                    done = true;
                                    break;
                                }
                            }
                            int[] results = pstatement.executeBatch();
                            batchStart = k;
                            if (result == null) {
                                result = results;
                            } else {
                                int len = result.length;
                                result = Arrays.copyOf(result, len + results.length);
                                System.arraycopy(results, 0, result, len, results.length);
                            }
                        }
                    } catch (SQLException e) {
                        int size = k + 1;
                        if (result == null) {
                            result = new int[size];
                        } else {
                            result = Arrays.copyOf(result, size);
                        }
                        //if there is a BatchUpdateException, there are more update counts to accumulate
                        if (e instanceof BatchUpdateException) {
                            BatchUpdateException bue = (BatchUpdateException)e;
                            int[] batchResults = bue.getUpdateCounts();
                            for (int j = 0; j < batchResults.length; j++) {
                                result[batchStart + j] = batchResults[j];
                            }
                            size = batchStart + batchResults.length;
                        } else {
                            size = batchStart;
                        }
                        //resize the result and throw exception
                        throw new TranslatorBatchException(e, Arrays.copyOf(result, size));
                    }
                } else {
                    bind(pstatement, translatedComm.getPreparedValues(), null);
                    updateCount = pstatement.executeUpdate();
                    result = new int[] {updateCount};
                }
                addStatementWarnings();
                succeeded = true;
            }
            if (keyColumnNames != null) {
                try {
                    ResultSet keys = statement.getGeneratedKeys();
                    GeneratedKeys generatedKeys = context.returnGeneratedKeys();
                    //many databases only support returning a single generated value, but we'll still attempt to gather all
                    outer: while (keys.next()) {
                        List<Object> vals = new ArrayList<Object>(keyColumnNames.length);
                        for (int i = 0; i < keyColumnNames.length; i++) {
                            Class<?> javaType = generatedKeys.getColumnTypes()[i];
                            Object value = this.executionFactory.retrieveValue(keys, i+1, javaType);
                            if (value != null && TypeFacility.getRuntimeType(value.getClass()) != javaType) {
                                //TODO we may need to let the engine to the final conversion
                                LogManager.logDetail(LogConstants.CTX_CONNECTOR, JDBCPlugin.Util.gs(JDBCPlugin.Event.TEIID11023, javaType, keyColumnNames[i], value.getClass()));
                                continue outer;
                            }
                            vals.add(value);
                        }
                        generatedKeys.addKey(vals);
                    }
                } catch (SQLException e) {
                    context.addWarning(e);
                    LogManager.logDetail(LogConstants.CTX_CONNECTOR, e, "Exception determining generated keys, no keys will be returned"); //$NON-NLS-1$
                }
            }
        } catch (SQLException err) {
             throw new JDBCExecutionException(JDBCPlugin.Event.TEIID11013, err, translatedComm);
        } finally {
            if (commitType) {
                restoreAutoCommit(!succeeded, translatedComm);
            }
        }
    }

    /**
     * @return
     * @throws TranslatorException
     */
    private boolean getAutoCommit(TranslatedCommand tCommand) throws TranslatorException {
        if (!atomic) {
            return false;
        }
        try {
            return connection.getAutoCommit();
        } catch (SQLException err) {
             throw new JDBCExecutionException(JDBCPlugin.Event.TEIID11014, err, tCommand);
        }
    }

    /**
     * Set autoCommit back to true
     *
     * @param exceptionOccurred
     * @throws TranslatorException
     */
    private void restoreAutoCommit(boolean exceptionOccurred,
                                   TranslatedCommand tCommand) throws TranslatorException {
        try {
            if (exceptionOccurred) {
                connection.rollback();
            }
        } catch (SQLException err) {
             throw new JDBCExecutionException(JDBCPlugin.Event.TEIID11015, err, tCommand);
        } finally {
            try {
                if (!exceptionOccurred) {
                    connection.commit(); // in JbossAs setAutocommit = true does not trigger the commit.
                }
                connection.setAutoCommit(true);
            } catch (SQLException err) {
                 throw new JDBCExecutionException(JDBCPlugin.Event.TEIID11016, err, tCommand);
            }
        }
    }

    @Override
    public int[] getUpdateCounts() throws DataNotAvailableException,
            TranslatorException {
        return result;
    }

    public void setMaxPreparedInsertBatchSize(int maxPreparedInsertBatchSize) {
        this.maxPreparedInsertBatchSize = maxPreparedInsertBatchSize;
    }

    public void setAtomic(boolean atomic) {
        this.atomic = atomic;
    }
}
