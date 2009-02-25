/*
 * JBoss, Home of Professional Open Source.
 * See the COPYRIGHT.txt file distributed with this work for information
 * regarding copyright ownership.  Some portions may be licensed
 * to Red Hat, Inc. under one or more contributor license agreements.
 * 
 * This library is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License as published by the Free Software Foundation; either
 * version 2.1 of the License, or (at your option) any later version.
 * 
 * This library is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 * Lesser General Public License for more details.
 * 
 * You should have received a copy of the GNU Lesser General Public
 * License along with this library; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA
 * 02110-1301 USA.
 */

package org.teiid.connector.jdbc;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import org.teiid.connector.jdbc.translator.TranslatedCommand;
import org.teiid.connector.jdbc.translator.Translator;

import com.metamatrix.connector.api.ConnectorException;
import com.metamatrix.connector.api.ConnectorLogger;
import com.metamatrix.connector.api.DataNotAvailableException;
import com.metamatrix.connector.api.ExecutionContext;
import com.metamatrix.connector.api.UpdateExecution;
import com.metamatrix.connector.language.IBatchedUpdates;
import com.metamatrix.connector.language.IBulkInsert;
import com.metamatrix.connector.language.ICommand;

/**
 */
public class JDBCUpdateExecution extends JDBCBaseExecution implements
                                                          UpdateExecution {

	private ICommand command;
	private int[] result;
	
    /**
     * @param connection
     * @param sqlTranslator
     * @param logger
     * @param props
     * @param id
     */
    public JDBCUpdateExecution(ICommand command, Connection connection,
                               Translator sqlTranslator,
                               ConnectorLogger logger,
                               Properties props,
                               ExecutionContext context) {
        super(connection, sqlTranslator, logger, props, context);
        this.command = command;
    }

    // ===========================================================================================================================
    // Methods
    // ===========================================================================================================================

    @Override
    public void execute() throws ConnectorException {
        if (command instanceof IBulkInsert) {
            result = new int [] {execute((IBulkInsert)command)};
        } else if (command instanceof IBatchedUpdates) {
        	result = execute(((IBatchedUpdates)command));
        } else {
            // translate command
            TranslatedCommand translatedComm = translateCommand(command);

            result = new int [] {executeTranslatedCommand(translatedComm)};
        }
    }

    /**
     * @see com.metamatrix.data.api.BatchedUpdatesExecution#execute(com.metamatrix.connector.language.ICommand[])
     * @since 4.2
     */
    public int[] execute(IBatchedUpdates batchedCommand) throws ConnectorException {
        boolean succeeded = false;

        boolean commitType = getAutoCommit(null);
        ICommand[] commands = (ICommand[])batchedCommand.getUpdateCommands().toArray(new ICommand[batchedCommand.getUpdateCommands().size()]);
        int[] results = new int[commands.length];

        TranslatedCommand command = null;
        
        try {
            // temporarily turn the auto commit off, and set it back to what it was
            // before at the end of the command execution.
            if (commitType) {
                connection.setAutoCommit(false);
            }

            List<TranslatedCommand> executedCmds = new ArrayList<TranslatedCommand>();

            TranslatedCommand previousCommand = null;
            
            for (int i = 0; i < commands.length; i++) {
                command = translateCommand(commands[i]);
                if (command.isPrepared()) {
                    PreparedStatement pstmt = null;
                    if (previousCommand != null && previousCommand.isPrepared() && previousCommand.getSql().equals(command.getSql())) {
                        pstmt = (PreparedStatement)statement;
                    } else {
                        if (!executedCmds.isEmpty()) {
                            executeBatch(i, results, executedCmds);
                        }
                        pstmt = getPreparedStatement(command.getSql());
                    }
                    sqlTranslator.bindPreparedStatementValues(this.connection, pstmt, command);
                    pstmt.addBatch();
                } else {
                    if (previousCommand != null && previousCommand.isPrepared()) {
                        executeBatch(i, results, executedCmds);
                        getStatement();
                    }
                    if (statement == null) {
                        getStatement();
                    }
                    statement.addBatch(command.getSql());
                }
                executedCmds.add(command);
                previousCommand = command;
            }
            if (!executedCmds.isEmpty()) {
                executeBatch(commands.length, results, executedCmds);
            }
            succeeded = true;
        } catch (SQLException e) {
        	throw new JDBCExecutionException(e, command);
        } finally {
            if (commitType) {
                restoreAutoCommit(!succeeded, null);
            }
        }

        return results;
    }

    /**
     * An implementation to bulk insert rows into single table.
     * 
     * @param command
     * @return
     * @throws ConnectorException
     */
    public int execute(IBulkInsert command) throws ConnectorException {
        boolean succeeded = false;

        // translate command
        TranslatedCommand translatedComm = translateCommand(command);

        // create statement or PreparedStatement and execute
        String sql = translatedComm.getSql();

        boolean commitType = getAutoCommit(translatedComm);
        int updateCount = -1;
        try {
            // temporarily turn the auto commit off, and set it back to what it was
            // before at the end of the command execution.
            if (commitType) {
                connection.setAutoCommit(false);
            }
            PreparedStatement stmt = getPreparedStatement(sql);
            updateCount = sqlTranslator.executeStatementForBulkInsert(this.connection, stmt, translatedComm);
            addStatementWarnings();
            succeeded = true;
        } catch (SQLException e) {
        	throw new JDBCExecutionException(e, translatedComm);
        } finally {
            if (commitType) {
                restoreAutoCommit(!succeeded, translatedComm);
            }
        }
        return updateCount;
    }

    private void executeBatch(int commandCount,
                              int[] results,
                              List<TranslatedCommand> commands) throws ConnectorException {
        try {
            int[] batchResults = statement.executeBatch();
            addStatementWarnings();
            for (int j = 0; j < batchResults.length; j++) {
                results[commandCount - 1 - j] = batchResults[batchResults.length - 1 - j];
            }
            commands.clear();
        } catch (SQLException err) {
            throw new JDBCExecutionException(err, commands.toArray(new TranslatedCommand[commands.size()])); //$NON-NLS-1$
        }
    }

    /**
     * @param translatedComm
     * @throws ConnectorException
     * @since 4.3
     */
    private int executeTranslatedCommand(TranslatedCommand translatedComm) throws ConnectorException {
        // create statement or PreparedStatement and execute
        String sql = translatedComm.getSql();

        try {
        	int updateCount;
            if (!translatedComm.isPrepared()) {
                updateCount = getStatement().executeUpdate(sql);
            } else {
            	PreparedStatement pstatement = getPreparedStatement(sql);
                sqlTranslator.bindPreparedStatementValues(this.connection, pstatement, translatedComm);
                updateCount = pstatement.executeUpdate();
            } 
            addStatementWarnings();
            return updateCount;
        } catch (SQLException err) {
        	throw new JDBCExecutionException(err, translatedComm);
        }
    }

    /**
     * @param command
     * @return
     * @throws ConnectorException
     */
    private boolean getAutoCommit(TranslatedCommand command) throws ConnectorException {
        try {
            return connection.getAutoCommit();
        } catch (SQLException err) {
        	throw new JDBCExecutionException(err, command);
        }
    }

    /**
     * If the auto comm
     * 
     * @param exceptionOccurred
     * @param command
     * @throws ConnectorException
     */
    private void restoreAutoCommit(boolean exceptionOccurred,
                                   TranslatedCommand command) throws ConnectorException {
        try {
            if (exceptionOccurred) {
                connection.rollback();
            }
            connection.setAutoCommit(true);
        } catch (SQLException err) {
        	throw new JDBCExecutionException(err, command);
        }
    }
    
    @Override
    public int[] getUpdateCounts() throws DataNotAvailableException,
    		ConnectorException {
    	return result;
    }
}
