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

import org.teiid.connector.api.ConnectorException;
import org.teiid.connector.api.ConnectorLogger;
import org.teiid.connector.api.DataNotAvailableException;
import org.teiid.connector.api.ExecutionContext;
import org.teiid.connector.api.UpdateExecution;
import org.teiid.connector.jdbc.translator.TranslatedCommand;
import org.teiid.connector.jdbc.translator.Translator;
import org.teiid.connector.language.IBatchedUpdates;
import org.teiid.connector.language.ICommand;
import org.teiid.connector.language.ILiteral;


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
        if (command instanceof IBatchedUpdates) {
        	result = execute(((IBatchedUpdates)command));
        } else {
            // translate command
            TranslatedCommand translatedComm = translateCommand(command);

            result = executeTranslatedCommand(translatedComm);
        }
    }

    /**
     * @see com.metamatrix.data.api.BatchedUpdatesExecution#execute(org.teiid.connector.language.ICommand[])
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
                    bindPreparedStatementValues(pstmt, command, 1);
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
            throw new JDBCExecutionException(err, commands.toArray(new TranslatedCommand[commands.size()]));
        }
    }

    /**
     * @param translatedComm
     * @throws ConnectorException
     * @since 4.3
     */
    private int[] executeTranslatedCommand(TranslatedCommand translatedComm) throws ConnectorException {
        // create statement or PreparedStatement and execute
        String sql = translatedComm.getSql();
        boolean commitType = false;
        boolean succeeded = false;
        try {
        	int updateCount = 0;
            if (!translatedComm.isPrepared()) {
                updateCount = getStatement().executeUpdate(sql);
            } else {
            	PreparedStatement pstatement = getPreparedStatement(sql);
                int rowCount = 1;
                for (int i = 0; i< translatedComm.getPreparedValues().size(); i++) {
                    ILiteral paramValue = (ILiteral)translatedComm.getPreparedValues().get(i);
                    if (paramValue.isMultiValued()) {
                    	rowCount = ((List<?>)paramValue).size();
                    	break;
                    }
                }
                if (rowCount > 1) {
                    commitType = getAutoCommit(translatedComm);
                    if (commitType) {
                        connection.setAutoCommit(false);
                    }
                }
                bindPreparedStatementValues(pstatement, translatedComm, rowCount);
            	if (rowCount > 1) {
                    int[] results = pstatement.executeBatch();
                    
                    for (int i=0; i<results.length; i++) {
                        updateCount += results[i];
                    }
                    succeeded = true;
            	} else {
            		updateCount = pstatement.executeUpdate();
            	}
            } 
            addStatementWarnings();
            return new int[] {updateCount};
        } catch (SQLException err) {
        	throw new JDBCExecutionException(err, translatedComm);
        } finally {
        	if (commitType) {
                restoreAutoCommit(!succeeded, translatedComm);
            }
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
        } catch (SQLException err) {
        	throw new JDBCExecutionException(err, command);
        } finally {
        	try {
        		connection.setAutoCommit(true);
        	} catch (SQLException err) {
            	throw new JDBCExecutionException(err, command);
            }
        }
    }
    
    @Override
    public int[] getUpdateCounts() throws DataNotAvailableException,
    		ConnectorException {
    	return result;
    }
}
