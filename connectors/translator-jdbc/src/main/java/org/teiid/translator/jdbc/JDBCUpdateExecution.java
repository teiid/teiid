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

package org.teiid.translator.jdbc;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import org.teiid.language.BatchedUpdates;
import org.teiid.language.Command;
import org.teiid.language.Literal;
import org.teiid.translator.ConnectorException;
import org.teiid.translator.DataNotAvailableException;
import org.teiid.translator.ExecutionContext;
import org.teiid.translator.UpdateExecution;


/**
 */
public class JDBCUpdateExecution extends JDBCBaseExecution implements UpdateExecution {

	private Command command;
	private int[] result;
	
    /**
     * @param connection
     * @param sqlTranslator
     * @param logger
     * @param props
     * @param id
     */
	public JDBCUpdateExecution(Command command, Connection connection, ExecutionContext context, JDBCExecutionFactory env, Translator translator) {
        super(connection, context, env, translator);
        this.command = command;
    }

    // ===========================================================================================================================
    // Methods
    // ===========================================================================================================================

    @Override
    public void execute() throws ConnectorException {
        if (command instanceof BatchedUpdates) {
        	result = execute(((BatchedUpdates)command));
        } else {
            // translate command
            TranslatedCommand translatedComm = translateCommand(command);

            result = executeTranslatedCommand(translatedComm);
        }
    }

    /**
     * @see com.metamatrix.data.api.BatchedUpdatesExecution#execute(org.teiid.language.Command[])
     * @since 4.2
     */
    public int[] execute(BatchedUpdates batchedCommand) throws ConnectorException {
        boolean succeeded = false;

        boolean commitType = getAutoCommit(null);
        Command[] commands = batchedCommand.getUpdateCommands().toArray(new Command[batchedCommand.getUpdateCommands().size()]);
        int[] results = new int[commands.length];

        TranslatedCommand tCommand = null;
        
        try {
            // temporarily turn the auto commit off, and set it back to what it was
            // before at the end of the command execution.
            if (commitType) {
                connection.setAutoCommit(false);
            }

            List<TranslatedCommand> executedCmds = new ArrayList<TranslatedCommand>();

            TranslatedCommand previousCommand = null;
            
            for (int i = 0; i < commands.length; i++) {
            	tCommand = translateCommand(commands[i]);
                if (tCommand.isPrepared()) {
                    PreparedStatement pstmt = null;
                    if (previousCommand != null && previousCommand.isPrepared() && previousCommand.getSql().equals(tCommand.getSql())) {
                        pstmt = (PreparedStatement)statement;
                    } else {
                        if (!executedCmds.isEmpty()) {
                            executeBatch(i, results, executedCmds);
                        }
                        pstmt = getPreparedStatement(tCommand.getSql());
                    }
                    bindPreparedStatementValues(pstmt, tCommand, 1);
                    pstmt.addBatch();
                } else {
                    if (previousCommand != null && previousCommand.isPrepared()) {
                        executeBatch(i, results, executedCmds);
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
                executeBatch(commands.length, results, executedCmds);
            }
            succeeded = true;
        } catch (SQLException e) {
        	throw new JDBCExecutionException(e, tCommand);
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
                    Literal paramValue = (Literal)translatedComm.getPreparedValues().get(i);
                    if (paramValue.isMultiValued()) {
                    	rowCount = ((List<?>)paramValue.getValue()).size();
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
    private boolean getAutoCommit(TranslatedCommand tCommand) throws ConnectorException {
    	if (this.context.isTransactional()) {
    		return false;
    	}
        try {
            return connection.getAutoCommit();
        } catch (SQLException err) {
        	throw new JDBCExecutionException(err, tCommand);
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
                                   TranslatedCommand tCommand) throws ConnectorException {
        try {
            if (exceptionOccurred) {
                connection.rollback();
            }
        } catch (SQLException err) {
        	throw new JDBCExecutionException(err, tCommand);
        } finally {
        	try {
        		connection.commit(); // in JbossAs setAutocommit = true does not trigger the commit.
        		connection.setAutoCommit(true);
        	} catch (SQLException err) {
            	throw new JDBCExecutionException(err, tCommand);
            }
        }
    }
    
    @Override
    public int[] getUpdateCounts() throws DataNotAvailableException,
    		ConnectorException {
    	return result;
    }
}
