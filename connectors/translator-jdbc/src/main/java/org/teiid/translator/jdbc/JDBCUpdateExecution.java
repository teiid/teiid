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
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.teiid.GeneratedKeys;
import org.teiid.language.BatchedCommand;
import org.teiid.language.BatchedUpdates;
import org.teiid.language.Command;
import org.teiid.language.Insert;
import org.teiid.translator.DataNotAvailableException;
import org.teiid.translator.ExecutionContext;
import org.teiid.translator.TranslatorException;
import org.teiid.translator.TypeFacility;
import org.teiid.translator.UpdateExecution;


/**
 */
public class JDBCUpdateExecution extends JDBCBaseExecution implements UpdateExecution {

	private int[] result;
	private int maxPreparedInsertBatchSize;
	private boolean atomic = true;
	
    /**
     * @param connection
     * @param sqlTranslator
     * @param logger
     * @param props
     * @param id
     */
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
        	result = execute(((BatchedUpdates)command));
        } else {
            // translate command
            TranslatedCommand translatedComm = translateCommand(command);

            result = executeTranslatedCommand(translatedComm);
        }
    }

    public int[] execute(BatchedUpdates batchedCommand) throws TranslatorException {
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
                    bind(pstmt, tCommand.getPreparedValues(), null);
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
        	 throw new JDBCExecutionException(JDBCPlugin.Event.TEIID11011, e, tCommand);
        } finally {
            if (commitType) {
                restoreAutoCommit(!succeeded, null);
            }
        }

        return results;
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
    private int[] executeTranslatedCommand(TranslatedCommand translatedComm) throws TranslatorException {
        // create statement or PreparedStatement and execute
        String sql = translatedComm.getSql();
        boolean commitType = false;
        boolean succeeded = false;
        try {
        	int updateCount = 0;
        	Statement statement = null;
            if (!translatedComm.isPrepared()) {
            	statement = getStatement();
            	if (context.getCommandContext().isReturnAutoGeneratedKeys() && executionFactory.supportsGeneratedKeys(context, command)) {
            		updateCount = statement.executeUpdate(sql, Statement.RETURN_GENERATED_KEYS);
            	} else {
            		updateCount = statement.executeUpdate(sql);
            	}
                addStatementWarnings();
            } else {
            	PreparedStatement pstatement = getPreparedStatement(sql);
            	statement = pstatement;
            	Iterator<? extends List<?>> vi = null;
            	if (command instanceof BatchedCommand) {
            		BatchedCommand batchCommand = (BatchedCommand)command;
            		vi = batchCommand.getParameterValues();
            	}
            	
                if (vi != null) {
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
            				} else {
            					if (i == 0) {
	            					break outer;
	            				}
	            				done = true;
	            				break;
            				}
            			}
            		    int[] results = pstatement.executeBatch();
            		    
            		    for (int i=0; i<results.length; i++) {
            		        updateCount += results[i];
            		    }
            		}
                } else {
            		bind(pstatement, translatedComm.getPreparedValues(), null);
        			updateCount = pstatement.executeUpdate();
        			addStatementWarnings();
                }
                succeeded = true;
            } 
            if (executionFactory.supportsGeneratedKeys() && context.getCommandContext().isReturnAutoGeneratedKeys() && command instanceof Insert) {
        		ResultSet keys = statement.getGeneratedKeys();
        		ResultSetMetaData rsmd = keys.getMetaData();
        		int cols = rsmd.getColumnCount();
        		Class<?>[] columnDataTypes = new Class<?>[cols];
        		String[] columnNames = new String[cols];
        		//this is typically expected to be an int/long, but we'll be general here.  we may eventual need the type logic off of the metadata importer
                for (int i = 0; i < cols; i++) {
                	columnDataTypes[i] = TypeFacility.getDataTypeClass(TypeFacility.getDataTypeNameFromSQLType(rsmd.getColumnType(i+1)));
                	columnNames[i] = rsmd.getColumnName(i+1);
                }
                GeneratedKeys generatedKeys = context.getCommandContext().returnGeneratedKeys(columnNames, columnDataTypes);
                //many databases only support returning a single generated value, but we'll still attempt to gather all
        		while (keys.next()) {
                    List<Object> vals = new ArrayList<Object>(columnDataTypes.length);
                    for (int i = 0; i < columnDataTypes.length; i++) {
                        Object value = this.executionFactory.retrieveValue(keys, i+1, columnDataTypes[i]);
                        vals.add(value); 
                    }
                    generatedKeys.addKey(vals);
        		}
        	}
            return new int[] {updateCount};
        } catch (SQLException err) {
        	 throw new JDBCExecutionException(JDBCPlugin.Event.TEIID11013, err, translatedComm);
        } finally {
        	if (commitType) {
                restoreAutoCommit(!succeeded, translatedComm);
            }
        }
    }

    /**
     * @param command
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
     * @param command
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
