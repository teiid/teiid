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
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

import org.teiid.GeneratedKeys;
import org.teiid.language.BatchedCommand;
import org.teiid.language.BatchedUpdates;
import org.teiid.language.Command;
import org.teiid.language.Insert;
import org.teiid.language.NamedTable;
import org.teiid.logging.LogConstants;
import org.teiid.logging.LogManager;
import org.teiid.metadata.Column;
import org.teiid.metadata.KeyRecord;
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
        	execute(((BatchedUpdates)command));
        } else {
            // translate command
            TranslatedCommand translatedComm = translateCommand(command);

            executeTranslatedCommand(translatedComm);
        }
    }

    public int[] execute(BatchedUpdates batchedCommand) throws TranslatorException {
        boolean succeeded = false;

        boolean commitType = getAutoCommit(null);
        Command[] commands = batchedCommand.getUpdateCommands().toArray(new Command[batchedCommand.getUpdateCommands().size()]);
        result = new int[commands.length];

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
                            executeBatch(i, result, executedCmds);
                        }
                        pstmt = getPreparedStatement(tCommand.getSql());
                    }
                    bind(pstmt, tCommand.getPreparedValues(), null);
                    pstmt.addBatch();
                } else {
                    if (previousCommand != null && previousCommand.isPrepared()) {
                        executeBatch(i, result, executedCmds);
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
        } catch (SQLException e) {
        	 throw new JDBCExecutionException(JDBCPlugin.Event.TEIID11011, e, tCommand);
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
        	Class<?>[] keyColumnDataTypes = null;
        	String[] keyColumnNames = null;
        	if (command instanceof Insert && context.getCommandContext().isReturnAutoGeneratedKeys() && executionFactory.supportsGeneratedKeys(context, command)) {
    			Insert insert = (Insert)command;
    			NamedTable nt = insert.getTable();
    			if (nt.getMetadataObject() != null) {
    				KeyRecord key = nt.getMetadataObject().getPrimaryKey();
    				if (key != null) {
    					List<Column> cols = key.getColumns();
    					keyColumnDataTypes = new Class<?>[cols.size()];
						keyColumnNames = new String[cols.size()];
    	                for (int i = 0; i < cols.size(); i++) {
    	                	Column c = cols.get(i);
    	                	keyColumnDataTypes[i] = c.getJavaType();
    	                	//won't work in scenarios where the teiid name is changed or contains a .
    	                	keyColumnNames[i] = c.getName();
    	                }
    				}
				}
        	}
            if (!translatedComm.isPrepared()) {
            	statement = getStatement();
            	//handle autoGeneratedKeys
            	if (keyColumnDataTypes != null) {
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
            	if (keyColumnDataTypes != null) {
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
            		    if (result == null) {
            		    	result = results;
            		    } else {
            		    	int len = result.length;
            		    	result = Arrays.copyOf(result, len + results.length);
            		    	System.arraycopy(results, 0, result, len, results.length);
            		    }
            		}
                } else {
            		bind(pstatement, translatedComm.getPreparedValues(), null);
        			updateCount = pstatement.executeUpdate();
        			result = new int[] {updateCount};
        			addStatementWarnings();
                }
                succeeded = true;
            } 
            if (keyColumnDataTypes != null) {
            	try {
	        		ResultSet keys = statement.getGeneratedKeys();
	                GeneratedKeys generatedKeys = context.getCommandContext().returnGeneratedKeys(keyColumnNames, keyColumnDataTypes);
	                //many databases only support returning a single generated value, but we'll still attempt to gather all
	        		outer: while (keys.next()) {
	                    List<Object> vals = new ArrayList<Object>(keyColumnDataTypes.length);
	                    for (int i = 0; i < keyColumnDataTypes.length; i++) {
	                        Object value = this.executionFactory.retrieveValue(keys, i+1, keyColumnDataTypes[i]);
	                        if (value != null && TypeFacility.getRuntimeType(value.getClass()) != keyColumnDataTypes[i]) {
	                        	//TODO we may need to let the engine to the final conversion
	                        	LogManager.logDetail(LogConstants.CTX_CONNECTOR, JDBCPlugin.Util.gs(JDBCPlugin.Event.TEIID11023, keyColumnDataTypes[i], keyColumnNames[i], value.getClass()));
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
