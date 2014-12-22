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
package org.teiid.translator.hbase;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Iterator;
import java.util.List;

import org.teiid.language.BatchedCommand;
import org.teiid.language.BatchedUpdates;
import org.teiid.language.Command;
import org.teiid.language.Insert;
import org.teiid.logging.LogConstants;
import org.teiid.logging.LogManager;
import org.teiid.metadata.RuntimeMetadata;
import org.teiid.translator.DataNotAvailableException;
import org.teiid.translator.ExecutionContext;
import org.teiid.translator.HBaseConnection;
import org.teiid.translator.TranslatorException;
import org.teiid.translator.UpdateExecution;

public class HBaseUpdateExecution extends HBaseExecution implements UpdateExecution {
	
	private int[] result;
	private int maxPreparedInsertBatchSize;
	private boolean atomic = true;
		
	public HBaseUpdateExecution(HBaseExecutionFactory executionFactory
							  , Command command
							  , ExecutionContext executionContext
							  , RuntimeMetadata metadata
							  , HBaseConnection connection) throws HBaseExecutionException {
		super(command, executionFactory, executionContext, metadata, connection);
		this.maxPreparedInsertBatchSize = this.executionFactory.getMaxPreparedInsertBatchSize();
		
		visitCommand();
	}

	@Override
	public void execute() throws TranslatorException {
		
		LogManager.logInfo(LogConstants.CTX_CONNECTOR, this.command);
		
		if(command instanceof BatchedUpdates) {
			result = executeBatchedUpdate();
		} else {
			result = executeUpdate();
		}
	}

	private int[] executeUpdate() throws HBaseExecutionException {
		
		String sql = vistor.getSQL();
		
		boolean commitType = false;
        boolean succeeded = false;
        
        try {
			int updateCount = 0;
			Statement statement = null;
			if(!vistor.isPrepared()) {
				statement = getStatement();
				updateCount = statement.executeUpdate(sql);
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
                    commitType = getAutoCommit();
                    if (commitType) {
                        connection.setAutoCommit(false);
                    }
					int maxBatchSize = (command instanceof Insert) ? maxPreparedInsertBatchSize : Integer.MAX_VALUE;
            		boolean done = false;
            		outer: while (!done) {
            			for (int i = 0; i < maxBatchSize; i++) {
            				if (vi.hasNext()) {
    	            			List<?> values = vi.next();
    	            			bind(pstatement, vistor.getPreparedValues(), values);
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
            		bind(pstatement, vistor.getPreparedValues(), null);
        			updateCount = pstatement.executeUpdate();
        			addStatementWarnings();
                }
                succeeded = true;
			}
			return new int[] {updateCount};
		} catch (SQLException e) {
			throw new HBaseExecutionException(HBasePlugin.Event.TEIID27003, e, sql);
		} finally {
			if (commitType) {
                restoreAutoCommit(!succeeded);
            } else {
            	autoCommit(succeeded);
            }
		}
		
	}

	private void autoCommit(boolean succeeded) throws HBaseExecutionException {

		try {
			if(succeeded && !getAutoCommit()) {
				connection.commit();
			}
		} catch (SQLException e) {
			throw new HBaseExecutionException(HBasePlugin.Event.TEIID27003, e, HBasePlugin.Event.TEIID27013, command);
		}
	}

	private void restoreAutoCommit(boolean exceptionOccurred) throws HBaseExecutionException {
		
		 try {
			if (exceptionOccurred) {
			     connection.rollback();
			 }
		} catch (SQLException e) {
			throw new HBaseExecutionException(HBasePlugin.Event.TEIID27003, e, HBasePlugin.Event.TEIID27013, command);
		} finally {
			try {
        		if (!exceptionOccurred) {
        			connection.commit();
        		}
        		connection.setAutoCommit(true);
        	} catch (SQLException e) {
        		throw new HBaseExecutionException(HBasePlugin.Event.TEIID27003, e, HBasePlugin.Event.TEIID27013, command);
            }
		}
		
	}

	private boolean getAutoCommit() throws SQLException {
		if (!atomic) {
			return false;
		}
		return connection.getAutoCommit();
	}

	private int[] executeBatchedUpdate() {
		
		return null;
	}

	@Override
	public int[] getUpdateCounts() throws DataNotAvailableException, TranslatorException {
		return result;
	}
	
	public void setAtomic(boolean atomic) {
		this.atomic = atomic;
	}

}
