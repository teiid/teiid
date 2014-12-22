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

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import org.teiid.language.QueryExpression;
import org.teiid.logging.LogConstants;
import org.teiid.logging.LogManager;
import org.teiid.metadata.RuntimeMetadata;
import org.teiid.translator.DataNotAvailableException;
import org.teiid.translator.ExecutionContext;
import org.teiid.translator.HBaseConnection;
import org.teiid.translator.ResultSetExecution;
import org.teiid.translator.TranslatorException;

public class HBaseQueryExecution extends HBaseExecution implements ResultSetExecution {
	
	private Class<?>[] columnDataTypes;
	
	protected ResultSet results;
	
	public HBaseQueryExecution(HBaseExecutionFactory executionFactory
							 , QueryExpression command
							 , ExecutionContext executionContext
							 , RuntimeMetadata metadata
							 , HBaseConnection hbconnection) throws HBaseExecutionException {
		super(command, executionFactory, executionContext, metadata, hbconnection);
		this.columnDataTypes = command.getColumnTypes();
		
		visitCommand();
	}

	@Override
	public void execute() throws TranslatorException {

		LogManager.logInfo(LogConstants.CTX_CONNECTOR, this.command);
		
		boolean usingTxn = false;
		boolean success = false;
		try {
			results = getStatement().executeQuery(vistor.getSQL());
			success = true;
		} catch (SQLException e) {
			throw new HBaseExecutionException(HBasePlugin.Event.TEIID27002, e, command);
		} finally {
			if (usingTxn) {
	        	try {
		        	try {
			        	if (success) {
			        		connection.commit();
			        	} else {
			        		connection.rollback();
			        	}
		        	} finally {
			    		connection.setAutoCommit(true);
		        	}
	        	} catch (SQLException e) {
	        	}
        	}
		}
	}
	
	@Override
	public List<?> next() throws TranslatorException, DataNotAvailableException {
		
		try {
			if (results.next()) {
				List<Object> vals = new ArrayList<Object>(columnDataTypes.length);
				
				for (int i = 0; i < columnDataTypes.length; i++) {
                    // Convert from 0-based to 1-based
                    Object value = this.executionFactory.retrieveValue(results, i+1, columnDataTypes[i]);
                    vals.add(value); 
                }

                return vals;
			}
		} catch (SQLException e) {
			throw new HBaseExecutionException(HBasePlugin.Event.TEIID27002, e, HBasePlugin.Event.TEIID27011, command);
		}
		
		return null;
	}

	@Override
	public void close() {

		if (results != null) {
            try {
                results.close();
                results = null;
            } catch (SQLException e) {
            	LogManager.logDetail(LogConstants.CTX_CONNECTOR, e, "Exception closing ResultSet"); 
            }
        }
		
		super.close();
	}
	

}
