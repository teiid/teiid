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

import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.SQLWarning;
import java.sql.Statement;
import java.util.List;

import org.teiid.language.Command;
import org.teiid.language.Literal;
import org.teiid.logging.LogConstants;
import org.teiid.logging.LogManager;
import org.teiid.logging.MessageLevel;
import org.teiid.translator.Execution;
import org.teiid.translator.ExecutionContext;
import org.teiid.translator.TranslatorException;


/**
 */
public abstract class JDBCBaseExecution implements Execution  {

    // ===========================================================================================================================
    // Fields
    // ===========================================================================================================================

    // Passed to constructor
    protected Connection connection;
    protected ExecutionContext context;
    protected JDBCExecutionFactory executionFactory;

    // Derived from properties
    protected boolean trimString;
    protected int fetchSize;

    // Set during execution
    protected Statement statement;

    // ===========================================================================================================================
    // Constructors
    // ===========================================================================================================================

    protected JDBCBaseExecution(Connection connection, ExecutionContext context, JDBCExecutionFactory jef) {
        this.connection = connection;
        this.context = context;

        this.executionFactory = jef;
        
        trimString = jef.isTrimStrings();
        fetchSize = context.getBatchSize();
    }
    
    /**
     * Bind the values in the TranslatedCommand to the PreparedStatement
     */
    protected void bindPreparedStatementValues(PreparedStatement stmt, TranslatedCommand tc, int rowCount) throws SQLException {
        List<?> params = tc.getPreparedValues();

        for (int row = 0; row < rowCount; row++) {
	        for (int i = 0; i< params.size(); i++) {
	            Literal paramValue = (Literal)params.get(i);
	            Object value = paramValue.getValue();
	            if (paramValue.isMultiValued()) {
	            	value = ((List<?>)value).get(row);
	            }
	            Class<?> paramType = paramValue.getType();
	            this.executionFactory.bindValue(stmt, value, paramType, i+1);
	        }
	        if (rowCount > 1) {
            	stmt.addBatch();
            }
        }
    }

    // ===========================================================================================================================
    // Methods
    // ===========================================================================================================================

    protected TranslatedCommand translateCommand(Command command) throws TranslatorException {
        TranslatedCommand translatedCommand = new TranslatedCommand(context, this.executionFactory);
        translatedCommand.translateCommand(command);

        if (translatedCommand.getSql() != null && LogManager.isMessageToBeRecorded(LogConstants.CTX_CONNECTOR, MessageLevel.DETAIL)) {
            LogManager.logDetail(LogConstants.CTX_CONNECTOR, "Source-specific command: " + translatedCommand.getSql()); //$NON-NLS-1$
        }

        return translatedCommand;
    }

    public synchronized void close() {
        try {
            if (statement != null) {
                statement.close();
            }
        } catch (SQLException e) {
			LogManager.logDetail(LogConstants.CTX_CONNECTOR, e, "Exception closing"); //$NON-NLS-1$
        } 
    }

    public synchronized void cancel() throws TranslatorException {
        // if both the DBMS and driver support aborting an SQL
        try {
            if (statement != null) {
                statement.cancel();
            }
        } catch (SQLException e) {
            // Defect 16187 - DataDirect does not support the cancel() method for
            // Statement.cancel() for DB2 and Informix. Here we are tolerant
            // of these and other JDBC drivers that do not support the cancel() operation.
        }
    }

    protected void setSizeContraints(Statement statement) {
    	try {
			statement.setFetchSize(fetchSize);
		} catch (SQLException e) {
			if (LogManager.isMessageToBeRecorded(LogConstants.CTX_CONNECTOR, MessageLevel.DETAIL)) {
    			LogManager.logDetail(LogConstants.CTX_CONNECTOR, context.getRequestIdentifier(), " could not set fetch size: ", fetchSize); //$NON-NLS-1$
    		}
		}
    }

    protected synchronized Statement getStatement() throws SQLException {
        if (statement != null) {
            statement.close();
            statement = null;
        }
        statement = connection.createStatement();
        setSizeContraints(statement);
        return statement;
    }

    protected synchronized CallableStatement getCallableStatement(String sql) throws SQLException {
        if (statement != null) {
            statement.close();
            statement = null;
        }
        statement = connection.prepareCall(sql);
        setSizeContraints(statement);
        return (CallableStatement)statement;
    }

    protected synchronized PreparedStatement getPreparedStatement(String sql) throws SQLException {
        if (statement != null) {
            statement.close();
            statement = null;
        }
        statement = connection.prepareStatement(sql);
        setSizeContraints(statement);
        return (PreparedStatement)statement;
    }

    /**
     * Returns the JDBC connection used by the execution object.
     * 
     * @return Returns the connection.
     * @since 4.1.1
     */
    public Connection getConnection() {
        return this.connection;
    }
    
    public void addStatementWarnings() throws SQLException {
    	SQLWarning warning = this.statement.getWarnings();
    	while (warning != null) {
    		SQLWarning toAdd = warning;
    		warning = toAdd.getNextWarning();
    		toAdd.setNextException(null);
    		if (LogManager.isMessageToBeRecorded(LogConstants.CTX_CONNECTOR, MessageLevel.DETAIL)) {
    			LogManager.logDetail(LogConstants.CTX_CONNECTOR, context.getRequestIdentifier() + " Warning: ", warning); //$NON-NLS-1$
    		}
    		context.addWarning(toAdd);
    	}
    	this.statement.clearWarnings();
    }
}
