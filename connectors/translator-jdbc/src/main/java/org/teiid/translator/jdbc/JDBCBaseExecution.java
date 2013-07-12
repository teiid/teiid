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
import org.teiid.language.Parameter;
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
    protected Command command;

    // Derived from properties
    protected boolean trimString;
    protected int fetchSize;

    // Set during execution
    protected Statement statement;

    // ===========================================================================================================================
    // Constructors
    // ===========================================================================================================================

    protected JDBCBaseExecution(Command command, Connection connection, ExecutionContext context, JDBCExecutionFactory jef) {
        this.connection = connection;
        this.context = context;

        this.executionFactory = jef;
        
        trimString = jef.isTrimStrings();
        fetchSize = context.getBatchSize();
        this.command = command;
    }
    
    /**
     * Bind the values in the TranslatedCommand to the PreparedStatement
     */
	protected void bind(PreparedStatement stmt, List<?> params, List<?> batchValues)
			throws SQLException {
		for (int i = 0; i< params.size(); i++) {
		    Object paramValue = params.get(i);
		    Object value = null;
		    Class<?> paramType = null;
		    if (paramValue instanceof Literal) {
		    	Literal litParam = (Literal)paramValue;
		    	value = litParam.getValue();
		    	paramType = litParam.getType();
		    } else {
		    	Parameter param = (Parameter)paramValue;
		    	if (batchValues == null) {
		    		throw new AssertionError("Expected batchValues when using a Parameter"); //$NON-NLS-1$
		    	}
		    	value = batchValues.get(param.getValueIndex());
		    	paramType = param.getType();
		    }
		    this.executionFactory.bindValue(stmt, value, paramType, i+1);
		}
		if (batchValues != null) {
			stmt.addBatch();
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
    		executionFactory.setFetchSize(command, context, statement, fetchSize);
		} catch (SQLException e) {
			if (LogManager.isMessageToBeRecorded(LogConstants.CTX_CONNECTOR, MessageLevel.DETAIL)) {
    			LogManager.logDetail(LogConstants.CTX_CONNECTOR, context.getRequestId(), " could not set fetch size: ", fetchSize); //$NON-NLS-1$
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
        if (context.getCommandContext().isReturnAutoGeneratedKeys() && executionFactory.supportsGeneratedKeys(context, command)) {
        	statement = connection.prepareStatement(sql, Statement.RETURN_GENERATED_KEYS);
        } else {
        	statement = connection.prepareStatement(sql);
        }
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
    	if (warning != null) {
			context.addWarning(warning);
			if (LogManager.isMessageToBeRecorded(LogConstants.CTX_CONNECTOR, MessageLevel.DETAIL)) {
		    	while (warning != null) {
					LogManager.logDetail(LogConstants.CTX_CONNECTOR, context.getRequestId() + " Warning: ", warning); //$NON-NLS-1$
		    		warning = warning.getNextWarning();
		    	}
			}
		}
    	this.statement.clearWarnings();
    }
}
