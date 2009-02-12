/*
 * JBoss, Home of Professional Open Source.
 * Copyright (C) 2008 Red Hat, Inc.
 * Copyright (C) 2000-2007 MetaMatrix, Inc.
 * Licensed to Red Hat, Inc. under one or more contributor 
 * license agreements.  See the copyright.txt file in the
 * distribution for a full listing of individual contributors.
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

package com.metamatrix.connector.jdbc;

import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Iterator;
import java.util.List;
import java.util.Properties;

import com.metamatrix.common.util.PropertiesUtils;
import com.metamatrix.connector.api.ConnectorLogger;
import com.metamatrix.connector.api.ExecutionContext;
import com.metamatrix.connector.exception.ConnectorException;
import com.metamatrix.connector.jdbc.extension.ResultsTranslator;
import com.metamatrix.connector.jdbc.extension.SQLTranslator;
import com.metamatrix.connector.jdbc.extension.TranslatedCommand;
import com.metamatrix.connector.language.ICommand;
import com.metamatrix.connector.pool.ConnectorIdentity;

/**
 */
public abstract class JDBCBaseExecution {

    // ===========================================================================================================================
    // Fields
    // ===========================================================================================================================

    // Passed to constructor
    protected Connection connection;
    protected SQLTranslator sqlTranslator;
    protected ResultsTranslator resultsTranslator;
    protected ConnectorIdentity id;
    protected ConnectorLogger logger;
    protected ExecutionContext context;

    // Derived from properties
    protected boolean trimString;
    protected int fetchSize;

    // Set during execution
    protected Statement statement;

    // ===========================================================================================================================
    // Constructors
    // ===========================================================================================================================

    protected JDBCBaseExecution(Connection connection,
                                SQLTranslator sqlTranslator,
                                ResultsTranslator resultsTranslator,
                                ConnectorLogger logger,
                                Properties props,
                                ExecutionContext context) {
        this.connection = connection;
        this.sqlTranslator = sqlTranslator;
        this.resultsTranslator = resultsTranslator;
        this.logger = logger;
        this.context = context;

        String propStr = props.getProperty(JDBCPropertyNames.TRIM_STRINGS);
        if (propStr != null) {
            trimString = Boolean.valueOf(propStr).booleanValue();
        }
        
        fetchSize = PropertiesUtils.getIntProperty(props, JDBCPropertyNames.FETCH_SIZE, context.getBatchSize());
        int max = resultsTranslator.getMaxResultRows();
        if (max > 0) {
        	fetchSize = Math.min(fetchSize, max);
        }
    }

    // ===========================================================================================================================
    // Methods
    // ===========================================================================================================================

    private void addSql(TranslatedCommand command,
                        StringBuffer message) {
        String sql = command.getSql();
        int ndx = sql.indexOf('?');
        if (ndx >= 0) {
            message.append(sql.substring(0, ndx));
            int len = sql.length();
            for (Iterator itr = command.getPreparedValues().iterator(); itr.hasNext() && ndx < len;) {
                message.append(itr.next());
                int nextNdx = sql.indexOf('?', ++ndx);
                if (nextNdx >= 0) {
                    message.append(sql.substring(ndx, nextNdx));
                } else {
                    message.append(sql.substring(ndx));
                }
                ndx = nextNdx;
            }
        } else {
            message.append(sql);
        }
    }

    /**
     * @param error
     * @param command
     * @return
     * @since 5.5
     */
    protected ConnectorException createAndLogError(SQLException error,
                                                   TranslatedCommand command) {
        ConnectorException connectorErr = createError(error, command);
        this.logger.logError(connectorErr.getMessage());
        return connectorErr;
    }

    /**
     * @param error
     * @param messageKey
     * @param commands
     * @return
     * @throws ConnectorException
     * @since 5.5
     */
    protected ConnectorException createAndLogError(Throwable error,
                                                   String messageKey,
                                                   List commands) throws ConnectorException {
        String msg;
        if (commands.isEmpty()) {
            msg = error.getMessage();
        } else {
            msg = JDBCPlugin.Util.getString(messageKey, error.getMessage());
            StringBuffer buf = new StringBuffer(msg);
            for (Iterator itr = commands.iterator(); itr.hasNext();) {
                buf.append("\n  "); //$NON-NLS-1$
                addSql((TranslatedCommand)itr.next(), buf);
            }
            msg = buf.toString();
        }
        this.logger.logError(msg);
        if (error instanceof ConnectorException) {
            error = ((ConnectorException)error).getCause();
        }
        throw new ConnectorException(error, msg);
    }

    /**
     * @param error
     * @param command
     * @return
     * @since 5.5
     */
    protected ConnectorException createError(SQLException error,
                                             TranslatedCommand command) {
        String msg = (command == null ? error.getMessage()
                        : JDBCPlugin.Util.getString("JDBCQueryExecution.Error_executing_query__1", //$NON-NLS-1$
                                                    error.getMessage(), createSql(command)));
        return new ConnectorException(error, msg);
    }

    private String createSql(TranslatedCommand command) {
        StringBuffer msg = new StringBuffer();
        addSql(command, msg);
        return msg.toString();
    }

    protected TranslatedCommand translateCommand(ICommand command) throws ConnectorException {
        TranslatedCommand translatedCommand = new TranslatedCommand(context, sqlTranslator);
        translatedCommand.translateCommand(command);

        if (translatedCommand.getSql() != null) {
            this.logger.logDetail("Source-specific command: " + translatedCommand.getSql()); //$NON-NLS-1$
        }

        return translatedCommand;
    }

    /*
     * @see com.metamatrix.data.Execution#close()
     */
    public synchronized void close() throws ConnectorException {
        try {
            if (statement != null) {
                statement.close();
            }
        } catch (SQLException e) {
            throw new ConnectorException(e);
        }
    }

    /*
     * @see com.metamatrix.data.Execution#cancel()
     */
    public synchronized void cancel() throws ConnectorException {
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

    protected void setSizeContraints(Statement statement) throws SQLException {
        if (resultsTranslator.getMaxResultRows() > 0) {
            statement.setMaxRows(resultsTranslator.getMaxResultRows());
        }
    	statement.setFetchSize(fetchSize);
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
    
    public SQLTranslator getSqlTranslator() {
		return sqlTranslator;
	}
}
