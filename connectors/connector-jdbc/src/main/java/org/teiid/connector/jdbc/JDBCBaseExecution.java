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

import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.SQLWarning;
import java.sql.Statement;
import java.util.Arrays;
import java.util.Properties;

import org.teiid.connector.jdbc.translator.TranslatedCommand;
import org.teiid.connector.jdbc.translator.Translator;

import com.metamatrix.common.util.PropertiesUtils;
import com.metamatrix.connector.api.ConnectorException;
import com.metamatrix.connector.api.ConnectorIdentity;
import com.metamatrix.connector.api.ConnectorLogger;
import com.metamatrix.connector.api.ExecutionContext;
import com.metamatrix.connector.basic.BasicExecution;
import com.metamatrix.connector.language.ICommand;

/**
 */
public abstract class JDBCBaseExecution extends BasicExecution  {

    // ===========================================================================================================================
    // Fields
    // ===========================================================================================================================

    // Passed to constructor
    protected Connection connection;
    protected Translator sqlTranslator;
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
                                Translator sqlTranslator,
                                ConnectorLogger logger,
                                Properties props,
                                ExecutionContext context) {
        this.connection = connection;
        this.sqlTranslator = sqlTranslator;
        this.logger = logger;
        this.context = context;

        String propStr = props.getProperty(JDBCPropertyNames.TRIM_STRINGS);
        if (propStr != null) {
            trimString = Boolean.valueOf(propStr).booleanValue();
        }
        
        fetchSize = PropertiesUtils.getIntProperty(props, JDBCPropertyNames.FETCH_SIZE, context.getBatchSize());
        int max = sqlTranslator.getMaxResultRows();
        if (max > 0) {
        	fetchSize = Math.min(fetchSize, max);
        }
    }

    // ===========================================================================================================================
    // Methods
    // ===========================================================================================================================

    protected TranslatedCommand translateCommand(ICommand command) throws ConnectorException {
        TranslatedCommand translatedCommand = new TranslatedCommand(context, sqlTranslator);
        translatedCommand.translateCommand(command);

        if (translatedCommand.getSql() != null && this.logger.isDetailEnabled()) {
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
        if (sqlTranslator.getMaxResultRows() > 0) {
            statement.setMaxRows(sqlTranslator.getMaxResultRows());
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
    
    public Translator getSqlTranslator() {
		return sqlTranslator;
	}
    
    public void addStatementWarnings() throws SQLException {
    	SQLWarning warning = this.statement.getWarnings();
    	while (warning != null) {
    		SQLWarning toAdd = warning;
    		warning = toAdd.getNextWarning();
    		toAdd.setNextException(null);
    		if (logger.isDetailEnabled()) {
    			logger.logDetail(context.getRequestIdentifier() + " Warning: ", warning); //$NON-NLS-1$
    		}
    		context.addWarning(toAdd);
    	}
    	this.statement.clearWarnings();
    }
}
