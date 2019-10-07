/*
 * Copyright Red Hat, Inc. and/or its affiliates
 * and other contributors as indicated by the @author tags and
 * the COPYRIGHT.txt file distributed with this work.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.teiid.translator.jdbc;

import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.SQLWarning;
import java.sql.Statement;
import java.util.List;

import org.teiid.language.Argument;
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
    protected volatile Statement statement;
    private volatile boolean canceled;

    // ===========================================================================================================================
    // Constructors
    // ===========================================================================================================================

    protected JDBCBaseExecution(Command command, Connection connection, ExecutionContext context, JDBCExecutionFactory jef) {
        this.connection = connection;
        this.context = context;
        try {
            if (this.connection.getTransactionIsolation() != context.getTransactionIsolation()) {
                this.connection.setTransactionIsolation(context.getTransactionIsolation());
            }
        } catch (Exception e) {
            LogManager.logDetail(LogConstants.CTX_CONNECTOR, e, "Could not set transaction isolation level"); //$NON-NLS-1$
        }
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
            } else if (paramValue instanceof Argument) {
                Argument arg = (Argument)paramValue;
                value = arg.getArgumentValue().getValue();
                paramType = arg.getType();
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
        context.logCommand(translatedCommand.getSql());
        return translatedCommand;
    }

    public void close() {
        try {
            if (statement != null) {
                if (canceled) {
                    try {
                        this.executionFactory.intializeConnectionAfterCancel(connection);
                    } catch (SQLException e) {
                        LogManager.logDetail(LogConstants.CTX_CONNECTOR, e, "Exception closing"); //$NON-NLS-1$
                    }
                }
                statement.close();
            }
        } catch (SQLException e) {
            LogManager.logDetail(LogConstants.CTX_CONNECTOR, e, "Exception closing"); //$NON-NLS-1$
        }
    }

    public void cancel() throws TranslatorException {
        // if both the DBMS and driver support aborting an SQL
        try {
            Statement s = this.statement;
            if (s != null) {
                s.cancel();
                this.canceled = true;
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

    protected Statement getStatement() throws SQLException {
        if (statement != null) {
            statement.close();
            statement = null;
        }
        statement = connection.createStatement();
        setSizeContraints(statement);
        return statement;
    }

    protected CallableStatement getCallableStatement(String sql) throws SQLException {
        if (statement != null) {
            statement.close();
            statement = null;
        }
        statement = connection.prepareCall(sql);
        setSizeContraints(statement);
        return (CallableStatement)statement;
    }

    protected PreparedStatement getPreparedStatement(String sql) throws SQLException {
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
