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

/*
 */

package com.metamatrix.connector.jdbc;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Calendar;
import java.util.Properties;
import java.util.TimeZone;

import com.metamatrix.connector.jdbc.extension.ResultsTranslator;
import com.metamatrix.connector.jdbc.extension.SQLTranslator;
import com.metamatrix.connector.jdbc.extension.TranslatedCommand;
import com.metamatrix.connector.jdbc.util.JDBCExecutionHelper;
import com.metamatrix.data.api.Batch;
import com.metamatrix.data.api.ConnectorEnvironment;
import com.metamatrix.data.api.ConnectorLogger;
import com.metamatrix.data.api.ExecutionContext;
import com.metamatrix.data.api.SynchQueryCommandExecution;
import com.metamatrix.data.api.SynchQueryExecution;
import com.metamatrix.data.exception.ConnectorException;
import com.metamatrix.data.language.IQuery;
import com.metamatrix.data.language.IQueryCommand;

/**
 * 
 */
public class JDBCQueryExecution extends JDBCBaseExecution implements
                                                         SynchQueryExecution, SynchQueryCommandExecution {

    // ===========================================================================================================================
    // Fields
    // ===========================================================================================================================

    protected int maxBatchSize = -1;
    protected ResultSet results;
    protected Class[] columnDataTypes;
    protected Calendar calendar;
    protected ConnectorEnvironment env;

    // ===========================================================================================================================
    // Constructors
    // ===========================================================================================================================

    public JDBCQueryExecution(Connection connection,
                              SQLTranslator sqlTranslator,
                              ResultsTranslator resultsTranslator,
                              ConnectorLogger logger,
                              Properties props,
                              ExecutionContext context,
                              ConnectorEnvironment env) {
        super(connection, sqlTranslator, resultsTranslator, logger, props, context);

        TimeZone dbmsTimeZone = resultsTranslator.getDatabaseTimezone();

        if (dbmsTimeZone != null) {
            calendar = Calendar.getInstance(dbmsTimeZone);
        } else {
            calendar = Calendar.getInstance();
        }
        
        this.env = env;
    }

    // ===========================================================================================================================
    // Methods
    // ===========================================================================================================================

    public void execute(IQueryCommand command,
                        int maxBatchSize) throws ConnectorException {
        this.maxBatchSize = maxBatchSize;

        // get column types
        columnDataTypes = JDBCExecutionHelper.getColumnDataTypes(command);

        // translate command
        TranslatedCommand translatedComm = translateCommand(command);

        String sql = translatedComm.getSql();

        try {

            if (translatedComm.getStatementType() == TranslatedCommand.STMT_TYPE_STATEMENT) {
                results = getStatement().executeQuery(sql);
            } else if (translatedComm.getStatementType() == TranslatedCommand.STMT_TYPE_PREPARED_STATEMENT) {
                PreparedStatement pstatement = getPreparedStatement(sql);
                resultsTranslator.bindPreparedStatementValues(this.connection, pstatement, translatedComm);
                results = pstatement.executeQuery();
            } else {
                throw new ConnectorException(
                                             JDBCPlugin.Util.getString("JDBCSynchExecution.Statement_type_not_support_for_command_1", new Integer(translatedComm.getStatementType()), sql)); //$NON-NLS-1$
            }

        } catch (SQLException e) {
            // try to cleanup the statement and may be resultset object
            close();

            throw createAndLogError(e, translatedComm);
        }
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.metamatrix.data.SynchExecution#nextBatch(int)
     */
    public Batch nextBatch() throws ConnectorException {
        return JDBCExecutionHelper.createBatch(results,
                                               columnDataTypes,
                                               maxBatchSize,
                                               trimString,
                                               this.resultsTranslator,
                                               context,
                                               calendar,
                                               env.getTypeFacility());
    }

    /**
     * @see com.metamatrix.connector.jdbc.JDBCBaseExecution#close()
     */
    public synchronized void close() throws ConnectorException {
        // first we would need to close the result set here then we can close
        // the statement, using the base class.
        if (results != null) {
            try {
                results.close();
                results = null;
            } catch (SQLException e) {
                throw new ConnectorException(e);
            }
        }
        super.close();
    }

    /** 
     * @see com.metamatrix.data.api.SynchQueryExecution#execute(com.metamatrix.data.language.IQuery, int)
     */
    public void execute(IQuery command,
                        int maxBatchSize) throws ConnectorException {
        execute((IQueryCommand)command, maxBatchSize);
    }

}
