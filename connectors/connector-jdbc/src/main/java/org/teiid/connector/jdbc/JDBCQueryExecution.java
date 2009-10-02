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

/*
 */

package org.teiid.connector.jdbc;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import org.teiid.connector.api.ConnectorEnvironment;
import org.teiid.connector.api.ConnectorException;
import org.teiid.connector.api.ConnectorLogger;
import org.teiid.connector.api.DataNotAvailableException;
import org.teiid.connector.api.ExecutionContext;
import org.teiid.connector.api.ResultSetExecution;
import org.teiid.connector.jdbc.translator.TranslatedCommand;
import org.teiid.connector.jdbc.translator.Translator;
import org.teiid.connector.language.ICommand;
import org.teiid.connector.language.IQueryCommand;


/**
 * 
 */
public class JDBCQueryExecution extends JDBCBaseExecution implements ResultSetExecution {

    // ===========================================================================================================================
    // Fields
    // ===========================================================================================================================

    protected ResultSet results;
    protected ConnectorEnvironment env;
    protected ICommand command;
    protected Class<?>[] columnDataTypes;

    // ===========================================================================================================================
    // Constructors
    // ===========================================================================================================================

    public JDBCQueryExecution(ICommand command, Connection connection,
                              Translator sqlTranslator,
                              ConnectorLogger logger,
                              Properties props,
                              ExecutionContext context,
                              ConnectorEnvironment env) {
        super(connection, sqlTranslator, logger, props, context);
        this.command = command;
        this.env = env;
    }
    
    @Override
    public void execute() throws ConnectorException {
        // get column types
        columnDataTypes = ((IQueryCommand)command).getColumnTypes();

        // translate command
        TranslatedCommand translatedComm = translateCommand(command);

        String sql = translatedComm.getSql();

        try {

            if (!translatedComm.isPrepared()) {
                results = getStatement().executeQuery(sql);
            } else {
            	PreparedStatement pstatement = getPreparedStatement(sql);
                bindPreparedStatementValues(pstatement, translatedComm, 1);
                results = pstatement.executeQuery();
            } 
            addStatementWarnings();
        } catch (SQLException e) {
            throw new JDBCExecutionException(e, translatedComm);
        }
    }

    @Override
    public List<?> next() throws ConnectorException, DataNotAvailableException {
        try {
            if (results.next()) {
                // New row for result set
                List<Object> vals = new ArrayList<Object>(columnDataTypes.length);

                for (int i = 0; i < columnDataTypes.length; i++) {
                    // Convert from 0-based to 1-based
                    Object value = sqlTranslator.retrieveValue(results, i+1, columnDataTypes[i]);
                    vals.add(value); 
                }

                return vals;
            } 
        } catch (SQLException e) {
            throw new ConnectorException(e,
                    JDBCPlugin.Util.getString("JDBCTranslator.Unexpected_exception_translating_results___8", e.getMessage())); //$NON-NLS-1$
        }
        
        return null;
    }
    
    /**
     * @see org.teiid.connector.jdbc.JDBCBaseExecution#close()
     */
    public void close() throws ConnectorException {
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

}
