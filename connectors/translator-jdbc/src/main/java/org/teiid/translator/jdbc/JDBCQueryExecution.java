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

package org.teiid.translator.jdbc;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import org.teiid.language.Command;
import org.teiid.language.QueryExpression;
import org.teiid.logging.LogConstants;
import org.teiid.logging.LogManager;
import org.teiid.translator.DataNotAvailableException;
import org.teiid.translator.ExecutionContext;
import org.teiid.translator.ResultSetExecution;
import org.teiid.translator.TranslatorException;


/**
 * 
 */
public class JDBCQueryExecution extends JDBCBaseExecution implements ResultSetExecution {

    // ===========================================================================================================================
    // Fields
    // ===========================================================================================================================

    protected ResultSet results;
    protected Class<?>[] columnDataTypes;

    // ===========================================================================================================================
    // Constructors
    // ===========================================================================================================================

    public JDBCQueryExecution(Command command, Connection connection, ExecutionContext context, JDBCExecutionFactory env) {
        super(command, connection, context, env);
    }
    
    @Override
    public void execute() throws TranslatorException {
        // get column types
        columnDataTypes = ((QueryExpression)command).getColumnTypes();

        // translate command
        TranslatedCommand translatedComm = translateCommand(command);

        String sql = translatedComm.getSql();
        
        LogManager.logTrace(LogConstants.CTX_CONNECTOR, "Source sql", sql); //$NON-NLS-1$

        try {

            if (!translatedComm.isPrepared()) {
                results = getStatement().executeQuery(sql);
            } else {
            	PreparedStatement pstatement = getPreparedStatement(sql);
                bind(pstatement, translatedComm.getPreparedValues(), null);
                results = pstatement.executeQuery();
            } 
            addStatementWarnings();
        } catch (SQLException e) {
             throw new JDBCExecutionException(JDBCPlugin.Event.TEIID11008, e, translatedComm);
        }
    }

    @Override
    public List<?> next() throws TranslatorException, DataNotAvailableException {
        try {
            if (results.next()) {
                // New row for result set
                List<Object> vals = new ArrayList<Object>(columnDataTypes.length);

                for (int i = 0; i < columnDataTypes.length; i++) {
                    // Convert from 0-based to 1-based
                    Object value = this.executionFactory.retrieveValue(results, i+1, columnDataTypes[i]);
                    vals.add(value); 
                }

                return vals;
            } 
        } catch (SQLException e) {
            throw new TranslatorException(e,
                    JDBCPlugin.Util.getString("JDBCTranslator.Unexpected_exception_translating_results___8", e.getMessage())); //$NON-NLS-1$
        }
        
        return null;
    }
    
    /**
     * @see org.teiid.translator.jdbc.JDBCBaseExecution#close()
     */
    public void close() {
        // first we would need to close the result set here then we can close
        // the statement, using the base class.
    	try {
	        if (results != null) {
	            try {
	                results.close();
	                results = null;
	            } catch (SQLException e) {
	            	LogManager.logDetail(LogConstants.CTX_CONNECTOR, e, "Exception closing"); //$NON-NLS-1$
	            }
	        }
    	} finally {
    		super.close();
    	}
    }

}
