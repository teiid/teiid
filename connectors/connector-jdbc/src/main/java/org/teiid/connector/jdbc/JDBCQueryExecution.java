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
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Types;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import org.teiid.connector.jdbc.translator.TranslatedCommand;
import org.teiid.connector.jdbc.translator.Translator;

import com.metamatrix.connector.api.ConnectorEnvironment;
import com.metamatrix.connector.api.ConnectorException;
import com.metamatrix.connector.api.ConnectorLogger;
import com.metamatrix.connector.api.DataNotAvailableException;
import com.metamatrix.connector.api.ExecutionContext;
import com.metamatrix.connector.api.ResultSetExecution;
import com.metamatrix.connector.api.TypeFacility;
import com.metamatrix.connector.api.ValueTranslator;
import com.metamatrix.connector.language.ICommand;
import com.metamatrix.connector.language.IQueryCommand;

/**
 * 
 */
public class JDBCQueryExecution extends JDBCBaseExecution implements ResultSetExecution {

    // ===========================================================================================================================
    // Fields
    // ===========================================================================================================================

    protected ResultSet results;
    protected Class[] columnDataTypes;
    protected ConnectorEnvironment env;
    protected ICommand command;
	private boolean[] transformKnown;
	private ValueTranslator[] transforms;
	private boolean[] trimColumn;
	private int[] nativeTypes;

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
                sqlTranslator.bindPreparedStatementValues(this.connection, pstatement, translatedComm);
                results = pstatement.executeQuery();
            } 
            addStatementWarnings();
            initResultSetInfo();

        } catch (SQLException e) {
            throw new JDBCExecutionException(e, translatedComm);
        }
    }

	protected void initResultSetInfo() throws SQLException {
		trimColumn = new boolean[columnDataTypes.length];
		nativeTypes = new int[columnDataTypes.length];
		ResultSetMetaData rsmd = results.getMetaData();
		for(int i=0; i<columnDataTypes.length; i++) {
			
			nativeTypes[i] = rsmd.getColumnType(i+1);
			if ((nativeTypes[i] == Types.BLOB && (columnDataTypes[i] == TypeFacility.RUNTIME_TYPES.BLOB || columnDataTypes[i] == TypeFacility.RUNTIME_TYPES.OBJECT))
					|| (nativeTypes[i] == Types.CLOB && (columnDataTypes[i] == TypeFacility.RUNTIME_TYPES.CLOB || columnDataTypes[i] == TypeFacility.RUNTIME_TYPES.OBJECT))) {
				context.keepExecutionAlive(true);
			}
			
		    if(columnDataTypes[i].equals(String.class)) {
		        if(trimString || nativeTypes[i] == Types.CHAR) {
		            trimColumn[i] = true;
		        } 
		    }
		}

		transformKnown = new boolean[columnDataTypes.length];
		transforms = new ValueTranslator[columnDataTypes.length];
	}
    
    @Override
    public List next() throws ConnectorException, DataNotAvailableException {
        try {
            if (results.next()) {
                // New row for result set
                List vals = new ArrayList(columnDataTypes.length);

                for (int i = 0; i < columnDataTypes.length; i++) {
                    // Convert from 0-based to 1-based
                    Object value = sqlTranslator.retrieveValue(results, i+1, columnDataTypes[i]);
                    if(value != null) {
                        // Determine transformation if unknown
                        if(! transformKnown[i]) {
                            Class valueType = value.getClass();
                            if(!columnDataTypes[i].isAssignableFrom(valueType)) {
                                transforms[i] = JDBCExecutionHelper.determineTransformation(valueType, columnDataTypes[i], sqlTranslator.getValueTranslators(), sqlTranslator.getTypeFacility());
                            }
                            transformKnown[i] = true;
                        }

                        // Transform value if necessary
                        if(transforms[i] != null) {
                            value = transforms[i].translate(value, context);
                        }
                                                    
                        // Trim string column if necessary
                        if(trimColumn[i]) {
                            value = JDBCExecutionHelper.trimString((String) value);
                        }
                    }
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
