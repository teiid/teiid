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
import java.sql.ParameterMetaData;
import java.sql.SQLException;
import java.sql.Types;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import org.teiid.connector.api.ConnectorEnvironment;
import org.teiid.connector.api.ConnectorException;
import org.teiid.connector.api.ConnectorLogger;
import org.teiid.connector.api.DataNotAvailableException;
import org.teiid.connector.api.ExecutionContext;
import org.teiid.connector.api.ProcedureExecution;
import org.teiid.connector.api.TypeFacility;
import org.teiid.connector.jdbc.translator.TranslatedCommand;
import org.teiid.connector.jdbc.translator.Translator;
import org.teiid.connector.language.ICommand;
import org.teiid.connector.language.IParameter;
import org.teiid.connector.language.IProcedure;
import org.teiid.connector.language.IParameter.Direction;
import org.teiid.connector.metadata.runtime.RuntimeMetadata;

/**
 */
public class JDBCProcedureExecution extends JDBCQueryExecution implements ProcedureExecution {

	private ParameterMetaData parameterMetaData;
	
    /**
     * @param connection
     * @param sqlTranslator
     * @param logger
     * @param props
     * @param id
     */
    public JDBCProcedureExecution(ICommand command,
        Connection connection,
        Translator sqlTranslator,
        ConnectorLogger logger,
        Properties props,
        RuntimeMetadata metadata, ExecutionContext context,
        ConnectorEnvironment env) {
        super(command, connection, sqlTranslator, logger, props, context, env);
    }

    @Override
    public void execute() throws ConnectorException {
    	IProcedure procedure = (IProcedure)command;
        columnDataTypes = procedure.getResultSetColumnTypes();

        //translate command
        TranslatedCommand translatedComm = translateCommand(procedure);
        
        //create statement or CallableStatement and execute
        String sql = translatedComm.getSql();
        try{
            //create parameter index map
            CallableStatement cstmt = getCallableStatement(sql);
            this.parameterMetaData = cstmt.getParameterMetaData();
            this.results = sqlTranslator.executeStoredProcedure(cstmt, translatedComm);
            if (results != null) {
            	initResultSetInfo();
            }
            addStatementWarnings();
        }catch(SQLException e){
            throw new ConnectorException(e, JDBCPlugin.Util.getString("JDBCQueryExecution.Error_executing_query__1", sql)); //$NON-NLS-1$
        }           
        
    }
    
    @Override
    public List<?> next() throws ConnectorException, DataNotAvailableException {
    	if (results == null) {
    		return null;
    	}
    	return super.next();
    }
        
    @Override
    public List<?> getOutputParameterValues() throws ConnectorException {
        try {
        	IProcedure proc = (IProcedure)this.command;
        	List<Object> result = new ArrayList<Object>();
        	int paramIndex = 1;
        	for (IParameter parameter : proc.getParameters()) {
        		if (parameter.getDirection() == Direction.RETURN) {
                	addParameterValue(result, paramIndex, parameter);
                	break;
        		}
			}
        	for (IParameter parameter : proc.getParameters()) {
        		if (parameter.getDirection() == Direction.RETURN || parameter.getDirection() == Direction.RESULT_SET) {
        			continue;
        		}
        		paramIndex++;
        		if (parameter.getDirection() == Direction.INOUT || parameter.getDirection() == Direction.OUT) {
        			addParameterValue(result, paramIndex, parameter);
        		}
			}
        	return result;
        } catch (SQLException e) {
            throw new ConnectorException(e);
        }
    }

	private void addParameterValue(List<Object> result, int paramIndex,
			IParameter parameter) throws SQLException {
		Object value = sqlTranslator.retrieveValue((CallableStatement)this.statement, paramIndex, parameter.getType());
		if (value != null
				&& TypeFacility.RUNTIME_TYPES.STRING.equals(value.getClass())
				&& (trimString || (parameterMetaData != null && parameterMetaData
						.getParameterType(paramIndex) == Types.CHAR))) {
			value = trimString((String)value);
		}
		result.add(value);
	}
    
}
