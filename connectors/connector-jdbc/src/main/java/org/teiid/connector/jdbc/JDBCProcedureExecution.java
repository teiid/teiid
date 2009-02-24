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
import java.sql.SQLException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.teiid.connector.jdbc.translator.TranslatedCommand;
import org.teiid.connector.jdbc.translator.Translator;

import com.metamatrix.connector.api.ConnectorEnvironment;
import com.metamatrix.connector.api.ConnectorException;
import com.metamatrix.connector.api.ConnectorLogger;
import com.metamatrix.connector.api.DataNotAvailableException;
import com.metamatrix.connector.api.ExecutionContext;
import com.metamatrix.connector.api.ProcedureExecution;
import com.metamatrix.connector.language.ICommand;
import com.metamatrix.connector.language.IParameter;
import com.metamatrix.connector.language.IProcedure;
import com.metamatrix.connector.language.IParameter.Direction;
import com.metamatrix.connector.metadata.runtime.Element;
import com.metamatrix.connector.metadata.runtime.RuntimeMetadata;

/**
 */
public class JDBCProcedureExecution extends JDBCQueryExecution implements ProcedureExecution {

    private Map parameterIndexMap;
    private RuntimeMetadata metadata;
    
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
        this.metadata = metadata;
    }

    @Override
    public void execute() throws ConnectorException {
    	IProcedure procedure = (IProcedure)command;
        columnDataTypes = getColumnDataTypes(procedure.getParameters(), metadata);

        //translate command
        TranslatedCommand translatedComm = translateCommand(procedure);
        
        //create statement or CallableStatement and execute
        String sql = translatedComm.getSql();
        try{
            //create parameter index map
            parameterIndexMap = createParameterIndexMap(procedure.getParameters(), sql);
            CallableStatement cstmt = getCallableStatement(sql);
            results = sqlTranslator.executeStoredProcedure(cstmt, translatedComm);
            if (results != null) {
            	initResultSetInfo();
            }
            addStatementWarnings();
        }catch(SQLException e){
            throw new ConnectorException(e, JDBCPlugin.Util.getString("JDBCQueryExecution.Error_executing_query__1", sql));
        }           
        
    }
    
    @Override
    public List next() throws ConnectorException, DataNotAvailableException {
    	if (results == null) {
    		return null;
    	}
    	return super.next();
    }
    
    /**
     * @param results
     * @return
     */
    public static Class[] getColumnDataTypes(List params, RuntimeMetadata metadata) throws ConnectorException {
        if (params != null) { 
            IParameter resultSet = null;
            Iterator iter = params.iterator();
            while(iter.hasNext()){
                IParameter param = (IParameter)iter.next();
                if(param.getDirection() == Direction.RESULT_SET){
                    resultSet = param;
                    break;
                }
            }

            if(resultSet != null){
                List<Element> columnMetadata = resultSet.getMetadataObject().getChildren();

                int size = columnMetadata.size();
                Class[] coulmnDTs = new Class[size];
                for(int i =0; i<size; i++ ){
                    coulmnDTs[i] = columnMetadata.get(i).getJavaType();
                }
                return coulmnDTs;
            }

        }
        return new Class[0];
    }
    
    /**
     * @param parameters List of IParameter
     * @param sql
     * @return Map of IParameter to index in sql.
     */
    public static Map createParameterIndexMap(List parameters, String sql) {
        if(parameters == null || parameters.isEmpty()){
            return Collections.EMPTY_MAP;
        }
        Map paramsIndexes = new HashMap();
        int index  = 1;
        
        //return parameter, if there is any,  is the first parameter
        Iterator iter = parameters.iterator();
        while(iter.hasNext()){
            IParameter param = (IParameter)iter.next();
            if(param.getDirection() == Direction.RETURN){
                paramsIndexes.put(param, new Integer(index++));
                break;
            }
        }
                      
        iter = parameters.iterator();
        while(iter.hasNext()){
            IParameter param = (IParameter)iter.next();
            if(param.getDirection() != Direction.RESULT_SET && param.getDirection() != Direction.RETURN){
                paramsIndexes.put(param, new Integer(index++));
            }
        }
        return paramsIndexes;
    }
        
    /* 
     * @see com.metamatrix.data.ProcedureExecution#getOutputValue(com.metamatrix.data.language.IParameter)
     */
    public Object getOutputValue(IParameter parameter) throws ConnectorException {
        if(parameter.getDirection() != Direction.OUT && parameter.getDirection() != Direction.INOUT &&  parameter.getDirection() != Direction.RETURN){
            throw new ConnectorException(JDBCPlugin.Util.getString("JDBCProcedureExecution.The_parameter_direction_must_be_out_or_inout_1")); //$NON-NLS-1$
        }
        
        Integer index = (Integer)this.parameterIndexMap.get(parameter);
        if(index == null){
            //should not come here
            throw new ConnectorException(JDBCPlugin.Util.getString("JDBCProcedureExecution.Unexpected_exception_1")); //$NON-NLS-1$
        }
        try {
        	Object value = sqlTranslator.retrieveValue((CallableStatement)this.statement, index.intValue(), parameter.getType());
            if(value == null){
                return null;
            }
            Object result = JDBCExecutionHelper.convertValue(value, parameter.getType(), this.sqlTranslator.getValueTranslators(), this.sqlTranslator.getTypeFacility(), trimString, context);
            return result;
        } catch (SQLException e) {
            throw new ConnectorException(e);
        }
    }
    
}
