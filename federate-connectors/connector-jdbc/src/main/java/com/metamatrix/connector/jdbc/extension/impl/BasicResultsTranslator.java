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

package com.metamatrix.connector.jdbc.extension.impl;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Iterator;
import java.util.List;
import java.util.TimeZone;

import com.metamatrix.connector.jdbc.JDBCPlugin;
import com.metamatrix.connector.jdbc.JDBCPropertyNames;
import com.metamatrix.connector.jdbc.extension.ResultsTranslator;
import com.metamatrix.connector.jdbc.extension.TranslatedCommand;
import com.metamatrix.connector.jdbc.extension.ValueRetriever;
import com.metamatrix.data.api.Batch;
import com.metamatrix.data.api.ConnectorEnvironment;
import com.metamatrix.data.api.ExecutionContext;
import com.metamatrix.data.api.TypeFacility;
import com.metamatrix.data.exception.ConnectorException;
import com.metamatrix.data.internal.ConnectorPropertyNames;
import com.metamatrix.data.language.ICommand;
import com.metamatrix.data.language.IParameter;

/**
 */
public class BasicResultsTranslator implements ResultsTranslator {

    private static final TimeZone LOCAL_TIME_ZONE = TimeZone.getDefault();

    private List valueTranslators = new ArrayList();
    private ValueRetriever valueRetriever = new BasicValueRetriever();
    private TimeZone dbmsTimeZone = null;
    private int maxResultRows = 0;
    private int fetchSize = 0;
    private TypeFacility typeFacility;
    
    /**
     * @see com.metamatrix.connector.jdbc.extension.ResultsTranslator#initialize(com.metamatrix.data.ConnectorEnvironment)
     */
    public void initialize(ConnectorEnvironment env) throws ConnectorException {
        
    	this.typeFacility = env.getTypeFacility();
    	
    	String maxResultRowsString = env.getProperties().getProperty(ConnectorPropertyNames.MAX_RESULT_ROWS);
        if ( maxResultRowsString != null && maxResultRowsString.trim().length() > 0 ) {
            try {
                maxResultRows = Integer.parseInt(maxResultRowsString);
                String exceptionOnMaxRowsString = env.getProperties().getProperty(
                        ConnectorPropertyNames.EXCEPTION_ON_MAX_ROWS);
                maxResultRows = Math.max(0, maxResultRows);
                //if the connector work needs to throw an excpetion, set the size plus 1
                if (maxResultRows > 0 && Boolean.valueOf(exceptionOnMaxRowsString).booleanValue()) {
                	maxResultRows++;
                }
            } catch (NumberFormatException e) {
                //this will already be logged by the connector worker
            }
        }
        
        String fetchSizeString = env.getProperties().getProperty(JDBCPropertyNames.FETCH_SIZE);
        if ( fetchSizeString != null && fetchSizeString.trim().length() > 0 ) {
            try {
                fetchSize = Integer.parseInt(fetchSizeString);
            } catch (NumberFormatException e) {
                Object[] params = new Object[]{JDBCPropertyNames.FETCH_SIZE};
                String msg = JDBCPlugin.Util.getString("BasicResultsTranslator.Couldn__t_parse_property", params); //$NON-NLS-1$
                env.getLogger().logWarning(msg);
            }
        }
                
        String timeZone = env.getProperties().getProperty(JDBCPropertyNames.DATABASE_TIME_ZONE);
        if(timeZone != null && timeZone.trim().length() > 0) {
        	TimeZone tz = TimeZone.getTimeZone(timeZone);
            // Check that the dbms time zone is really different than the local time zone
            if(!LOCAL_TIME_ZONE.hasSameRules(tz)) {
                this.dbmsTimeZone = tz;                
            }               
        }               
    }
    
    
    public TypeFacility getTypefacility() {
    	return typeFacility;
    }
    
    /**
     * This is a generic implementation. Because different dayabases handle
     * stored procedures differently, sbuclass should override this method
     * if necessary.
     * @see com.metamatrix.connector.jdbc.extension.ResultsTranslator#executeStoredProcedure(java.sql.CallableStatement, com.metamatrix.connector.jdbc.extension.TranslatedCommand)
     */
    public ResultSet executeStoredProcedure(CallableStatement statement, TranslatedCommand command) throws SQLException {
        List params = command.getPreparedValues();
        int index = 1;
        
        Iterator iter = params.iterator();
        while(iter.hasNext()){
            IParameter param = (IParameter)iter.next();
            if(param.getDirection() == IParameter.RETURN){
                registerSpecificTypeOfOutParameter(statement,param, index++);
            }
        }
        
        Calendar cal = getDatabaseCalendar();
                
        iter = params.iterator();
        while(iter.hasNext()){
            IParameter param = (IParameter)iter.next();
                    
            if(param.getDirection() == IParameter.INOUT){
                registerSpecificTypeOfOutParameter(statement,param, index);
            }else if(param.getDirection() == IParameter.OUT){
                registerSpecificTypeOfOutParameter(statement,param, index++);
            }
                    
            if(param.getDirection() == IParameter.IN || param.getDirection() == IParameter.INOUT){
                bindValue(statement, param.getValue(), param.getType(), index++, cal);
            }
        }
        
        if (maxResultRows > 0) {
            statement.setMaxRows(maxResultRows + 1);
        }
        
        if (fetchSize > 0) {
            if (maxResultRows > 0) {
                statement.setFetchSize(Math.min(fetchSize, maxResultRows + 1));
            } else {
                statement.setFetchSize(fetchSize);
            }
        }
        
        boolean resultSetNext = statement.execute();
        
        while (!resultSetNext) {
            int update_count = statement.getUpdateCount();
            if (update_count == -1) {
                break;
            }            
            resultSetNext = statement.getMoreResults();
        }
        return statement.getResultSet();
    }

    /**
     * @see com.metamatrix.connector.jdbc.extension.ResultsTranslator#getValueTranslators()
     */
    public List getValueTranslators() {
        return valueTranslators;
    }
    
    public ValueRetriever getValueRetriever() {
        return valueRetriever;
    }
   
    /**
     * For registering specific output parameter types we need to translate these into the appropriate
     * java.sql.Types output parameters
     * We will need to match these up with the appropriate standard sql types
     * @param cstmt
     * @param parameter
     * @throws SQLException
     */
    protected void registerSpecificTypeOfOutParameter(CallableStatement statement, IParameter param, int index) throws SQLException {
        Class runtimeType = param.getType();
        int typeToSet = TypeFacility.getSQLTypeFromRuntimeType(runtimeType);
        
        statement.registerOutParameter(index,typeToSet);
    }
    
    /**
     * Will be called by Query and Update executions if a PreparedStatement is used.
     * 
     * bindValue is ultimately called from this method and for binding CallableStatement
     * values, so subclasses should override that method if necessery to change the binding 
     * behavior.
     *  
     * @see com.metamatrix.connector.jdbc.extension.ResultsTranslator#bindPreparedStatementValues(java.sql.Connection, java.sql.PreparedStatement, com.metamatrix.connector.jdbc.extension.TranslatedCommand)
     */
    public void bindPreparedStatementValues(Connection conn, PreparedStatement stmt, TranslatedCommand command) throws SQLException {
        Calendar cal = getDatabaseCalendar();
        
        List params = command.getPreparedValues();
        
        setPreparedStatementValues(stmt, params, command.getPreparedTypes(), cal);
    }

    /**
     * Get a theadsafe instance of the calendar that is used by the database 
     * @return
     */
    private Calendar getDatabaseCalendar() {
        Calendar cal;
        if (dbmsTimeZone != null) {
            cal = Calendar.getInstance(dbmsTimeZone);            
        } else {
            cal = Calendar.getInstance();
        }
        return cal;
    }
    
    private void setPreparedStatementValues(PreparedStatement stmt, List paramValues, List paramTypes, Calendar cal) throws SQLException {
        for (int i = 0; i< paramValues.size(); i++) {
            Object parmvalue = paramValues.get(i);
            Class paramType = (Class)paramTypes.get(i);
            // this means the params is one row
            bindValue(stmt, parmvalue, paramType, i+1, cal);
        }          
    }

    /**
     * Sets prepared statement parameter i with param.
     * 
     * Performs special handling to translate dates using the database time zone and to
     * translate biginteger, float, and char to JDBC safe objects.
     *  
     * @param stmt
     * @param param
     * @param paramType
     * @param i
     * @param cal
     * @throws SQLException
     */
    protected void bindValue(PreparedStatement stmt, Object param, Class paramType, int i, Calendar cal) throws SQLException {
        int type = TypeFacility.getSQLTypeFromRuntimeType(paramType);
                
        if (param == null) {
            stmt.setNull(i, type);
            return;
        } 
        //if this is a Date object, then use the database calendar
        if (paramType.equals(TypeFacility.RUNTIME_TYPES.DATE)) {
            stmt.setDate(i,(java.sql.Date)param, cal);
            return;
        } 
        if (paramType.equals(TypeFacility.RUNTIME_TYPES.TIME)) {
            stmt.setTime(i,(java.sql.Time)param, cal);
            return;
        } 
        if (paramType.equals(TypeFacility.RUNTIME_TYPES.TIMESTAMP)) {
            stmt.setTimestamp(i,(java.sql.Timestamp)param, cal);
            return;
        }
        //convert these the following to jdbc safe values
        if (TypeFacility.RUNTIME_TYPES.BIG_INTEGER.equals(paramType)) {
            param = new BigDecimal((BigInteger)param);
        } else if (TypeFacility.RUNTIME_TYPES.FLOAT.equals(paramType)) {
            param = new Double(((Float)param).doubleValue());
        } else if (TypeFacility.RUNTIME_TYPES.CHAR.equals(paramType)) {
            param = ((Character)param).toString();
        }
        stmt.setObject(i, param, type);
    }
    
    public int executeStatementForBulkInsert(Connection conn, PreparedStatement stmt, TranslatedCommand command) throws SQLException {
        List rows = command.getPreparedValues();
        Calendar cal = getDatabaseCalendar();
        int updateCount = 0;
        
        for (int i = 0; i< rows.size(); i++) {
            List row = (List) rows.get(i);
             
            setPreparedStatementValues(stmt, row, command.getPreparedTypes(), cal);
            
            stmt.addBatch();
        }
        
        int[] results = stmt.executeBatch();
        
        for (int i=0; i<results.length; i++) {
            updateCount += results[i];
        }        
        return updateCount;
    } 

    public TimeZone getDatabaseTimezone() {
        return this.dbmsTimeZone;
    }       
    
    /** 
     * @see com.metamatrix.connector.jdbc.extension.ResultsTranslator#modifyBatch(com.metamatrix.data.api.Batch, com.metamatrix.data.api.ExecutionContext, com.metamatrix.data.language.ICommand)
     * @since 4.2
     */
    public Batch modifyBatch(Batch batch,
                             ExecutionContext context,
                             ICommand command) {
        return batch;
    }

	public int getMaxResultRows() {
		return maxResultRows;
	}
    
}
