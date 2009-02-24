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

package org.teiid.connector.jdbc.translator;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Time;
import java.sql.Timestamp;
import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.TimeZone;

import org.teiid.connector.jdbc.JDBCPlugin;
import org.teiid.connector.jdbc.JDBCPropertyNames;

import com.metamatrix.common.util.PropertiesUtils;
import com.metamatrix.connector.api.ConnectorEnvironment;
import com.metamatrix.connector.api.ConnectorException;
import com.metamatrix.connector.api.ExecutionContext;
import com.metamatrix.connector.api.TypeFacility;
import com.metamatrix.connector.api.ValueTranslator;
import com.metamatrix.connector.internal.ConnectorPropertyNames;
import com.metamatrix.connector.language.ICommand;
import com.metamatrix.connector.language.ILanguageFactory;
import com.metamatrix.connector.language.ILimit;
import com.metamatrix.connector.language.IParameter;
import com.metamatrix.connector.language.ISetQuery;
import com.metamatrix.connector.language.IParameter.Direction;

/**
 * Base class for creating source SQL queries and retrieving results.
 * Specific databases should override as necessary.
 */
public class Translator {

	// Because the retrieveValue() method will be hit for every value of 
    // every JDBC result set returned, we do lots of weird special stuff here 
    // to improve the performance (most importantly to remove big if/else checks
    // of every possible type.  
    
    private static final Map<Class<?>, Integer> TYPE_CODE_MAP = new HashMap<Class<?>, Integer>();
    
    private static final int INTEGER_CODE = 0;
    private static final int LONG_CODE = 1;
    private static final int DOUBLE_CODE = 2;
    private static final int BIGDECIMAL_CODE = 3;
    private static final int SHORT_CODE = 4;
    private static final int FLOAT_CODE = 5;
    private static final int TIME_CODE = 6;
    private static final int DATE_CODE = 7;
    private static final int TIMESTAMP_CODE = 8;
    private static final int BLOB_CODE = 9;
    private static final int CLOB_CODE = 10;
    
    static {
        TYPE_CODE_MAP.put(TypeFacility.RUNTIME_TYPES.INTEGER, new Integer(INTEGER_CODE));
        TYPE_CODE_MAP.put(TypeFacility.RUNTIME_TYPES.LONG, new Integer(LONG_CODE));
        TYPE_CODE_MAP.put(TypeFacility.RUNTIME_TYPES.DOUBLE, new Integer(DOUBLE_CODE));
        TYPE_CODE_MAP.put(TypeFacility.RUNTIME_TYPES.BIG_DECIMAL, new Integer(BIGDECIMAL_CODE));
        TYPE_CODE_MAP.put(TypeFacility.RUNTIME_TYPES.SHORT, new Integer(SHORT_CODE));
        TYPE_CODE_MAP.put(TypeFacility.RUNTIME_TYPES.FLOAT, new Integer(FLOAT_CODE));
        TYPE_CODE_MAP.put(TypeFacility.RUNTIME_TYPES.TIME, new Integer(TIME_CODE));
        TYPE_CODE_MAP.put(TypeFacility.RUNTIME_TYPES.DATE, new Integer(DATE_CODE));
        TYPE_CODE_MAP.put(TypeFacility.RUNTIME_TYPES.TIMESTAMP, new Integer(TIMESTAMP_CODE));
        TYPE_CODE_MAP.put(TypeFacility.RUNTIME_TYPES.BLOB, new Integer(BLOB_CODE));
        TYPE_CODE_MAP.put(TypeFacility.RUNTIME_TYPES.CLOB, new Integer(CLOB_CODE));
    }
	
    private static final MessageFormat COMMENT = new MessageFormat("/*teiid sessionid:{0}, requestid:{1}.{2}*/ "); //$NON-NLS-1$
    public final static TimeZone DEFAULT_TIME_ZONE = TimeZone.getDefault();

    private static final ThreadLocal<Calendar> CALENDAR = new ThreadLocal<Calendar>();

	private Map<String, FunctionModifier> functionModifiers = new HashMap<String, FunctionModifier>();
    private TimeZone databaseTimeZone;
    private ConnectorEnvironment environment;
    
    private boolean useComments;
    private boolean usePreparedStatements;
    
    private List<ValueTranslator<?, ?>> valueTranslators = new ArrayList<ValueTranslator<?, ?>>();
    private int maxResultRows = 0;
    private TypeFacility typeFacility;

    private volatile boolean initialConnection;
    private String connectionTestQuery;
    private int isValidTimeout = -1;
    
    /**
     * Initialize the SQLTranslator.
     * @param env
     * @param metadata
     * @throws ConnectorException
     */
    public void initialize(ConnectorEnvironment env) throws ConnectorException {
        this.environment = env;
        this.typeFacility = env.getTypeFacility();

        String timeZone = env.getProperties().getProperty(JDBCPropertyNames.DATABASE_TIME_ZONE);
        if(timeZone != null && timeZone.trim().length() > 0) {
        	TimeZone tz = TimeZone.getTimeZone(timeZone);
            // Check that the dbms time zone is really different than the local time zone
            if(!DEFAULT_TIME_ZONE.hasSameRules(tz)) {
                this.databaseTimeZone = tz;                
            }               
        }   
        
        this.useComments = PropertiesUtils.getBooleanProperty(env.getProperties(), JDBCPropertyNames.USE_COMMENTS_SOURCE_QUERY, false);
        this.usePreparedStatements = PropertiesUtils.getBooleanProperty(env.getProperties(), JDBCPropertyNames.USE_BIND_VARIABLES, false);
    	this.connectionTestQuery = env.getProperties().getProperty(JDBCPropertyNames.CONNECTION_TEST_QUERY, getDefaultConnectionTestQuery());
    	this.isValidTimeout = PropertiesUtils.getIntProperty(env.getProperties(), JDBCPropertyNames.IS_VALID_TIMEOUT, -1);
    		
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
    }
    
    public TimeZone getDatabaseTimeZone() {
		return databaseTimeZone;
	}
    
    public Calendar getDatabaseCalendar() {
    	if (this.databaseTimeZone == null) {
    		return null;
    	}
    	Calendar cal = CALENDAR.get();
    	if (cal == null) {
    		cal = Calendar.getInstance(this.databaseTimeZone);
    		CALENDAR.set(cal);
    	}
    	return cal;
    }
    
    public final ConnectorEnvironment getEnvironment() {
		return environment;
	}
    
    public final ILanguageFactory getLanguageFactory() {
    	return environment.getLanguageFactory();
    }
    
    /**
     * Modify the command.
     * @param command
     * @param context
     * @return
     */
    public ICommand modifyCommand(ICommand command, ExecutionContext context) throws ConnectorException {
    	return command;
    }
    
    /**
     * Return a map of function name in lower case to FunctionModifier.
     * @return Map of function name to FunctionModifier.
     */
    public Map<String, FunctionModifier> getFunctionModifiers() {
    	return functionModifiers;
    }
    
    public void registerFunctionModifier(String name, FunctionModifier modifier) {
    	this.functionModifiers.put(name, modifier);
    }
    
    public void registerValueTranslator(ValueTranslator<?, ?> translator) {
    	this.valueTranslators.add(translator);
    }
    
    /**
     * Subclasses should override this method to provide a different sql translation
     * of the literal boolean value.  By default, a boolean literal is represented as:
     * <code>'0'</code> or <code>'1'</code>.
     * @param booleanValue Boolean value, never null
     * @return Translated string
     */
    public String translateLiteralBoolean(Boolean booleanValue) {
        if(booleanValue.booleanValue()) {
            return "1"; //$NON-NLS-1$
        }
        return "0"; //$NON-NLS-1$
    }

    /**
     * Subclasses should override this method to provide a different sql translation
     * of the literal date value.  By default, a date literal is represented as:
     * <code>{d'2002-12-31'}</code>
     * @param dateValue Date value, never null
     * @return Translated string
     */
    public String translateLiteralDate(java.sql.Date dateValue) {
        return "{d'" + formatDateValue(dateValue) + "'}"; //$NON-NLS-1$ //$NON-NLS-2$
    }

    /**
     * Subclasses should override this method to provide a different sql translation
     * of the literal time value.  By default, a time literal is represented as:
     * <code>{t'23:59:59'}</code>
     * @param timeValue Time value, never null
     * @return Translated string
     */
    public String translateLiteralTime(Time timeValue) {
    	if (!hasTimeType()) {
    		return "{ts'1970-01-01 " + formatDateValue(timeValue) + "'}"; //$NON-NLS-1$ //$NON-NLS-2$
    	}
        return "{t'" + formatDateValue(timeValue) + "'}"; //$NON-NLS-1$ //$NON-NLS-2$
    }

    /**
     * Subclasses should override this method to provide a different sql translation
     * of the literal timestamp value.  By default, a timestamp literal is
     * represented as: <code>{ts'2002-12-31 23:59:59'}</code>.
     * @param timestampValue Timestamp value, never null
     * @return Translated string
     */
    public String translateLiteralTimestamp(Timestamp timestampValue) {
        return "{ts'" + formatDateValue(timestampValue) + "'}"; //$NON-NLS-1$ //$NON-NLS-2$
    }
    
    /**
     * Format the dateObject (of type date, time, or timestamp) into a string
     * using the DatabaseTimeZone format.
     * @param dateObject
     * @return Formatted string
     */
    public String formatDateValue(java.util.Date dateObject) {
        if (dateObject instanceof Timestamp && getTimestampNanoPrecision() < 9) {
        	Timestamp ts = (Timestamp)dateObject;
        	Timestamp newTs = new Timestamp(ts.getTime());
        	if (getTimestampNanoPrecision() > 0) {
	        	int mask = 10^(9-getTimestampNanoPrecision());
	        	newTs.setNanos(ts.getNanos()/mask*mask);
        	}
        	dateObject = newTs;
        }
    	Calendar cal = getDatabaseCalendar();
    	if(cal == null) {
            return dateObject.toString();
        }
        
        return getEnvironment().getTypeFacility().convertDate(dateObject,
				DEFAULT_TIME_ZONE, cal, dateObject.getClass()).toString();        
    }    
    
    public boolean addSourceComment() {
        return useComments;
    }   
    
    public String addLimitString(String queryCommand, ILimit limit) {
    	return queryCommand + " " + limit.toString(); //$NON-NLS-1$
    }
    
    /**
     * Indicates whether group alias should be of the form
     * "...FROM groupA AS X" or "...FROM groupA X".  Certain
     * data sources (such as Oracle) may not support the first
     * form. 
     * @return boolean
     */
    public boolean useAsInGroupAlias(){
        return true;
    }
    
    public boolean usePreparedStatements() {
    	return this.usePreparedStatements;
    }
    
    public boolean useParensForSetQueries() {
    	return false;
    }
    
    public boolean hasTimeType() {
    	return true;
    }
    
    public String getSetOperationString(ISetQuery.Operation operation) {
    	return operation.toString();
    }
    
    public String getSourceComment(ExecutionContext context, ICommand command) {
	    if (addSourceComment() && context != null) {
	    	synchronized (COMMENT) {
	            return COMMENT.format(new Object[] {context.getConnectionIdentifier(), context.getRequestIdentifier(), context.getPartIdentifier()});
			}
	    }
	    return ""; //$NON-NLS-1$ 
    }
    
    public String replaceElementName(String group, String element) {
    	return null;
    }
    
    public int getTimestampNanoPrecision() {
    	return 9;
    }
    
    public String getConnectionTestQuery() {
		return connectionTestQuery;
	}
    
    public String getDefaultConnectionTestQuery() {
    	return "select 1"; //$NON-NLS-1$
    }
    
    public TypeFacility getTypeFacility() {
    	return typeFacility;
    }
    
    /**
     * This is a generic implementation. Because different databases handle
     * stored procedures differently, subclasses should override this method
     * if necessary.
     */
    public ResultSet executeStoredProcedure(CallableStatement statement, TranslatedCommand command) throws SQLException {
        List params = command.getPreparedValues();
        int index = 1;
        
        Iterator iter = params.iterator();
        while(iter.hasNext()){
            IParameter param = (IParameter)iter.next();
            if(param.getDirection() == Direction.RETURN){
                registerSpecificTypeOfOutParameter(statement,param, index++);
            }
        }
        
        iter = params.iterator();
        while(iter.hasNext()){
            IParameter param = (IParameter)iter.next();
                    
            if(param.getDirection() == Direction.INOUT){
                registerSpecificTypeOfOutParameter(statement,param, index);
            }else if(param.getDirection() == Direction.OUT){
                registerSpecificTypeOfOutParameter(statement,param, index++);
            }
                    
            if(param.getDirection() == Direction.IN || param.getDirection() == Direction.INOUT){
                bindValue(statement, param.getValue(), param.getType(), index++);
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
     * @see com.metamatrix.connector.jdbc.extension.ResultsTranslator#bindPreparedStatementValues(java.sql.Connection, java.sql.PreparedStatement, org.teiid.connector.jdbc.translator.TranslatedCommand)
     */
    public void bindPreparedStatementValues(Connection conn, PreparedStatement stmt, TranslatedCommand command) throws SQLException {
        List params = command.getPreparedValues();
        
        setPreparedStatementValues(stmt, params, command.getPreparedTypes());
    }

    private void setPreparedStatementValues(PreparedStatement stmt, List paramValues, List paramTypes) throws SQLException {
        Calendar cal = getDatabaseCalendar();
    	for (int i = 0; i< paramValues.size(); i++) {
            Object parmvalue = paramValues.get(i);
            Class paramType = (Class)paramTypes.get(i);
            // this means the params is one row
            bindValue(stmt, parmvalue, paramType, i+1);
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
    protected void bindValue(PreparedStatement stmt, Object param, Class paramType, int i) throws SQLException {
        int type = TypeFacility.getSQLTypeFromRuntimeType(paramType);
                
        if (param == null) {
            stmt.setNull(i, type);
            return;
        } 
        //if this is a Date object, then use the database calendar
        if (paramType.equals(TypeFacility.RUNTIME_TYPES.DATE)) {
            stmt.setDate(i,(java.sql.Date)param, getDatabaseCalendar());
            return;
        } 
        if (paramType.equals(TypeFacility.RUNTIME_TYPES.TIME)) {
            stmt.setTime(i,(java.sql.Time)param, getDatabaseCalendar());
            return;
        } 
        if (paramType.equals(TypeFacility.RUNTIME_TYPES.TIMESTAMP)) {
            stmt.setTimestamp(i,(java.sql.Timestamp)param, getDatabaseCalendar());
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
             
            setPreparedStatementValues(stmt, row, command.getPreparedTypes());
            
            stmt.addBatch();
        }
        
        int[] results = stmt.executeBatch();
        
        for (int i=0; i<results.length; i++) {
            updateCount += results[i];
        }        
        return updateCount;
    } 

    public List modifyRow(List batch, ExecutionContext context, ICommand command) {
    	return batch;
    }
    
	public int getMaxResultRows() {
		return maxResultRows;
	}
    
    /* 
     * @see com.metamatrix.connector.jdbc.extension.ValueRetriever#retrieveValue(java.sql.ResultSet, int, java.lang.Class, java.util.Calendar)
     */
    public Object retrieveValue(ResultSet results, int columnIndex, Class expectedType) throws SQLException {
        Integer code = (Integer) TYPE_CODE_MAP.get(expectedType);
        if(code != null) {
            // Calling the specific methods here is more likely to get uniform (and fast) results from different
            // data sources as the driver likely knows the best and fastest way to convert from the underlying
            // raw form of the data to the expected type.  We use a switch with codes in order without gaps
            // as there is a special bytecode instruction that treats this case as a map such that not every value 
            // needs to be tested, which means it is very fast.
            switch(code.intValue()) {
                case INTEGER_CODE:  {
                    int value = results.getInt(columnIndex);                    
                    if(results.wasNull()) {
                        return null;
                    }
                    return Integer.valueOf(value);
                }
                case LONG_CODE:  {
                    long value = results.getLong(columnIndex);                    
                    if(results.wasNull()) {
                        return null;
                    } 
                    return Long.valueOf(value);
                }                
                case DOUBLE_CODE:  {
                    double value = results.getDouble(columnIndex);                    
                    if(results.wasNull()) {
                        return null;
                    } 
                    return new Double(value);
                }                
                case BIGDECIMAL_CODE:  {
                    return results.getBigDecimal(columnIndex); 
                }
                case SHORT_CODE:  {
                    short value = results.getShort(columnIndex);                    
                    if(results.wasNull()) {
                        return null;
                    }                    
                    return Short.valueOf(value);
                }
                case FLOAT_CODE:  {
                    float value = results.getFloat(columnIndex);                    
                    if(results.wasNull()) {
                        return null;
                    } 
                    return new Float(value);
                }
                case TIME_CODE: {
                	return results.getTime(columnIndex, getDatabaseCalendar());
                }
                case DATE_CODE: {
                    return results.getDate(columnIndex, getDatabaseCalendar());
                }
                case TIMESTAMP_CODE: {
                    return results.getTimestamp(columnIndex, getDatabaseCalendar());
                }
    			case BLOB_CODE: {
    				try {
    					return typeFacility.convertToRuntimeType(results.getBlob(columnIndex));
    				} catch (SQLException e) {
    					// ignore
    				}
    				break;
    			}
    			case CLOB_CODE: {
    				try {
    					return typeFacility.convertToRuntimeType(results.getClob(columnIndex));
    				} catch (SQLException e) {
    					// ignore
    				}
    				break;
    			}                
            }
        }

        return typeFacility.convertToRuntimeType(results.getObject(columnIndex));
    }

    public Object retrieveValue(CallableStatement results, int parameterIndex, Class expectedType) throws SQLException{
        Integer code = (Integer) TYPE_CODE_MAP.get(expectedType);
        if(code != null) {
            switch(code.intValue()) {
                case INTEGER_CODE:  {
                    int value = results.getInt(parameterIndex);                    
                    if(results.wasNull()) {
                        return null;
                    }
                    return Integer.valueOf(value);
                }
                case LONG_CODE:  {
                    long value = results.getLong(parameterIndex);                    
                    if(results.wasNull()) {
                        return null;
                    } 
                    return Long.valueOf(value);
                }                
                case DOUBLE_CODE:  {
                    double value = results.getDouble(parameterIndex);                    
                    if(results.wasNull()) {
                        return null;
                    } 
                    return new Double(value);
                }                
                case BIGDECIMAL_CODE:  {
                    return results.getBigDecimal(parameterIndex); 
                }
                case SHORT_CODE:  {
                    short value = results.getShort(parameterIndex);                    
                    if(results.wasNull()) {
                        return null;
                    }                    
                    return Short.valueOf(value);
                }
                case FLOAT_CODE:  {
                    float value = results.getFloat(parameterIndex);                    
                    if(results.wasNull()) {
                        return null;
                    } 
                    return new Float(value);
                }
                case TIME_CODE: {
                    try {
                        return results.getTime(parameterIndex, getDatabaseCalendar());
                    } catch (SQLException e) {
                        //ignore
                    }
                }
                case DATE_CODE: {
                    try {
                        return results.getDate(parameterIndex, getDatabaseCalendar());
                    } catch (SQLException e) {
                        //ignore
                    }
                }
                case TIMESTAMP_CODE: {
                    try {
                        return results.getTimestamp(parameterIndex, getDatabaseCalendar());
                    } catch (SQLException e) {
                        //ignore
                    }
                }
    			case BLOB_CODE: {
    				try {
    					return typeFacility.convertToRuntimeType(results.getBlob(parameterIndex));
    				} catch (SQLException e) {
    					// ignore
    				}
    			}
    			case CLOB_CODE: {
    				try {
    					return typeFacility.convertToRuntimeType(results.getClob(parameterIndex));
    				} catch (SQLException e) {
    					// ignore
    				}
    			}
            }
        }

        // otherwise fall through and call getObject() and rely on the normal
		// translation routines
		return typeFacility.convertToRuntimeType(results.getObject(parameterIndex));
    }
        
    protected void afterInitialConnectionCreation(Connection connection) {
        // now dig some details about this driver/database for log.
        try {
            StringBuffer sb = new StringBuffer();
            DatabaseMetaData dbmd = connection.getMetaData();
            sb.append("Commit=").append(connection.getAutoCommit()); //$NON-NLS-1$
            sb.append(";DatabaseProductName=").append(dbmd.getDatabaseProductName()); //$NON-NLS-1$
            sb.append(";DatabaseProductVersion=").append(dbmd.getDatabaseProductVersion()); //$NON-NLS-1$
            sb.append(";DriverMajorVersion=").append(dbmd.getDriverMajorVersion()); //$NON-NLS-1$
            sb.append(";DriverMajorVersion=").append(dbmd.getDriverMinorVersion()); //$NON-NLS-1$
            sb.append(";DriverName=").append(dbmd.getDriverName()); //$NON-NLS-1$
            sb.append(";DriverVersion=").append(dbmd.getDriverVersion()); //$NON-NLS-1$
            sb.append(";IsolationLevel=").append(dbmd.getDefaultTransactionIsolation()); //$NON-NLS-1$
            
            getEnvironment().getLogger().logInfo(sb.toString());
        } catch (SQLException e) {
            String errorStr = JDBCPlugin.Util.getString("ConnectionListener.failed_to_report_jdbc_connection_details"); //$NON-NLS-1$            
            getEnvironment().getLogger().logInfo(errorStr); 
        }
    }
    
    /**
     * defect request 13979 & 13978
     */
    public void afterConnectionCreation(Connection connection) {
        if (initialConnection) {
            initialConnection = false;
            afterInitialConnectionCreation(connection);
        }
    }
    
    public int getIsValidTimeout() {
		return isValidTimeout;
	}
    
    public SQLConversionVisitor getSQLConversionVisitor() {
    	return new SQLConversionVisitor(this);
    }

}
