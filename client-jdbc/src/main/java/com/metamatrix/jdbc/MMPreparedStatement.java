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

package com.metamatrix.jdbc;

import java.io.InputStream;
import java.io.Reader;
import java.math.BigDecimal;
import java.net.URL;
import java.nio.charset.Charset;
import java.sql.Array;
import java.sql.Blob;
import java.sql.Clob;
import java.sql.NClob;
import java.sql.ParameterMetaData;
import java.sql.PreparedStatement;
import java.sql.Ref;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.RowId;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.SQLXML;
import java.sql.Types;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.TimeZone;
import java.util.TreeMap;
import java.util.logging.Logger;

import com.metamatrix.api.exception.MetaMatrixComponentException;
import com.metamatrix.api.exception.MetaMatrixException;
import com.metamatrix.api.exception.MetaMatrixProcessingException;
import com.metamatrix.common.types.BlobImpl;
import com.metamatrix.common.types.ClobImpl;
import com.metamatrix.common.types.MMJDBCSQLTypeInfo;
import com.metamatrix.common.util.SqlUtil;
import com.metamatrix.common.util.TimestampWithTimezone;
import com.metamatrix.core.util.ArgCheck;
import com.metamatrix.dqp.client.MetadataResult;
import com.metamatrix.dqp.message.ResultsMessage;
import com.metamatrix.jdbc.api.ExecutionProperties;

/**
 * <p> Instances of PreparedStatement contain a SQL statement that has already been
 * compiled.  The SQL statement contained in a PreparedStatement object may have
 * one or more IN parameters. An IN parameter is a parameter whose value is not
 * specified when a SQL statement is created. Instead the statement has a placeholder
 * for each IN parameter.</p>
 * <p> The MMPreparedStatement object wraps the server's PreparedStatement object.
 * The methods in this class are used to set the IN parameters on a server's
 * preparedstatement object.</p>
 */

public class MMPreparedStatement extends MMStatement implements PreparedStatement {
	private static Logger logger = Logger.getLogger("org.teiid.jdbc"); //$NON-NLS-1$
	
    // sql, which this prepared statement is operating on
    protected String prepareSql;

    //map that holds parameter index to values for prepared statements
    private Map<Integer, Object> parameterMap;
    
    //a list of map that holds parameter index to values for prepared statements
    protected List<List<Object>> batchParameterList;

    // metadata
    private ResultSetMetaData metadata;
    
    private Calendar serverCalendar;

    /**
     * Factory Constuctor 
     * @param connection
     * @param sql
     * @param resultSetType
     * @param resultSetConcurrency
     */
    static MMPreparedStatement newInstance(MMConnection connection, String sql, int resultSetType, int resultSetConcurrency) throws SQLException {
        return new MMPreparedStatement(connection, sql, resultSetType, resultSetConcurrency);        
    }
    
    /**
     * <p>MMPreparedStatement constructor.
     * @param Driver's connection object.
     * @param String object representing the prepared statement
     */
    MMPreparedStatement(MMConnection connection, String sql, int resultSetType, int resultSetConcurrency) throws SQLException {
        super(connection, resultSetType, resultSetConcurrency);

        // this sql is for callable statement, don't check any more
        ArgCheck.isNotNull(sql, JDBCPlugin.Util.getString("MMPreparedStatement.Err_prep_sql")); //$NON-NLS-1$
        this.prepareSql = sql;

        TimeZone timezone = connection.getServerConnection().getLogonResult().getTimeZone();

        if (timezone != null && !timezone.hasSameRules(getDefaultCalendar().getTimeZone())) {
        	this.serverCalendar = Calendar.getInstance(timezone);
        }        
    }

    /**
     * <p>Adds a set of parameters to this PreparedStatement object's list of commands
     * to be sent to the database for execution as a batch.
     * @throws SQLException if there is an error
     */
    public void addBatch() throws SQLException {
        checkStatement();
    	if(batchParameterList == null){
    		batchParameterList = new ArrayList<List<Object>>();
		}
    	batchParameterList.add(getParameterValues());
		clearParameters();
    }

    /**
     * <p>Clears the values set for the PreparedStatement object's IN parameters and
     * releases the resources used by those values. In general, parameter values
     * remain in force for repeated use of statement.
     * @throws SQLException if there is an error while clearing params
     */
    public void clearParameters() throws SQLException {
        checkStatement();
        //clear the parameters list on servers prepared statement object
        if(parameterMap != null){
            parameterMap.clear();
        }
    }

    /**
     * In many cases, it is desirable to immediately release a Statements's database
     * and JDBC resources instead of waiting for this to happen when it is automatically
     * closed; the close method provides this immediate release.
     * @throws SQLException should never occur.
     */
    public void close() throws SQLException {
        prepareSql = null;
        super.close();
    }

    /**
     * <p>Executes the SQL statement contained in the PreparedStatement object and
     * indicates whether the first result is a result set, an update count, or
     * there are no results.
     * @return true is has result set
     */
    public boolean execute() throws SQLException {

        // check if the statement is open
        checkStatement();

        if(isUpdateSql(prepareSql)) {
            executeUpdate();
            return false;
        }
        internalExecuteQuery();
        return hasResultSet();
    }
    
    @Override
    public boolean execute(String sql) throws SQLException {
    	String msg = JDBCPlugin.Util.getString("JDBC.Method_not_supported"); //$NON-NLS-1$
        throw new MMSQLException(msg);
    }
    
    @Override
    public ResultSet executeQuery(String sql) throws SQLException {
    	String msg = JDBCPlugin.Util.getString("JDBC.Method_not_supported"); //$NON-NLS-1$
        throw new MMSQLException(msg);
    }
    
    @Override
    public int executeUpdate(String sql) throws SQLException {
    	String msg = JDBCPlugin.Util.getString("JDBC.Method_not_supported"); //$NON-NLS-1$
        throw new MMSQLException(msg);
    }

    /**
     * <p>
     * Executes a prepared SQL query and returns the result set in a ResultSet object. This method should be used only for SQL
     * statements that return a result set; any other result will cause an exception.
     * 
     * @return ResultSet object that results in the execution of this statement
     * @throws SQLException
     *             if there is an error executing
     */
    public ResultSet executeQuery() throws SQLException {
    	if (isUpdateSql(prepareSql)) {
    		throw new MMSQLException(JDBCPlugin.Util.getString("MMStatement.no_result_set")); //$NON-NLS-1$
    	}
    	
        internalExecuteQuery();
        
        if (!hasResultSet()) {
        	throw new MMSQLException(JDBCPlugin.Util.getString("MMStatement.no_result_set")); //$NON-NLS-1$
        }

        return resultSet;
    }

	private void internalExecuteQuery() throws SQLException, MMSQLException {
		checkStatement();

        // See NOTE1
        resetExecutionState();

        processQueryMessage(internalExecute(new String[] {prepareSql}, false));

        // handle exception
        MMSQLException ex = getException();
        if ( ex == null && resultSet == null ) {
            if ( commandStatus == TIMED_OUT ) {
                String msg = JDBCPlugin.Util.getString("MMStatement.Timeout_before_execute"); //$NON-NLS-1$
                setException(new MMSQLException(msg));
            } else if ( commandStatus == CANCELLED ) {
                String msg = JDBCPlugin.Util.getString("MMStatement.Cancel_before_execute"); //$NON-NLS-1$
                setException(new MMSQLException(msg));
            }
        }

        ex = getException();
        if(ex != null) {
            clearParameters();
            throw ex;
        }

        logger.info(JDBCPlugin.Util.getString("MMStatement.Success_query", prepareSql)); //$NON-NLS-1$
	}
    
    private void processUpdateMessage(ResultsMessage resultsMsg) throws SQLException {
        // get results from ResultsMessage
        MetaMatrixException resultsExp = resultsMsg.getException();
        // warnings thrown
        List resultsWarning = resultsMsg.getWarnings();

        MMPreparedStatement.this.setAnalysisInfo(resultsMsg);

        if (resultsExp != null) {
            setException(resultsExp);
        } else {
            // save warnings if have any
            if (resultsWarning != null) {
                accumulateWarnings(resultsWarning);
            }
            // wrap results into ResultSet, only one update count
            resultSet = new MMResultSet(resultsMsg, this);
        }
    }

    /**
     * <p>Executes an SQL INSERT, UPDATE or DELETE statement and returns the number
     * of rows that were affected.
     * @return int value that results in the execution of this statement
     */
    public int executeUpdate() throws SQLException {
    	executeUpdate(false);
    	return super.getUpdateCount();
    }
    
    private void executeUpdate(boolean isBatchedUpdate) throws SQLException {
        //Check to see the statement is closed and throw an exception
        checkStatement();
        if (getMMConnection().isReadOnly()) {
            throw new MMSQLException(JDBCPlugin.Util.getString("MMStatement.Operation_Not_Supported", prepareSql)); //$NON-NLS-1$
        }
        
        // See NOTE1
        resetExecutionState();

        processUpdateMessage(internalExecute(new String[] {prepareSql}, isBatchedUpdate));

        // handle exception
        MMSQLException ex = getException();
        if ( ex == null ) {
            if ( commandStatus == TIMED_OUT ) {
            	String msg = JDBCPlugin.Util.getString("MMStatement.Timeout_before_execute"); //$NON-NLS-1$
            	setException(new MMSQLException(msg));
        	} else if ( commandStatus == CANCELLED ) {
            	String msg = JDBCPlugin.Util.getString("MMStatement.Cancel_before_execute"); //$NON-NLS-1$
            	setException(new MMSQLException(msg));
        	} else if (resultSet.next()) {
            	try {
                	if (isBatchedUpdate) {
                		Object result = resultSet.getObject(1);
                		if(result instanceof int[]){
                			updateCounts = (int[])resultSet.getObject(1);
                		}else{
                            int commandIndex = 0;
                            updateCounts = new int[batchParameterList.size()];
                            do {
                                updateCounts[commandIndex++] = resultSet.getInt(1);
                            } while (resultSet.next());
                		}            		
                	}else{
                		rowsAffected = resultSet.getInt(1);
                	}
                	logger.info(JDBCPlugin.Util.getString("MMStatement.Success_update", prepareSql)); //$NON-NLS-1$
	            } catch (SQLException se) {
        	        setException(MMSQLException.create(se, JDBCPlugin.Util.getString("MMStatement.Err_getting_update_row"))); //$NON-NLS-1$
            	} finally {
                	resultSet.close();
                	resultSet = null;
            	}
        	}
        }

        ex = getException();
        if(ex != null) {
            throw MMSQLException.create(ex, JDBCPlugin.Util.getString("MMStatement.Err_update", prepareSql, ex.getMessage())); //$NON-NLS-1$
        }
    }

    /**
     * <p>Retreives a ResultSetMetaData object with information about the numbers,
     * types, and properties of columns in the ResultSet object that will be returned
     * when this preparedstatement object is executed.
     * @return ResultSetMetaData object
     * @throws SQLException, currently there is no means of getting results
     * metadata before getting results.
     */
    public ResultSetMetaData getMetaData() throws SQLException {

        // check if the statement is open
        checkStatement();

        //if update return null
        if(SqlUtil.isUpdateSql(this.prepareSql)){
        	return null;
        }
        
        if(metadata == null) {
            if(resultSet != null) {
                metadata = resultSet.getMetaData();
            } else {
    			MetadataResult results;
				try {
					results = this.getDQP().getMetadata(this.currentRequestID, prepareSql, Boolean.valueOf(getExecutionProperty(ExecutionProperties.ALLOW_DBL_QUOTED_VARIABLE)).booleanValue());
				} catch (MetaMatrixComponentException e) {
					throw MMSQLException.create(e);
				} catch (MetaMatrixProcessingException e) {
					throw MMSQLException.create(e);
				}
                StaticMetadataProvider provider = StaticMetadataProvider.createWithData(results.getColumnMetadata(), results.getParameterCount());
                metadata = ResultsMetadataWithProvider.newInstance(provider);
            }
        }

        return metadata;
    }

    /**
     * <p>Sets the parameter in position parameterIndex to the input stream object
     * fin, from which length bytes will be read and sent to metamatrix.
     * @param parameterIndex of the parameter whose value is to be set
     * @param in input stream in ASCII to which the parameter value is to be set.
     * @param length, number of bytes to read from the stream
     */
    public void setAsciiStream (int parameterIndex, java.io.InputStream in, int length) throws SQLException {
        //create a clob from the ascii stream
    	setObject(parameterIndex, new ClobImpl(in, Charset.forName("ASCII"), length)); //$NON-NLS-1$
    }

    /**
     * <p>Sets the IN parameter at paramaterIndex to a BigDecimal object. The parameter
     * type is set to NUMERIC
     * @param parameterIndex of the parameter whose value is to be set
     * @param BigDecimal object to which the parameter value is to be set.
     * @throws SQLException, should not occur
     */
    public void setBigDecimal (int parameterIndex, java.math.BigDecimal value) throws SQLException {
        setObject(parameterIndex, value);
    }

    /**
     * <p>Sets the parameter in position parameterIndex to the input stream object
     * fin, from which length bytes will be read and sent to metamatrix.
     * @param parameterIndex of the parameter whose value is to be set
     * @param input stream in binary to which the parameter value is to be set.
     * @param length, number of bytes to read from the stream
     */
    public void setBinaryStream(int parameterIndex, java.io.InputStream in, int length) throws SQLException {
    	//create a blob from the ascii stream
    	setObject(parameterIndex, new BlobImpl(in, length));
    }

    /**
     * <p>Sets the parameter in position parameterIndex to a Blob object.
     * @param parameterIndex of the parameter whose value is to be set
     * @param Blob object to which the parameter value is to be set.
     * @throws SQLException if parameter type/datatype do not match
     */
    public void setBlob (int parameterIndex, Blob x) throws SQLException {
        setObject(parameterIndex, x);
    }

    /**
     * <p>Sets parameter number parameterIndex to b, a Java boolean value. The parameter
     * type is set to BIT
     * @param parameterIndex of the parameter whose value is to be set
     * @param boolean value to which the parameter value is to be set.
     * @throws SQLException, if parameter type/datatype do not match
     */
    public void setBoolean (int parameterIndex, boolean value) throws SQLException {
        setObject(parameterIndex, new Boolean(value));
    }

    /**
     * <p>Sets parameter number parameterIndex to x, a Java byte value. The parameter
     * type is set to TINYINT
     * @param parameterIndex of the parameter whose value is to be set
     * @param byte value to which the parameter value is to be set.
     * @throws SQLException, if parameter type/datatype do not match
     */
    public void setByte(int parameterIndex, byte value) throws SQLException {
        setObject(parameterIndex, new Byte(value));
    }

    /**
     * <p>Sets parameter number parameterIndex to x[], a Java array of bytes.
     * @param parameterIndex of the parameter whose value is to be set
     * @param bytes array to which the parameter value is to be set.
     */
    public void setBytes(int parameterIndex, byte bytes[]) throws SQLException {
    	//create a blob from the ascii stream
    	setObject(parameterIndex, new BlobImpl(bytes));
    }

    /**
     * <p>Sets the parameter in position parameterIndex to a Reader stream object.
     * @param parameterIndex of the parameter whose value is to be set
     * @param Reader object to which the parameter value is to be set.
     * @param length indicating number of charachters to be read from the stream
     */
    public void setCharacterStream (int parameterIndex, java.io.Reader reader, int length) throws SQLException {
    	//create a clob from the ascii stream
    	setObject(parameterIndex, new ClobImpl(reader, length));
    }

    /**
     * <p>Sets the parameter in position parameterIndex to a Clob object.
     * @param parameterIndex of the parameter whose value is to be set
     * @param Clob object to which the parameter value is to be set.
     * @throws SQLException if parameter type/datatype do not match.
     */
    public void setClob (int parameterIndex, Clob x) throws SQLException {
        setObject(parameterIndex, x);
    }

    /**
     * <p>Sets parameter number parameterIndex to x, a java.sql.Date object. The parameter
     * type is set to DATE
     * @param parameterIndex of the parameter whose value is to be set
     * @param Date object to which the parameter value is to be set.
     * @throws SQLException, if parameter type/datatype do not match
     */
    public void setDate(int parameterIndex, java.sql.Date value) throws SQLException {
        setDate(parameterIndex, value, null);
    }

    /**
     * <p>Sets parameter number parameterIndex to x, a java.sql.Date object. The parameter
     * type is set to DATE
     * @param parameterIndex of the parameter whose value is to be set
     * @param Date object to which the parameter value is to be set.
     * @param Calendar object to constrct date(useful to get include timezone info)
     * @throws SQLException, if parameter type/datatype do not match
     */
    public void setDate(int parameterIndex, java.sql.Date x ,java.util.Calendar cal) throws SQLException {

        if (cal == null || x == null) {
            setObject(parameterIndex, x);
            return;
        }
                
        // set the parameter on the stored procedure
        setObject(parameterIndex, TimestampWithTimezone.createDate(x, cal.getTimeZone(), getDefaultCalendar()));
    }

    /**
     * <p>Sets parameter number parameterIndex to x, a double value. The parameter
     * type is set to DOUBLE
     * @param parameterIndex of the parameter whose value is to be set
     * @param double value to which the parameter value is to be set.
     * @throws SQLException, if parameter type/datatype do not match
     */
    public void setDouble(int parameterIndex, double value) throws SQLException {
        setObject(parameterIndex, new Double(value));
    }

    /**
     * <p>Sets parameter number parameterIndex to value, a float value. The parameter
     * type is set to FLOAT
     * @param parameterIndex of the parameter whose value is to be set
     * @param float value to which the parameter value is to be set.
     * @throws SQLException, if parameter type/datatype do not match
     */
    public void setFloat(int parameterIndex, float value) throws SQLException {
        setObject(parameterIndex, new Float(value));
    }

    /**
     * <p>Sets parameter number parameterIndex to value, a int value. The parameter
     * type is set to INTEGER
     * @param parameterIndex of the parameter whose value is to be set
     * @param int value to which the parameter value is to be set.
     * @throws SQLException, if parameter type/datatype do not match
     */
    public void setInt(int parameterIndex, int value) throws SQLException {
        setObject(parameterIndex, new Integer(value));
    }

    /**
     * <p>Sets parameter number parameterIndex to x, a long value. The parameter
     * type is set to BIGINT
     * @param parameterIndex of the parameter whose value is to be set
     * @param long value to which the parameter value is to be set.
     * @throws SQLException, if parameter type/datatype do not match
     */
    public void setLong(int parameterIndex, long value) throws SQLException {
        setObject(parameterIndex, new Long(value));
    }

    /**
     * <p>Sets parameter number parameterIndex to a null value.
     * @param parameterIndex of the parameter whose value is to be set
     * @param jdbc type of the parameter whose value is to be set to null
     * @throws SQLException, if parameter type/datatype do not match
     */
    public void setNull(int parameterIndex, int jdbcType) throws SQLException {
        setObject(parameterIndex, null);
    }

    /**
     * <p>Sets parameter number parameterIndex to a null value.
     * @param parameterIndex of the parameter whose value is to be set
     * @param jdbc type of the parameter whose value is to be set to null
     * @param fully qualifies typename of the parameter being set.
     * @throws SQLException, if parameter type/datatype do not match
     */
    public void setNull(int parameterIndex, int jdbcType, String typeName) throws SQLException {
        setObject(parameterIndex, null);
    }

    /**
     * <p>Sets parameter number parameterIndex to an object value
     * @param parameterIndex of the parameter whose value is to be set
     * @param an object value to which the parameter value is to be set.
     * @param int value giving the JDBC type to conver the object to
     * @param int value giving the scale to be set if the type is DECIMAL or NUMERIC
     * @throws SQLException, if there is an error setting the parameter value
     */
    public void setObject (int parameterIndex, Object value, int targetJdbcType, int scale) throws SQLException {

       if(value == null) {
            setObject(parameterIndex, null);
            return;
        }

       if(targetJdbcType != Types.DECIMAL || targetJdbcType != Types.NUMERIC) {
            setObject(parameterIndex, value, targetJdbcType);
        // Decimal and NUMERIC types correspong to java.math.BigDecimal
        } else {
            // transform the object to a BigDecimal
            BigDecimal bigDecimalObject = DataTypeTransformer.getBigDecimal(value);
            // set scale on the BigDecimal
            bigDecimalObject.setScale(scale);

            setObject(parameterIndex, bigDecimalObject);
        }
    }

    public void setObject(int parameterIndex, Object value, int targetJdbcType) throws SQLException {

        Object targetObject = null;

       if(value == null) {
            setObject(parameterIndex, null);
            return;
        }

        // get the java class name for the given JDBC type
        String javaClassName = MMJDBCSQLTypeInfo.getJavaClassName(targetJdbcType);
        // transform the value to the target datatype
        if(javaClassName.equalsIgnoreCase(MMJDBCSQLTypeInfo.STRING_CLASS)) {
           targetObject = value.toString();
        } else if(javaClassName.equalsIgnoreCase(MMJDBCSQLTypeInfo.CHAR_CLASS)) {
            targetObject = DataTypeTransformer.getCharacter(value);
        } else if(javaClassName.equalsIgnoreCase(MMJDBCSQLTypeInfo.INTEGER_CLASS)) {
            targetObject = DataTypeTransformer.getInteger(value);
        } else if(javaClassName.equalsIgnoreCase(MMJDBCSQLTypeInfo.BYTE_CLASS)) {
            targetObject = DataTypeTransformer.getByte(value);
        } else if(javaClassName.equalsIgnoreCase(MMJDBCSQLTypeInfo.SHORT_CLASS)) {
            targetObject = DataTypeTransformer.getShort(value);
        } else if(javaClassName.equalsIgnoreCase(MMJDBCSQLTypeInfo.LONG_CLASS)) {
            targetObject = DataTypeTransformer.getLong(value);
        } else if(javaClassName.equalsIgnoreCase(MMJDBCSQLTypeInfo.FLOAT_CLASS)) {
            targetObject = DataTypeTransformer.getFloat(value);
        } else if(javaClassName.equalsIgnoreCase(MMJDBCSQLTypeInfo.DOUBLE_CLASS)) {
            targetObject = DataTypeTransformer.getDouble(value);
        } else if(javaClassName.equalsIgnoreCase(MMJDBCSQLTypeInfo.BOOLEAN_CLASS)) {
            targetObject = DataTypeTransformer.getBoolean(value);
        } else if(javaClassName.equalsIgnoreCase(MMJDBCSQLTypeInfo.BIGDECIMAL_CLASS)) {
            targetObject = DataTypeTransformer.getBigDecimal(value);
        } else if(javaClassName.equalsIgnoreCase(MMJDBCSQLTypeInfo.TIMESTAMP_CLASS)) {
            targetObject = DataTypeTransformer.getTimestamp(value);
        } else if(javaClassName.equalsIgnoreCase(MMJDBCSQLTypeInfo.DATE_CLASS)) {
            targetObject = DataTypeTransformer.getDate(value);
        } else if(javaClassName.equalsIgnoreCase(MMJDBCSQLTypeInfo.TIME_CLASS)) {
            targetObject = DataTypeTransformer.getTime(value);
        } else if (javaClassName.equalsIgnoreCase(MMJDBCSQLTypeInfo.BLOB_CLASS)) {
            targetObject = DataTypeTransformer.getBlob(value);
        } else if (javaClassName.equalsIgnoreCase(MMJDBCSQLTypeInfo.CLOB_CLASS)) {
            targetObject = DataTypeTransformer.getClob(value);
        } else {
            String msg = JDBCPlugin.Util.getString("MMPreparedStatement.Err_transform_obj"); //$NON-NLS-1$
            throw new MMSQLException(msg);
        }

        setObject(parameterIndex, targetObject);
    }

    /**
     * <p>Sets parameter number parameterIndex to an object value
     * @param parameterIndex of the parameter whose value is to be set
     * @param an object value to which the parameter value is to be set.
     * @throws SQLException, if there is an error setting the parameter value
     */
    public void setObject(int parameterIndex, Object value) throws SQLException {
        ArgCheck.isPositive(parameterIndex, JDBCPlugin.Util.getString("MMPreparedStatement.Invalid_param_index")); //$NON-NLS-1$

        if(parameterMap == null){
            parameterMap = new TreeMap<Integer, Object>();
        }
        
        if (serverCalendar != null && value instanceof java.util.Date) {
            value = TimestampWithTimezone.create((java.util.Date)value, getDefaultCalendar().getTimeZone(), serverCalendar, value.getClass());
        }

        parameterMap.put(new Integer(parameterIndex), value);
    }

    /**
     * <p>Sets parameter number parameterIndex to x, a short value. The parameter
     * type is set to TINYINT
     * @param parameterIndex of the parameter whose value is to be set
     * @param short value to which the parameter value is to be set.
     * @throws SQLException, if there is an error setting the parameter value
     */
    public void setShort(int parameterIndex, short value) throws SQLException {
        setObject(parameterIndex, new Short(value));
    }

    /**
     * <p>Sets parameter number parameterIndex to x, a String value. The parameter
     * type is set to VARCHAR
     * @param parameterIndex of the parameter whose value is to be set
     * @param String object to which the parameter value is to be set.
     * @throws SQLException
     */
    public void setString(int parameterIndex, String value) throws SQLException {
        setObject(parameterIndex, value);
    }

    /**
     * <p>Sets parameter number parameterIndex to x, a java.sql.Time object. The parameter
     * type is set to TIME
     * @param parameterIndex of the parameter whose value is to be set
     * @param Time object to which the parameter value is to be set.
     * @throws SQLException, if parameter type/datatype do not match
     */
    public void setTime(int parameterIndex, java.sql.Time value) throws SQLException {
        setTime(parameterIndex, value, null);
    }

    /**
     * <p>Sets parameter number parameterIndex to x, a java.sql.Time object. The parameter
     * type is set to TIME
     * @param parameterIndex of the parameter whose value is to be set
     * @param Time object to which the parameter value is to be set.
     * @param Calendar object to constrct Time(useful to get include timezone info)
     * @throws SQLException, if parameter type/datatype do not match
     */
    public void setTime(int parameterIndex, java.sql.Time x, java.util.Calendar cal) throws SQLException {

       if (cal == null || x == null) {
           setObject(parameterIndex, x);
           return;
       }
               
       // set the parameter on the stored procedure
       setObject(parameterIndex, TimestampWithTimezone.createTime(x, cal.getTimeZone(), getDefaultCalendar()));
    }

    /**
     * <p>Sets parameter number parameterIndex to x, a java.sql.Timestamp object. The
     * parameter type is set to TIMESTAMP
     * @param parameterIndex of the parameter whose value is to be set
     * @param Timestamp object to which the parameter value is to be set.
     * @throws SQLException, if parameter type/datatype do not match
     */
    public void setTimestamp(int parameterIndex, java.sql.Timestamp value) throws SQLException {
        setTimestamp(parameterIndex, value, null);
    }

    /**
     * <p>Sets parameter number parameterIndex to x, a java.sql.Timestamp object. The
     * parameter type is set to TIMESTAMP
     * @param parameterIndex of the parameter whose value is to be set
     * @param Timestamp object to which the parameter value is to be set.
     * @param Calendar object to constrct timestamp(useful to get include timezone info)
     * @throws SQLException, if parameter type/datatype do not match
     */
    public void setTimestamp(int parameterIndex, java.sql.Timestamp x, java.util.Calendar cal) throws SQLException {

        if (cal == null || x == null) {
            setObject(parameterIndex, x);
            return;
        }
                
        // set the parameter on the stored procedure
        setObject(parameterIndex, TimestampWithTimezone.createTimestamp(x, cal.getTimeZone(), getDefaultCalendar()));
    }

    /**
     * Sets the designated parameter to the given java.net.URL value. The driver
     * converts this to an SQL DATALINK value when it sends it to the database.
     * @param parameter int index
     * @param x URL to be set
     * @throws SQLException
     */
    public void setURL(int parameterIndex, URL x) throws SQLException {
        setObject(parameterIndex, x);
    }

    /**
     * Internal Execution method.
     * @param sql Request Message's sql.
     * @param listener Message Listener
     * @throws SQLException thrown when fail to create the {@link AllResultsImpl}
     */
    protected ResultsMessage internalExecute(String[] commands, boolean isPreparedBatchUpdate) throws SQLException  {
        try {
        	return sendRequestMessageAndWait(commands, true, false, isPreparedBatchUpdate? getParameterValuesList(): getParameterValues(), false, isPreparedBatchUpdate);
        } catch ( Throwable ex ) {
            throw MMSQLException.create(ex, JDBCPlugin.Util.getString("MMStatement.Error_executing_stmt", commands[0])); //$NON-NLS-1$
        }
    }

    List getParameterValuesList() {
    	if(batchParameterList == null || batchParameterList.isEmpty()){
    		return Collections.EMPTY_LIST;
    	}
    	return new ArrayList(batchParameterList);
    }
    
    List getParameterValues() {
        if(parameterMap == null || parameterMap.isEmpty()){
            return Collections.EMPTY_LIST;
        }
        return new ArrayList(parameterMap.values());
    }

	/* (non-Javadoc)
	 * @see java.sql.PreparedStatement#getParameterMetaData()
	 */
	public ParameterMetaData getParameterMetaData() throws SQLException {
		/* Implement for JDBC 3.0 */
		return null;
	}

    /**
     * Exposed for unit testing 
     */
    void setServerCalendar(Calendar serverCalendar) {
        this.serverCalendar = serverCalendar;
    }

    public int[] executeBatch() throws SQLException {
    	if (batchParameterList == null || batchParameterList.isEmpty()) {
    	     return new int[0];
    	}
    	try{
    	 	executeUpdate(true);
    	}finally{
    		batchParameterList.clear();
    	}
    	return updateCounts;
    }
    
	public void setSQLXML(int parameterIndex, SQLXML xmlObject) throws SQLException {
		setObject(parameterIndex, xmlObject);
	}

	public void setArray(int parameterIndex, Array x) throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	public void setAsciiStream(int parameterIndex, InputStream x)
			throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	public void setAsciiStream(int parameterIndex, InputStream x, long length)
			throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	public void setBinaryStream(int parameterIndex, InputStream x)
			throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	public void setBinaryStream(int parameterIndex, InputStream x, long length)
			throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	public void setBlob(int parameterIndex, InputStream inputStream)
			throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	public void setBlob(int parameterIndex, InputStream inputStream, long length)
			throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	public void setCharacterStream(int parameterIndex, Reader reader)
			throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	public void setCharacterStream(int parameterIndex, Reader reader,
			long length) throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	public void setClob(int parameterIndex, Reader reader) throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	public void setClob(int parameterIndex, Reader reader, long length)
			throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	public void setNCharacterStream(int parameterIndex, Reader value)
			throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	public void setNCharacterStream(int parameterIndex, Reader value,
			long length) throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	public void setNClob(int parameterIndex, NClob value) throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	public void setNClob(int parameterIndex, Reader reader) throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	public void setNClob(int parameterIndex, Reader reader, long length)
			throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	public void setNString(int parameterIndex, String value)
			throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	public void setRef(int parameterIndex, Ref x) throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	public void setRowId(int parameterIndex, RowId x) throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	public void setUnicodeStream(int parameterIndex, InputStream x, int length)
			throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}
}
