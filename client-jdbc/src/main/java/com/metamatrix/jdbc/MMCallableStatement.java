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
import java.sql.Array;
import java.sql.Blob;
import java.sql.CallableStatement;
import java.sql.Clob;
import java.sql.Date;
import java.sql.Ref;
import java.sql.SQLException;
//## JDBC4.0-begin ##
import java.sql.SQLXML;
import java.sql.NClob;
import java.sql.RowId;
//## JDBC4.0-end ##

/*## JDBC3.0-JDK1.5-begin ##
import com.metamatrix.core.jdbc.SQLXML; 
## JDBC3.0-JDK1.5-end ##*/

import java.sql.Time;
import java.sql.Timestamp;
import java.util.Calendar;
import java.util.Map;

import com.metamatrix.common.util.SqlUtil;
import com.metamatrix.common.util.TimestampWithTimezone;
import com.metamatrix.dqp.message.RequestMessage;

/**
 * <p> This class inherits Statement methods, which deal with SQL statements in
 * general and also inherits PreparedStatement methods, which deal with IN parameters.
 * This object provides a way to call stored procedures in MetaMatrix.  This call
 * is written in an escape syntax that may take one of two forms: one form with
 * result parameter, and the other without one.  A result parameter, a kind of OUT
 * parameter is the return value for the stored procedure.  Both forms may have a
 * variable number of parameters used for input(IN parameters), output(OUT parameters),
 * or both (INOUT parameters).</p>
 * <p> The methods in this class can be used to retrieve values of OUT parameters or
 * the output aspect of INOUT parameters.</p>
 */
public class MMCallableStatement extends MMPreparedStatement implements CallableStatement {

    // object representing parameter value
    private Object parameterValue;

    /**
     * Factory Constructor (be sure to cast it to  MMCallableStatement)
     */
    static MMCallableStatement newInstance(MMConnection connection, String procedureCall, int resultSetType, int resultSetConcurrency) throws SQLException {
        return new MMCallableStatement(connection, procedureCall, resultSetType, resultSetConcurrency);        
    }
    
    /**
     * <p>MMCallableStatement constructor that sets the procedureName, IN parameters
     * and OUT parameters on this object.
     * @param Driver's connection object which creates this object.
     * @param procedureCall string
     * @throws SQLException if there is an error parsing the call
     */
    MMCallableStatement(MMConnection connection, String procedureCall, int resultSetType, int resultSetConcurrency) throws SQLException {
        // set the connection on the super class
        super(connection, procedureCall, resultSetType, resultSetConcurrency);
        this.prepareSql = procedureCall;
    }
    
    @Override
    protected RequestMessage createRequestMessage(String[] commands,
    		boolean isBatchedCommand, Boolean requiresResultSet) {
    	RequestMessage message = super.createRequestMessage(commands, isBatchedCommand, requiresResultSet);
    	message.setCallableStatement(true);
    	message.setPreparedStatement(false);
    	return message;
    }
    
    /**
     * In many cases, it is desirable to immediately release a Statements's database
     * and JDBC resources instead of waiting for this to happen when it is automatically
     * closed; the close method provides this immediate release.
     * @throws SQLException should never occur.
     */
    public void close() throws SQLException {
        this.prepareSql = null;
        super.close();
    }

    /**
     * <p>Gets the value of a OUTPUT parameter as a java.math.BigDecimal object with
     * scale digits to the right of the decimal point.
     * @param parameterIndex whose value is to be fetched from the result.
     * @return The parameter at the given index is returned as an BigDecimal object.
     * @throws SQLException if param datatype is not NUMERIC
     * @deprecated
     */
    public java.math.BigDecimal getBigDecimal(int parameterIndex, int scale) throws SQLException {
        BigDecimal bigDecimalParam = DataTypeTransformer.getBigDecimal(getObject(parameterIndex));

        // set scale on the param value
        bigDecimalParam.setScale(scale);

        // return param value
        return bigDecimalParam;
    }
    
    /**
     * <p>Gets the value of a OUTPUT parameter as a java.math.BigDecimal object.
     * @param parameterIndex whose value is to be fetched from the result.
     * @return The parameter at the given index is returned as an BigDecimal object.
     * @throws SQLException if param datatype is not NUMERIC
     */
    public java.math.BigDecimal getBigDecimal(int parameterIndex) throws SQLException {
        return DataTypeTransformer.getBigDecimal(getObject(parameterIndex));
    }

    /**
     * <p>Gets the value of a OUTPUT parameter as a java.sql.Blob object.
     * @param parameterIndex whose value is to be fetched from the result.
     * @return The parameter at the given index is returned as a Blob object.
     */
    public Blob getBlob(int parameterIndex) throws SQLException {
        return DataTypeTransformer.getBlob(getObject(parameterIndex));
    }

    /**
     * <p>Gets the value of a OUTPUT parameter as a boolean.
     * @param parameterIndex whose value is to be fetched from the result.
     * @return The parameter at the given index is returned as a boolean value.
     * @throws SQLException if param datatype is not BIT
     */
    public boolean getBoolean(int parameterIndex) throws SQLException {
        return DataTypeTransformer.getBoolean(getObject(parameterIndex));
    }

    /**
     * <p>Gets the value of a OUTPUT parameter as a byte.
     * @param parameterIndex whose value is to be fetched from the result.
     * @return The parameter at the given index is returned as a byte value.
     * @throws SQLException if param datatype is not TINYINT
     */
    public byte getByte(int parameterIndex) throws SQLException {
        return DataTypeTransformer.getByte(getObject(parameterIndex));
    }

    /**
     * <p>Gets the value of a OUTPUT parameter as a java.sql.Clob object.
     * @param parameterIndex whose value is to be fetched from the result.
     * @return The parameter at the given index is returned as a Clob object.
     */
    public Clob getClob(int parameterIndex) throws SQLException {
        return DataTypeTransformer.getClob(getObject(parameterIndex));
    }

    /**
     * <p>Gets the value of a OUTPUT parameter as a java.sql.Date object.
     * @param parameterIndex whose value is to be fetched from the result.
     * @return The parameter at the given index is returned as a Date object.
     * @throws SQLException if param datatype is not DATE
     */
    public java.sql.Date getDate(int parameterIndex) throws SQLException {
        return getDate(parameterIndex, null);
    }

    /**
     * <p>Gets the value of a OUTPUT parameter as a java.sql.Date object. Calender
     * object contains the timezone info for the Date.
     * @param parameterIndex whose value is to be fetched from the result.
     * @param Calendar object used to construct the Date object.
     * @return The parameter at the given index is returned as a Date object.
     * @throws SQLException if param datatype is not DATE
     */
    public java.sql.Date getDate(int parameterIndex, Calendar cal) throws SQLException {
        Date value = DataTypeTransformer.getDate(getObject(parameterIndex));

        if (value == null) {
            return null;
        }
        
        if (cal != null) {
            value = TimestampWithTimezone.createDate(value, getDefaultCalendar().getTimeZone(), cal);
        }
        
        return value;
    }

    /**
     * <p>Gets the value of a OUTPUT parameter as a double.
     * @param parameterIndex whose value is to be fetched from the result.
     * @return The parameter at the given index is returned as a double value.
     * @throws SQLException if param datatype is not DOUBLE or FLOAT
     */
    public double getDouble(int parameterIndex) throws SQLException {
        return DataTypeTransformer.getDouble(getObject(parameterIndex));
    }

    /**
     * <p>Gets the value of a OUTPUT parameter as a float.
     * @param parameterIndex whose value is to be fetched from the result.
     * @return The parameter at the given index is returned as a float value.
     * @throws SQLException if param datatype is not FLOAT
     */
    public float getFloat(int parameterIndex) throws SQLException {
        return DataTypeTransformer.getFloat(getObject(parameterIndex));
    }

    /**
     * <p>Gets the value of a OUTPUT parameter as a int.
     * @param parameterIndex whose value is to be fetched from the result.
     * @return The parameter at the given index is returned as a int value.
     * @throws SQLException if param datatype is not INTEGER
     */
    public int getInt(int parameterIndex) throws SQLException {
        return DataTypeTransformer.getInteger(getObject(parameterIndex));
    }

    /**
     * <p>Gets the value of a OUTPUT parameter as a long.
     * @param parameterIndex whose value is to be fetched from the result.
     * @return The parameter at the given index is returned as a long value.
     * @throws SQLException if param datatype is not BIGINT
     */
    public long getLong(int parameterIndex) throws SQLException {
        return DataTypeTransformer.getLong(getObject(parameterIndex));
    }

    /**
     * <p>Gets the value of a OUTPUT parameter as an object.
     * @param parameterIndex whose value is to be fetched from the result.
     * @return The parameter at the given index is returned as an object.
     * @throws SQLException
     */
    public Object getObject(int parameterIndex) throws SQLException {
        //checkParameter(parameterIndex);

        Object indexInResults = this.outParamIndexMap.get(new Integer(parameterIndex));
        if(indexInResults == null){
            throw new IllegalArgumentException(JDBCPlugin.Util.getString("MMCallableStatement.Param_not_found", parameterIndex)); //$NON-NLS-1$
        }
        checkStatement();
        parameterValue = resultSet.getOutputParamValue(((Integer)indexInResults).intValue());
        return parameterValue;
    }

    /**
     * <p>Gets the value of a OUTPUT parameter as a short.
     * @param parameterIndex whose value is to be fetched from the result.
     * @return The parameter at the given index is returned as a short value.
     * @throws SQLException if param datatype is not SMALLINT
     */
    public short getShort(int parameterIndex) throws SQLException {
        return DataTypeTransformer.getShort(getObject(parameterIndex));
    }

    /**
     * <p>Gets the value of a OUTPUT parameter as a String.
     * @param parameterIndex whose value is to be fetched from the result.
     * @return The parameter at the given index is returned as a String object.
     * @throws SQLException if param datatype is not CHAR, VARCHAR, LONGVARCHAR
     */
    public String getString(int parameterIndex) throws SQLException {
        // return the parameter value a String object
       return getObject(parameterIndex).toString();
    }

    /**
     * <p>Gets the value of a OUTPUT parameter as a java.sql.Time object.
     * @param parameterIndex whose value is to be fetched from the result.
     * @return The parameter at the given index is returned as a Time object.
     * @throws SQLException if param datatype is not TIME
     */
    public Time getTime(int parameterIndex) throws SQLException {
        return getTime(parameterIndex, null);
    }

    /**
     * <p>Gets the value of a OUTPUT parameter as a java.sql.Timestamp object. Calendar
     * object contains the timezone information.
     * @param parameterIndex whose value is to be fetched from the result.
     * @param Calendar object used to construct the Date object.
     * @return The parameter at the given index is returned as a Time object.
     * @throws SQLException if param datatype is not TIME
     */
    public Time getTime(int parameterIndex, java.util.Calendar cal) throws SQLException {
        Time value = DataTypeTransformer.getTime(getObject(parameterIndex));

        if (value == null) {
            return null;
        }
        
        if (cal != null) {
            value = TimestampWithTimezone.createTime(value, getDefaultCalendar().getTimeZone(), cal);
        }
        
        return value;
    }

    /**
     * <p>Gets the value of a OUTPUT parameter as a java.sql.Timestamp object.
     * @param parameterIndex whose value is to be fetched from the result.
     * @return The parameter at the given index is returned as a Timestamp object.
     * @throws SQLException if param datatype is not TIMESTAMP
     */
    public Timestamp getTimestamp(int parameterIndex) throws SQLException {
        return getTimestamp(parameterIndex, null);
    }

    /**
     * <p>Gets the value of a OUTPUT parameter as a java.sql.Timestamp object. Calendar
     * object contains the timezone information.
     * @param parameterIndex whose value is to be fetched from the result.
     * @param Calendar object used to construct the Date object.
     * @return The parameter at the given index is returned as a Timestamp object.
     * @throws SQLException if param datatype is not TIMESTAMP
     */
    public Timestamp getTimestamp(int parameterIndex, java.util.Calendar cal) throws SQLException {
        Timestamp value = DataTypeTransformer.getTimestamp(getObject(parameterIndex));

        if (value == null) {
            return null;
        }
        
        if (cal != null) {
            value = TimestampWithTimezone.createTimestamp(value, getDefaultCalendar().getTimeZone(), cal);
        }
        
        return value;
    }

    /**
     * <p>Register the OUT parameter in the ordinal position parameterIndex to jdbcsql
     * type. Scale is used by setXXX methods to determine number of decimals.
     * @param parameterIndex. Index of the OUT parameter in the stored procedure.
     * @param jdbcSqlType. SQL type codes from java.sql.Types
     * @param SQLException, should never occur
     */
    public void registerOutParameter(int parameterIndex, int jdbcSqlType) throws SQLException {
        // ignore - we don't care
    }

    /**
     * <p>Register the OUT parameter in the ordinal position parameterIndex to jdbcsql
     * type. Scale is used by setXXX methods to determine number of decimals.
     * @param parameterIndex. Index of the OUT parameter in the stored procedure.
     * @param jdbcSqlType. SQL type codes from java.sql.Types
     * @param scale. The number of decimal digits on the OUT param.
     * @param SQLException, should never occur
     */
    public void registerOutParameter(int parameterIndex, int jdbcSqlType, int scale) throws SQLException {
        // ignore - we don't care
    }

    /**
     * <p>Register the OUT parameter in the ordinal position parameterIndex to jdbcsql
     * type. The param typename(SQL name for user-named type) is ignored as SQL3
     * datatypes are not supported.
     * @param parameterIndex. Index of the OUT parameter in the stored procedure.
     * @param jdbcSqlType. SQL type codes from java.sql.Types
     * @param typeName. SQL name of user-named type being used
     * @param SQLException, should never occur
     */
    public void registerOutParameter (int parameterIndex, int jdbcSqlType, String typeName) throws SQLException {
        // ignore - we don't care
    }
   
    /**
     * <p>Indicates whether the last OUT parameter read was a return null.
     * @return true if the last param read was null else false.
     * @throws SQLException, if the statement is already closed.
     */
    public boolean wasNull() throws SQLException {
        checkStatement();

        return parameterValue == null;
    }

	public SQLXML getSQLXML(int parameterIndex) throws SQLException {
		return DataTypeTransformer.getSQLXML(getObject(parameterIndex));
	}

	public Array getArray(int parameterIndex) throws SQLException {
		throw SqlUtil.createFeatureNotSupportedException();
	}

	public Array getArray(String parameterName) throws SQLException {
		throw SqlUtil.createFeatureNotSupportedException();
	}

	public BigDecimal getBigDecimal(String parameterName) throws SQLException {
		throw SqlUtil.createFeatureNotSupportedException();
	}

	public Blob getBlob(String parameterName) throws SQLException {
		throw SqlUtil.createFeatureNotSupportedException();
	}
	
	public boolean getBoolean(String parameterName) throws SQLException {
		throw SqlUtil.createFeatureNotSupportedException();
	}

	public byte getByte(String parameterName) throws SQLException {
		throw SqlUtil.createFeatureNotSupportedException();
	}

	public byte[] getBytes(int parameterIndex) throws SQLException {
		throw SqlUtil.createFeatureNotSupportedException();
	}

	public byte[] getBytes(String parameterName) throws SQLException {
		throw SqlUtil.createFeatureNotSupportedException();
	}

	public Reader getCharacterStream(int parameterIndex) throws SQLException {
		throw SqlUtil.createFeatureNotSupportedException();
	}

	public Reader getCharacterStream(String parameterName) throws SQLException {
		throw SqlUtil.createFeatureNotSupportedException();
	}

	public Clob getClob(String parameterName) throws SQLException {
		throw SqlUtil.createFeatureNotSupportedException();
	}

	public Date getDate(String parameterName) throws SQLException {
		throw SqlUtil.createFeatureNotSupportedException();
	}

	public Date getDate(String parameterName, Calendar cal) throws SQLException {
		throw SqlUtil.createFeatureNotSupportedException();
	}

	public double getDouble(String parameterName) throws SQLException {
		throw SqlUtil.createFeatureNotSupportedException();
	}

	public float getFloat(String parameterName) throws SQLException {
		throw SqlUtil.createFeatureNotSupportedException();
	}

	public int getInt(String parameterName) throws SQLException {
		throw SqlUtil.createFeatureNotSupportedException();
	}

	public long getLong(String parameterName) throws SQLException {
		throw SqlUtil.createFeatureNotSupportedException();
	}

	public Reader getNCharacterStream(int parameterIndex) throws SQLException {
		throw SqlUtil.createFeatureNotSupportedException();
	}

	public Reader getNCharacterStream(String parameterName) throws SQLException {
		throw SqlUtil.createFeatureNotSupportedException();
	}

	//## JDBC4.0-begin ##
	public NClob getNClob(int parameterIndex) throws SQLException {
		throw SqlUtil.createFeatureNotSupportedException();
	}
	//## JDBC4.0-end ##

	//## JDBC4.0-begin ##
	public NClob getNClob(String parameterName) throws SQLException {
		throw SqlUtil.createFeatureNotSupportedException();
	}
	//## JDBC4.0-end ##

	public String getNString(int parameterIndex) throws SQLException {
		throw SqlUtil.createFeatureNotSupportedException();
	}

	public String getNString(String parameterName) throws SQLException {
		throw SqlUtil.createFeatureNotSupportedException();
	}

	public Object getObject(String parameterName) throws SQLException {
		throw SqlUtil.createFeatureNotSupportedException();
	}

	public Object getObject(int parameterIndex, Map<String, Class<?>> map) throws SQLException {
		throw SqlUtil.createFeatureNotSupportedException();
	}

	public Object getObject(String parameterName, Map<String, Class<?>> map) throws SQLException {
		throw SqlUtil.createFeatureNotSupportedException();
	}

	public Ref getRef(int parameterIndex) throws SQLException {
		throw SqlUtil.createFeatureNotSupportedException();
	}

	public Ref getRef(String parameterName) throws SQLException {
		throw SqlUtil.createFeatureNotSupportedException();
	}

	//## JDBC4.0-begin ##    
	public RowId getRowId(int parameterIndex) throws SQLException {
		throw SqlUtil.createFeatureNotSupportedException();
	}
    //## JDBC4.0-end ##

	//## JDBC4.0-begin ##
	public RowId getRowId(String parameterName) throws SQLException {
		throw SqlUtil.createFeatureNotSupportedException();
	}
	//## JDBC4.0-end ##
	
	public SQLXML getSQLXML(String parameterName) throws SQLException {
		throw SqlUtil.createFeatureNotSupportedException();
	}

	public short getShort(String parameterName) throws SQLException {
		throw SqlUtil.createFeatureNotSupportedException();
	}

	public String getString(String parameterName) throws SQLException {
		throw SqlUtil.createFeatureNotSupportedException();
	}

	public Time getTime(String parameterName) throws SQLException {
		throw SqlUtil.createFeatureNotSupportedException();
	}

	public Time getTime(String parameterName, Calendar cal) throws SQLException {
		throw SqlUtil.createFeatureNotSupportedException();
	}

	public Timestamp getTimestamp(String parameterName) throws SQLException {
		throw SqlUtil.createFeatureNotSupportedException();
	}

	public Timestamp getTimestamp(String parameterName, Calendar cal) throws SQLException {
		throw SqlUtil.createFeatureNotSupportedException();
	}

	public URL getURL(int parameterIndex) throws SQLException {
		throw SqlUtil.createFeatureNotSupportedException();
	}

	public URL getURL(String parameterName) throws SQLException {
		throw SqlUtil.createFeatureNotSupportedException();
	}

	public void registerOutParameter(String parameterName, int sqlType)	throws SQLException {
		throw SqlUtil.createFeatureNotSupportedException();
	}

	public void registerOutParameter(String parameterName, int sqlType,int scale) throws SQLException {
		throw SqlUtil.createFeatureNotSupportedException();
	}

	public void registerOutParameter(String parameterName, int sqlType,	String typeName) throws SQLException {
		throw SqlUtil.createFeatureNotSupportedException();
	}

	public void setAsciiStream(String parameterName, InputStream x)	throws SQLException {
		throw SqlUtil.createFeatureNotSupportedException();
	}

	public void setAsciiStream(String parameterName, InputStream x, int length)	throws SQLException {
		throw SqlUtil.createFeatureNotSupportedException();
	}

	public void setAsciiStream(String parameterName, InputStream x, long length) throws SQLException {
		throw SqlUtil.createFeatureNotSupportedException();
	}

	public void setBigDecimal(String parameterName, BigDecimal x) throws SQLException {
		throw SqlUtil.createFeatureNotSupportedException();
	}

	public void setBinaryStream(String parameterName, InputStream x) throws SQLException {
		throw SqlUtil.createFeatureNotSupportedException();
	}

	public void setBinaryStream(String parameterName, InputStream x, int length)
			throws SQLException {
		throw SqlUtil.createFeatureNotSupportedException();
	}

	public void setBinaryStream(String parameterName, InputStream x, long length)
			throws SQLException {
		throw SqlUtil.createFeatureNotSupportedException();
	}

	public void setBlob(String parameterName, Blob x) throws SQLException {
		throw SqlUtil.createFeatureNotSupportedException();
	}

	public void setBlob(String parameterName, InputStream inputStream)
			throws SQLException {
		throw SqlUtil.createFeatureNotSupportedException();
	}

	public void setBlob(String parameterName, InputStream inputStream,
			long length) throws SQLException {
		throw SqlUtil.createFeatureNotSupportedException();
	}

	public void setBoolean(String parameterName, boolean x) throws SQLException {
		throw SqlUtil.createFeatureNotSupportedException();
	}

	public void setByte(String parameterName, byte x) throws SQLException {
		throw SqlUtil.createFeatureNotSupportedException();
	}

	public void setBytes(String parameterName, byte[] x) throws SQLException {
		throw SqlUtil.createFeatureNotSupportedException();
	}

	public void setCharacterStream(String parameterName, Reader reader)
			throws SQLException {
		throw SqlUtil.createFeatureNotSupportedException();	}

	public void setCharacterStream(String parameterName, Reader reader,
			int length) throws SQLException {
		throw SqlUtil.createFeatureNotSupportedException();
	}

	public void setCharacterStream(String parameterName, Reader reader,
			long length) throws SQLException {
		throw SqlUtil.createFeatureNotSupportedException();
	}

	public void setClob(String parameterName, Clob x) throws SQLException {
		throw SqlUtil.createFeatureNotSupportedException();
	}

	public void setClob(String parameterName, Reader reader)
			throws SQLException {
		throw SqlUtil.createFeatureNotSupportedException();
	}

	public void setClob(String parameterName, Reader reader, long length)
			throws SQLException {
		throw SqlUtil.createFeatureNotSupportedException();
	}

	public void setDate(String parameterName, Date x) throws SQLException {
		throw SqlUtil.createFeatureNotSupportedException();
	}

	public void setDate(String parameterName, Date x, Calendar cal)
			throws SQLException {
		throw SqlUtil.createFeatureNotSupportedException();
	}

	public void setDouble(String parameterName, double x) throws SQLException {
		throw SqlUtil.createFeatureNotSupportedException();
	}

	public void setFloat(String parameterName, float x) throws SQLException {
		throw SqlUtil.createFeatureNotSupportedException();
	}

	public void setInt(String parameterName, int x) throws SQLException {
		throw SqlUtil.createFeatureNotSupportedException();
	}

	public void setLong(String parameterName, long x) throws SQLException {
		throw SqlUtil.createFeatureNotSupportedException();
	}

	public void setNCharacterStream(String parameterName, Reader value)
			throws SQLException {
		throw SqlUtil.createFeatureNotSupportedException();
	}

	public void setNCharacterStream(String parameterName, Reader value,
			long length) throws SQLException {
		throw SqlUtil.createFeatureNotSupportedException();
	}

	//## JDBC4.0-begin ##
	public void setNClob(String parameterName, NClob value) throws SQLException {
		throw SqlUtil.createFeatureNotSupportedException();
	}
	//## JDBC4.0-end ##

	public void setNClob(String parameterName, Reader reader)
			throws SQLException {
		throw SqlUtil.createFeatureNotSupportedException();
	}

	public void setNClob(String parameterName, Reader reader, long length)
			throws SQLException {
		throw SqlUtil.createFeatureNotSupportedException();
	}

	public void setNString(String parameterName, String value)
			throws SQLException {
		throw SqlUtil.createFeatureNotSupportedException();
	}

	public void setNull(String parameterName, int sqlType) throws SQLException {
		throw SqlUtil.createFeatureNotSupportedException();
	}		

	public void setNull(String parameterName, int sqlType, String typeName)
			throws SQLException {
		throw SqlUtil.createFeatureNotSupportedException();
	}

	public void setObject(String parameterName, Object x) throws SQLException {
		throw SqlUtil.createFeatureNotSupportedException();
	}

	public void setObject(String parameterName, Object x, int targetSqlType)
			throws SQLException {
		throw SqlUtil.createFeatureNotSupportedException();
	}

	public void setObject(String parameterName, Object x, int targetSqlType,
			int scale) throws SQLException {
		throw SqlUtil.createFeatureNotSupportedException();
	}

	//## JDBC4.0-begin ##
	public void setRowId(String parameterName, RowId x) throws SQLException {
		throw SqlUtil.createFeatureNotSupportedException();
	}
	//## JDBC4.0-end ##

	public void setSQLXML(String parameterName, SQLXML xmlObject)
			throws SQLException {
		throw SqlUtil.createFeatureNotSupportedException();
	}


	public void setShort(String parameterName, short x) throws SQLException {
		throw SqlUtil.createFeatureNotSupportedException();
	}

	public void setString(String parameterName, String x) throws SQLException {
		throw SqlUtil.createFeatureNotSupportedException();
	}

	public void setTime(String parameterName, Time x) throws SQLException {
		throw SqlUtil.createFeatureNotSupportedException();
	}

	public void setTime(String parameterName, Time x, Calendar cal)
			throws SQLException {
		throw SqlUtil.createFeatureNotSupportedException();
	}

	public void setTimestamp(String parameterName, Timestamp x)
			throws SQLException {
		throw SqlUtil.createFeatureNotSupportedException();
	}

	public void setTimestamp(String parameterName, Timestamp x, Calendar cal)
			throws SQLException {
		throw SqlUtil.createFeatureNotSupportedException();
	}

	public void setURL(String parameterName, URL val) throws SQLException {
		throw SqlUtil.createFeatureNotSupportedException();
	}

}