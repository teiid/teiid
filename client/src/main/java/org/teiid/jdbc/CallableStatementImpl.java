/*
 * Copyright Red Hat, Inc. and/or its affiliates
 * and other contributors as indicated by the @author tags and
 * the COPYRIGHT.txt file distributed with this work.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.teiid.jdbc;

import java.io.InputStream;
import java.io.Reader;
import java.math.BigDecimal;
import java.net.URL;
import java.sql.*;
import java.util.Calendar;
import java.util.Map;

import org.teiid.client.RequestMessage;
import org.teiid.client.RequestMessage.ResultsMode;
import org.teiid.client.RequestMessage.StatementType;
import org.teiid.core.util.SqlUtil;
import org.teiid.core.util.TimestampWithTimezone;


public class CallableStatementImpl extends PreparedStatementImpl implements CallableStatement {

    // object representing parameter value
    private Object parameterValue;

    /**
     * <p>MMCallableStatement constructor that sets the procedureName, IN parameters
     * and OUT parameters on this object.
     * @param connection object which creates this object.
     * @param procedureCall string
     * @throws SQLException if there is an error parsing the call
     */
    CallableStatementImpl(ConnectionImpl connection, String procedureCall, int resultSetType, int resultSetConcurrency) throws SQLException {
        // set the connection on the super class
        super(connection, procedureCall, resultSetType, resultSetConcurrency);
        this.prepareSql = procedureCall;
    }

    @Override
    protected void resetExecutionState() throws SQLException {
        super.resetExecutionState();
        parameterValue = null;
    }

    @Override
    protected RequestMessage createRequestMessage(String[] commands,
            boolean isBatchedCommand, ResultsMode resultsMode) {
        RequestMessage message = super.createRequestMessage(commands, isBatchedCommand, resultsMode);
        message.setStatementType(StatementType.CALLABLE);
        return message;
    }

    public void close() throws SQLException {
        this.prepareSql = null;
        super.close();
    }

    public java.math.BigDecimal getBigDecimal(int parameterIndex, int scale) throws SQLException {
        BigDecimal bigDecimalParam = DataTypeTransformer.getBigDecimal(getObject(parameterIndex));

        // set scale on the param value
        return bigDecimalParam.setScale(scale);
    }

    public java.math.BigDecimal getBigDecimal(int parameterIndex) throws SQLException {
        return DataTypeTransformer.getBigDecimal(getObject(parameterIndex));
    }

    public Blob getBlob(int parameterIndex) throws SQLException {
        return DataTypeTransformer.getBlob(getObject(parameterIndex));
    }

    public boolean getBoolean(int parameterIndex) throws SQLException {
        return DataTypeTransformer.getBoolean(getObject(parameterIndex));
    }

    public byte getByte(int parameterIndex) throws SQLException {
        return DataTypeTransformer.getByte(getObject(parameterIndex));
    }

    public Clob getClob(int parameterIndex) throws SQLException {
        return DataTypeTransformer.getClob(getObject(parameterIndex));
    }

    public java.sql.Date getDate(int parameterIndex) throws SQLException {
        return getDate(parameterIndex, null);
    }

    public java.sql.Date getDate(int parameterIndex, Calendar cal) throws SQLException {
        Object val = getObject(parameterIndex);
        return getDate(cal, val);
    }

    private java.sql.Date getDate(Calendar cal, Object val) throws SQLException {
        Date value = DataTypeTransformer.getDate(val);

        if (value == null) {
            return null;
        }

        if (cal != null) {
            value = TimestampWithTimezone.createDate(value, getDefaultCalendar().getTimeZone(), cal);
        }

        return value;
    }

    public double getDouble(int parameterIndex) throws SQLException {
        return DataTypeTransformer.getDouble(getObject(parameterIndex));
    }

    public float getFloat(int parameterIndex) throws SQLException {
        return DataTypeTransformer.getFloat(getObject(parameterIndex));
    }

    public int getInt(int parameterIndex) throws SQLException {
        return DataTypeTransformer.getInteger(getObject(parameterIndex));
    }

    public long getLong(int parameterIndex) throws SQLException {
        return DataTypeTransformer.getLong(getObject(parameterIndex));
    }

    public Object getObject(int parameterIndex) throws SQLException {
        return getObject(Integer.valueOf(parameterIndex));
    }

    Object getObject(Object parameterIndex) throws SQLException {
        Integer indexInResults = null;
        if (parameterIndex instanceof String) {
            indexInResults = this.outParamByName.get(parameterIndex);
        } else {
            indexInResults = this.outParamIndexMap.get(parameterIndex);
        }
        if(indexInResults == null){
            throw new TeiidSQLException(JDBCPlugin.Util.getString("MMCallableStatement.Param_not_found", parameterIndex)); //$NON-NLS-1$
        }
        checkStatement();
        parameterValue = resultSet.getOutputParamValue(indexInResults);
        return parameterValue;
    }

    public short getShort(int parameterIndex) throws SQLException {
        return DataTypeTransformer.getShort(getObject(parameterIndex));
    }

    public String getString(int parameterIndex) throws SQLException {
       return DataTypeTransformer.getString(getObject(parameterIndex));
    }

    public Time getTime(int parameterIndex) throws SQLException {
        return getTime(parameterIndex, null);
    }

    public Time getTime(int parameterIndex, java.util.Calendar cal) throws SQLException {
        Object val = getObject(parameterIndex);
        return getTime(cal, val);
    }

    private Time getTime(java.util.Calendar cal, Object val)
            throws SQLException {
        Time value = DataTypeTransformer.getTime(val);

        if (value == null) {
            return null;
        }

        if (cal != null) {
            value = TimestampWithTimezone.createTime(value, getDefaultCalendar().getTimeZone(), cal);
        }

        return value;
    }

    public Timestamp getTimestamp(int parameterIndex) throws SQLException {
        return getTimestamp(parameterIndex, null);
    }

    public Timestamp getTimestamp(int parameterIndex, java.util.Calendar cal) throws SQLException {
        Object val = getObject(parameterIndex);
        return getTimestamp(cal, val);
    }

    private Timestamp getTimestamp(java.util.Calendar cal, Object val)
            throws SQLException {
        Timestamp value = DataTypeTransformer.getTimestamp(val);

        if (value == null) {
            return null;
        }

        if (cal != null) {
            value = TimestampWithTimezone.createTimestamp(value, getDefaultCalendar().getTimeZone(), cal);
        }

        return value;
    }

    public void registerOutParameter(int parameterIndex, int jdbcSqlType) throws SQLException {
        // ignore - we don't care
    }

    public void registerOutParameter(int parameterIndex, int jdbcSqlType, int scale) throws SQLException {
        // ignore - we don't care
    }

    public void registerOutParameter (int parameterIndex, int jdbcSqlType, String typeName) throws SQLException {
        // ignore - we don't care
    }

    public boolean wasNull() throws SQLException {
        checkStatement();

        return parameterValue == null;
    }

    public SQLXML getSQLXML(int parameterIndex) throws SQLException {
        return DataTypeTransformer.getSQLXML(getObject(parameterIndex));
    }

    public Array getArray(int parameterIndex) throws SQLException {
        return DataTypeTransformer.getArray(getObject(parameterIndex));
    }

    public Array getArray(String parameterName) throws SQLException {
        return DataTypeTransformer.getArray(getObject(parameterName));
    }

    public BigDecimal getBigDecimal(String parameterName) throws SQLException {
        return DataTypeTransformer.getBigDecimal(getObject(parameterName));
    }

    public Blob getBlob(String parameterName) throws SQLException {
        return DataTypeTransformer.getBlob(getObject(parameterName));
    }

    public boolean getBoolean(String parameterName) throws SQLException {
        return DataTypeTransformer.getBoolean(getObject(parameterName));
    }

    public byte getByte(String parameterName) throws SQLException {
        return DataTypeTransformer.getByte(getObject(parameterName));
    }

    public byte[] getBytes(int parameterIndex) throws SQLException {
        return DataTypeTransformer.getBytes(getObject(parameterIndex));
    }

    public byte[] getBytes(String parameterName) throws SQLException {
        return DataTypeTransformer.getBytes(getObject(parameterName));
    }

    public Reader getCharacterStream(int parameterIndex) throws SQLException {
        return DataTypeTransformer.getCharacterStream(getObject(parameterIndex));
    }

    public Reader getCharacterStream(String parameterName) throws SQLException {
        return DataTypeTransformer.getCharacterStream(getObject(parameterName));
    }

    public Clob getClob(String parameterName) throws SQLException {
        return DataTypeTransformer.getClob(getObject(parameterName));
    }

    public Date getDate(String parameterName) throws SQLException {
        return DataTypeTransformer.getDate(getObject(parameterName));
    }

    public Date getDate(String parameterName, Calendar cal) throws SQLException {
        return getDate(cal, getObject(parameterName));
    }

    public double getDouble(String parameterName) throws SQLException {
        return DataTypeTransformer.getDouble(getObject(parameterName));
    }

    public float getFloat(String parameterName) throws SQLException {
        return DataTypeTransformer.getFloat(getObject(parameterName));
    }

    public int getInt(String parameterName) throws SQLException {
        return DataTypeTransformer.getInteger(getObject(parameterName));
    }

    public long getLong(String parameterName) throws SQLException {
        return DataTypeTransformer.getLong(getObject(parameterName));
    }

    public Reader getNCharacterStream(int parameterIndex) throws SQLException {
        return DataTypeTransformer.getCharacterStream(getObject(parameterIndex));
    }

    public Reader getNCharacterStream(String parameterName) throws SQLException {
        return DataTypeTransformer.getCharacterStream(getObject(parameterName));
    }

    public NClob getNClob(int parameterIndex) throws SQLException {
        throw SqlUtil.createFeatureNotSupportedException();
    }

    public NClob getNClob(String parameterName) throws SQLException {
        return DataTypeTransformer.getNClob(getObject(parameterName));
    }

    public String getNString(int parameterIndex) throws SQLException {
        return DataTypeTransformer.getString(getObject(parameterIndex));
    }

    public String getNString(String parameterName) throws SQLException {
        return DataTypeTransformer.getString(getObject(parameterName));
    }

    public Object getObject(String parameterName) throws SQLException {
        return getObject((Object)parameterName);
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

    public RowId getRowId(int parameterIndex) throws SQLException {
        throw SqlUtil.createFeatureNotSupportedException();
    }

    public RowId getRowId(String parameterName) throws SQLException {
        throw SqlUtil.createFeatureNotSupportedException();
    }

    public SQLXML getSQLXML(String parameterName) throws SQLException {
        return DataTypeTransformer.getSQLXML(getObject(parameterName));
    }

    public short getShort(String parameterName) throws SQLException {
        return DataTypeTransformer.getShort(getObject(parameterName));
    }

    public String getString(String parameterName) throws SQLException {
        return DataTypeTransformer.getString(getObject(parameterName));
    }

    public Time getTime(String parameterName) throws SQLException {
        return DataTypeTransformer.getTime(getObject(parameterName));
    }

    public Time getTime(String parameterName, Calendar cal) throws SQLException {
        return getTime(cal, getObject(parameterName));
    }

    public Timestamp getTimestamp(String parameterName) throws SQLException {
        return DataTypeTransformer.getTimestamp(getObject(parameterName));
    }

    public Timestamp getTimestamp(String parameterName, Calendar cal) throws SQLException {
        return getTimestamp(cal, getObject(parameterName));
    }

    public URL getURL(int parameterIndex) throws SQLException {
        throw SqlUtil.createFeatureNotSupportedException();
    }

    public URL getURL(String parameterName) throws SQLException {
        throw SqlUtil.createFeatureNotSupportedException();
    }

    public void registerOutParameter(String parameterName, int sqlType)    throws SQLException {
    }

    public void registerOutParameter(String parameterName, int sqlType,int scale) throws SQLException {
    }

    public void registerOutParameter(String parameterName, int sqlType,    String typeName) throws SQLException {
    }

    public void setAsciiStream(String parameterName, InputStream x)    throws SQLException {
        setAsciiStream((Object)parameterName, x);
    }

    public void setAsciiStream(String parameterName, InputStream x, int length)    throws SQLException {
        setAsciiStream((Object)parameterName, x);
    }

    public void setAsciiStream(String parameterName, InputStream x, long length) throws SQLException {
        setAsciiStream((Object)parameterName, x);
    }

    public void setBigDecimal(String parameterName, BigDecimal x) throws SQLException {
        setObject(parameterName, x);
    }

    public void setBinaryStream(String parameterName, InputStream x) throws SQLException {
        setBlob((Object)parameterName, x);
    }

    public void setBinaryStream(String parameterName, InputStream x, int length)
            throws SQLException {
        setBinaryStream(parameterName, x);
    }

    public void setBinaryStream(String parameterName, InputStream x, long length)
            throws SQLException {
        setBinaryStream(parameterName, x);
    }

    public void setBlob(String parameterName, Blob x) throws SQLException {
        setObject(parameterName, x);
    }

    public void setBlob(String parameterName, InputStream inputStream)
            throws SQLException {
        setBlob((Object)parameterName, inputStream);
    }

    public void setBlob(String parameterName, InputStream inputStream,
            long length) throws SQLException {
        setBlob((Object)parameterName, inputStream);
    }

    public void setBoolean(String parameterName, boolean x) throws SQLException {
        setObject((Object)parameterName, x);
    }

    public void setByte(String parameterName, byte x) throws SQLException {
        setObject((Object)parameterName, x);
    }

    public void setBytes(String parameterName, byte[] x) throws SQLException {
        setObject((Object)parameterName, x);
    }

    public void setCharacterStream(String parameterName, Reader reader)
            throws SQLException {
        setClob(parameterName, reader);
    }

    public void setCharacterStream(String parameterName, Reader reader,
            int length) throws SQLException {
        setClob(parameterName, reader);
    }

    public void setCharacterStream(String parameterName, Reader reader,
            long length) throws SQLException {
        setClob(parameterName, reader);
    }

    public void setClob(String parameterName, Clob x) throws SQLException {
        setObject((Object)parameterName, x);
    }

    public void setClob(String parameterName, Reader reader)
            throws SQLException {
        setClob((Object)parameterName, reader);
    }

    public void setClob(String parameterName, Reader reader, long length)
            throws SQLException {
        setClob((Object)parameterName, reader);
    }

    public void setDate(String parameterName, Date x) throws SQLException {
        setObject((Object)parameterName, x);
    }

    public void setDate(String parameterName, Date x, Calendar cal)
            throws SQLException {
        setDate((Object)parameterName, x, cal);
    }

    public void setDouble(String parameterName, double x) throws SQLException {
        setObject((Object)parameterName, x);
    }

    public void setFloat(String parameterName, float x) throws SQLException {
        setObject((Object)parameterName, x);
    }

    public void setInt(String parameterName, int x) throws SQLException {
        setObject((Object)parameterName, x);
    }

    public void setLong(String parameterName, long x) throws SQLException {
        setObject((Object)parameterName, x);
    }

    public void setNCharacterStream(String parameterName, Reader value)
            throws SQLException {
        setClob((Object)parameterName, value);
    }

    public void setNCharacterStream(String parameterName, Reader value,
            long length) throws SQLException {
        setClob((Object)parameterName, value);
    }

    public void setNClob(String parameterName, NClob value) throws SQLException {
        setObject((Object)parameterName, value);
    }

    public void setNClob(String parameterName, Reader reader)
            throws SQLException {
        setClob((Object)parameterName, reader);
    }

    public void setNClob(String parameterName, Reader reader, long length)
            throws SQLException {
        setClob((Object)parameterName, reader);
    }

    public void setNString(String parameterName, String value)
            throws SQLException {
        setObject((Object)parameterName, null);
    }

    public void setNull(String parameterName, int sqlType) throws SQLException {
        setObject((Object)parameterName, null);
    }

    public void setNull(String parameterName, int sqlType, String typeName)
            throws SQLException {
        setObject((Object)parameterName, null);
    }

    public void setObject(String parameterName, Object x) throws SQLException {
        setObject((Object)parameterName, x);
    }

    public void setObject(String parameterName, Object x, int targetSqlType)
            throws SQLException {
        setObject((Object)parameterName, x, targetSqlType);
    }

    public void setObject(String parameterName, Object x, int targetSqlType,
            int scale) throws SQLException {
        setObject((Object)parameterName, x, targetSqlType, scale);
    }

    public void setRowId(String parameterName, RowId x) throws SQLException {
        throw SqlUtil.createFeatureNotSupportedException();
    }

    public void setSQLXML(String parameterName, SQLXML xmlObject)
            throws SQLException {
        setObject((Object)parameterName, xmlObject);
    }

    public void setShort(String parameterName, short x) throws SQLException {
        setObject((Object)parameterName, x);
    }

    public void setString(String parameterName, String x) throws SQLException {
        setObject((Object)parameterName, x);
    }

    public void setTime(String parameterName, Time x) throws SQLException {
        setObject((Object)parameterName, x);
    }

    public void setTime(String parameterName, Time x, Calendar cal)
            throws SQLException {
        setTime((Object)parameterName, x, cal);
    }

    public void setTimestamp(String parameterName, Timestamp x)
            throws SQLException {
        setObject((Object)parameterName, x);
    }

    public void setTimestamp(String parameterName, Timestamp x, Calendar cal)
            throws SQLException {
        setTimestamp((Object)parameterName, x, cal);
    }

    public void setURL(String parameterName, URL val) throws SQLException {
        setObject((Object)parameterName, val);
    }

    public <T> T getObject(int columnIndex, Class<T> type) throws SQLException {
        throw SqlUtil.createFeatureNotSupportedException();
    }

    public <T> T getObject(String columnLabel, Class<T> type)
            throws SQLException {
        throw SqlUtil.createFeatureNotSupportedException();
    }

}