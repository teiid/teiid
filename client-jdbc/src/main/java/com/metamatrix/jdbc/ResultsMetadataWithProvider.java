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

import java.sql.SQLException;

import com.metamatrix.common.types.MMJDBCSQLTypeInfo;
import com.metamatrix.dqp.metadata.ResultsMetadataConstants;

/**
 */
public class ResultsMetadataWithProvider extends WrapperImpl implements com.metamatrix.jdbc.api.ResultSetMetaData {

    private ResultsMetadataProvider provider;
    
    /**
     * Factory Constructor 
     * @param statement
     * @param valueID
     */
    public static ResultsMetadataWithProvider newInstance(ResultsMetadataProvider provider) {
        return new ResultsMetadataWithProvider(provider);        
    }
    
    public ResultsMetadataWithProvider(ResultsMetadataProvider provider) {
        setMetadataProvider(provider);
    }
    
    void setMetadataProvider(ResultsMetadataProvider provider) {
        this.provider = provider;
    }
    
    private void verifyProvider() throws SQLException {
        if(this.provider == null) {
            throw new SQLException(JDBCPlugin.Util.getString("ResultsMetadataWithProvider.No_provider")); //$NON-NLS-1$
        }
    }
    
    /**
     * Adjust from 1-based to internal 0-based representation
     * @param index External 1-based representation
     * @return Internal 0-based representation
     */
    private int adjustColumn(int index) {
        return index-1;
    }
    
    public String getVirtualDatabaseName(int index) throws SQLException {
        verifyProvider();
        return provider.getStringValue(adjustColumn(index), ResultsMetadataConstants.VIRTUAL_DATABASE_NAME);
    }

    public String getVirtualDatabaseVersion(int index) throws SQLException {
        verifyProvider();
        return provider.getStringValue(adjustColumn(index), ResultsMetadataConstants.VIRTUAL_DATABASE_VERSION);
    }

    public int getColumnCount() throws SQLException {
        verifyProvider();
        return provider.getColumnCount();
    }

    public boolean isAutoIncrement(int index) throws SQLException {
        verifyProvider();
        return provider.getBooleanValue(adjustColumn(index), ResultsMetadataConstants.AUTO_INCREMENTING);
    }

    public boolean isCaseSensitive(int index) throws SQLException {
        verifyProvider();
        return provider.getBooleanValue(adjustColumn(index), ResultsMetadataConstants.CASE_SENSITIVE);
    }

    public boolean isSearchable(int index) throws SQLException {
        verifyProvider();
        Integer searchable = (Integer) provider.getValue(adjustColumn(index), ResultsMetadataConstants.SEARCHABLE);
        return !(ResultsMetadataConstants.SEARCH_TYPES.UNSEARCHABLE.equals(searchable));
    }

    public boolean isCurrency(int index) throws SQLException {
        verifyProvider();
        return provider.getBooleanValue(adjustColumn(index), ResultsMetadataConstants.CURRENCY);
    }

    public int isNullable(int index) throws SQLException {
        verifyProvider();
        Object nullable = provider.getValue(adjustColumn(index), ResultsMetadataConstants.NULLABLE);
        if(nullable.equals(ResultsMetadataConstants.NULL_TYPES.NULLABLE)) {
            return columnNullable;    
        } else if(nullable.equals(ResultsMetadataConstants.NULL_TYPES.NOT_NULL)) {
            return columnNoNulls;
        } else {
            return columnNullableUnknown;
        }
    }
                        
    public boolean isSigned(int index) throws SQLException {
        verifyProvider();
        return provider.getBooleanValue(adjustColumn(index), ResultsMetadataConstants.SIGNED);
    }

    public int getColumnDisplaySize(int index) throws SQLException {
        verifyProvider();
        return provider.getIntValue(adjustColumn(index), ResultsMetadataConstants.DISPLAY_SIZE);
    }

    public String getColumnLabel(int index) throws SQLException {
        verifyProvider();
        return provider.getStringValue(adjustColumn(index), ResultsMetadataConstants.ELEMENT_LABEL);
    }

    public String getColumnName(int index) throws SQLException {
        verifyProvider();
        return provider.getStringValue(adjustColumn(index), ResultsMetadataConstants.ELEMENT_NAME);
    }

    public String getSchemaName(int index) throws SQLException {
        verifyProvider();
        return provider.getStringValue(adjustColumn(index), ResultsMetadataConstants.VIRTUAL_DATABASE_NAME);
    }

    public int getPrecision(int index) throws SQLException {
        verifyProvider();
        return provider.getIntValue(adjustColumn(index), ResultsMetadataConstants.PRECISION);
    }

    public int getScale(int index) throws SQLException {
        verifyProvider();
        return provider.getIntValue(adjustColumn(index), ResultsMetadataConstants.SCALE);
    }

    public String getTableName(int index) throws SQLException {
        verifyProvider();
        return provider.getStringValue(adjustColumn(index), ResultsMetadataConstants.GROUP_NAME);
    }

    public String getCatalogName(int index) throws SQLException {
        return null;
    }

    public int getColumnType(int index) throws SQLException {
        verifyProvider();
        
        String runtimeTypeName = provider.getStringValue(adjustColumn(index), ResultsMetadataConstants.DATA_TYPE);
        return MMJDBCSQLTypeInfo.getSQLType(runtimeTypeName);
    }

    public String getColumnTypeName(int index) throws SQLException {
        verifyProvider();        
        return provider.getStringValue(adjustColumn(index), ResultsMetadataConstants.DATA_TYPE);
    }

    public boolean isReadOnly(int index) throws SQLException {
        verifyProvider();
        return ! provider.getBooleanValue(adjustColumn(index), ResultsMetadataConstants.WRITABLE);
    }

    public boolean isWritable(int index) throws SQLException {
        verifyProvider();
        return provider.getBooleanValue(adjustColumn(index), ResultsMetadataConstants.WRITABLE);
    }

    public boolean isDefinitelyWritable(int index) throws SQLException {
        verifyProvider();
        return provider.getBooleanValue(adjustColumn(index), ResultsMetadataConstants.WRITABLE);
    }

    public String getColumnClassName(int index) throws SQLException {
        verifyProvider();
        
        return MMJDBCSQLTypeInfo.getJavaClassName(getColumnType(index));
    }

    public int getParameterCount() throws SQLException{
        return provider.getParameterCount();
    }

}
