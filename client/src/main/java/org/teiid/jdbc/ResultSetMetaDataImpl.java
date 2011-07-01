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

package org.teiid.jdbc;

import java.sql.ResultSetMetaData;
import java.sql.SQLException;

import org.teiid.client.metadata.ResultsMetadataConstants;
import org.teiid.core.types.JDBCSQLTypeInfo;


/**
 */
public class ResultSetMetaDataImpl extends WrapperImpl implements ResultSetMetaData {

    private MetadataProvider provider;

    private boolean useJDBC4ColumnNameAndLabelSemantics = true;
    
    public ResultSetMetaDataImpl(MetadataProvider provider, String supportBackwardsCompatibility) {
    	this.provider = provider;
    	this.useJDBC4ColumnNameAndLabelSemantics = (supportBackwardsCompatibility != null && supportBackwardsCompatibility.equalsIgnoreCase("false") ? false : true);
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
        return provider.getStringValue(adjustColumn(index), ResultsMetadataConstants.VIRTUAL_DATABASE_NAME);
    }

    public String getVirtualDatabaseVersion(int index) throws SQLException {
        return provider.getStringValue(adjustColumn(index), ResultsMetadataConstants.VIRTUAL_DATABASE_VERSION);
    }

    public int getColumnCount() throws SQLException {
        return provider.getColumnCount();
    }

    public boolean isAutoIncrement(int index) throws SQLException {
        return provider.getBooleanValue(adjustColumn(index), ResultsMetadataConstants.AUTO_INCREMENTING);
    }

    public boolean isCaseSensitive(int index) throws SQLException {
        return provider.getBooleanValue(adjustColumn(index), ResultsMetadataConstants.CASE_SENSITIVE);
    }

    public boolean isSearchable(int index) throws SQLException {
        Integer searchable = (Integer) provider.getValue(adjustColumn(index), ResultsMetadataConstants.SEARCHABLE);
        return !(ResultsMetadataConstants.SEARCH_TYPES.UNSEARCHABLE.equals(searchable));
    }

    public boolean isCurrency(int index) throws SQLException {
        return provider.getBooleanValue(adjustColumn(index), ResultsMetadataConstants.CURRENCY);
    }

    public int isNullable(int index) throws SQLException {
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
        return provider.getBooleanValue(adjustColumn(index), ResultsMetadataConstants.SIGNED);
    }

    public int getColumnDisplaySize(int index) throws SQLException {
        return provider.getIntValue(adjustColumn(index), ResultsMetadataConstants.DISPLAY_SIZE);
    }

    public String getColumnLabel(int index) throws SQLException {
        String result = provider.getStringValue(adjustColumn(index), ResultsMetadataConstants.ELEMENT_LABEL);
        if (result != null) {
        	return result;
        }
        return getColumnName(index);
    }

    public String getColumnName(int index) throws SQLException {

    	if (!useJDBC4ColumnNameAndLabelSemantics) {
            String result = provider.getStringValue(adjustColumn(index), ResultsMetadataConstants.ELEMENT_LABEL);
            if (result != null) {
            	return result;
            }
    	}
        return provider.getStringValue(adjustColumn(index), ResultsMetadataConstants.ELEMENT_NAME);
    }

    public String getSchemaName(int index) throws SQLException {
        String name = provider.getStringValue(adjustColumn(index), ResultsMetadataConstants.GROUP_NAME);
        if (name != null) {
        	int dotIndex = name.indexOf('.');
        	if (dotIndex != -1) {
        		return name.substring(0, dotIndex);
        	}
        }
        return null;
    }

    public int getPrecision(int index) throws SQLException {
        return provider.getIntValue(adjustColumn(index), ResultsMetadataConstants.PRECISION);
    }

    public int getScale(int index) throws SQLException {
        return provider.getIntValue(adjustColumn(index), ResultsMetadataConstants.SCALE);
    }

    public String getTableName(int index) throws SQLException {
        String name = provider.getStringValue(adjustColumn(index), ResultsMetadataConstants.GROUP_NAME);
        if (name != null) {
        	int dotIndex = name.indexOf('.');
        	if (dotIndex != -1) {
        		return name.substring(dotIndex + 1);
        	}
        }
        return name;
    }

    public String getCatalogName(int index) throws SQLException {
    	return provider.getStringValue(adjustColumn(index), ResultsMetadataConstants.VIRTUAL_DATABASE_NAME);
    }

    public int getColumnType(int index) throws SQLException {
        String runtimeTypeName = getColumnTypeName(index);
        return JDBCSQLTypeInfo.getSQLType(runtimeTypeName);
    }

    public String getColumnTypeName(int index) throws SQLException {
        return provider.getStringValue(adjustColumn(index), ResultsMetadataConstants.DATA_TYPE);
    }

    public boolean isReadOnly(int index) throws SQLException {
        return ! provider.getBooleanValue(adjustColumn(index), ResultsMetadataConstants.WRITABLE);
    }

    public boolean isWritable(int index) throws SQLException {
        return provider.getBooleanValue(adjustColumn(index), ResultsMetadataConstants.WRITABLE);
    }

    public boolean isDefinitelyWritable(int index) throws SQLException {
        return provider.getBooleanValue(adjustColumn(index), ResultsMetadataConstants.WRITABLE);
    }

    public String getColumnClassName(int index) throws SQLException {
        return JDBCSQLTypeInfo.getJavaClassName(getColumnType(index));
    }

}
