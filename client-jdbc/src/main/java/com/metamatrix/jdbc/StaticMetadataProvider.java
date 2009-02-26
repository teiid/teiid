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
import java.util.Map;


/**
 */
public class StaticMetadataProvider extends AbstractMetadataProvider {

    // Map of detail maps -- <columnIndex, Map<propertyName, metadataObject>>
    private Map[] columnMetadata;
    private int paramCount;

    StaticMetadataProvider() {
    }
    
    public static StaticMetadataProvider createWithData(Map[] columnMetadata, int paramCount) {
        StaticMetadataProvider provider = null;
        
        provider = new StaticMetadataProvider();    
        provider.setData(columnMetadata);
        provider.setParameterCount(paramCount);        
        return provider;
    }
    
    /**
     * Set column metadata.  The Map[] holds metadata for each column, 
     * indexed by column 
     * @param columnMetadata Each Map is from metadata key to metadata value
     */
    private void setData(Map[] columnMetadata) { 
        this.columnMetadata = columnMetadata;
    }

    private void checkMetadataExists() throws SQLException {
        if(columnMetadata == null) {
            throw new SQLException(JDBCPlugin.Util.getString("StaticMetadataProvider.No_metadata"));  //$NON-NLS-1$
        }         
    }
        
    public Object getValue(int columnIndex, Integer metadataPropertyKey) throws SQLException {
        checkMetadataExists();
        
        if(columnIndex < 0 || columnIndex >= columnMetadata.length) {
            throw new SQLException(JDBCPlugin.Util.getString("StaticMetadataProvider.Invalid_column", columnIndex)); //$NON-NLS-1$
        }
        
        Map column = this.columnMetadata[columnIndex];
        return column.get(metadataPropertyKey);
    }

    public int getColumnCount() throws SQLException {
        checkMetadataExists();
        return columnMetadata.length;
    }

    public int getParameterCount() {
        return paramCount;
    }

    public void setParameterCount(int paramCount) {
        this.paramCount = paramCount;
    }

}
