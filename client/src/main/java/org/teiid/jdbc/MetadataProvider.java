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

import java.sql.SQLException;
import java.util.Map;


/**
 */
public class MetadataProvider {

    // Map of detail maps -- <columnIndex, Map<propertyName, metadataObject>>
	protected Map[] metadata;

    public MetadataProvider(Map[] metadata) {
    	if (metadata == null) {
    		this.metadata = new Map[0];
    	} else {
    		this.metadata = metadata;
    	}
    }
    
    public Object getValue(int columnIndex, Integer metadataPropertyKey) throws SQLException {
        if(columnIndex < 0 || columnIndex >= metadata.length) {
            throw new SQLException(JDBCPlugin.Util.getString("StaticMetadataProvider.Invalid_column", columnIndex)); //$NON-NLS-1$
        }
        
        Map column = this.metadata[columnIndex];
        return column.get(metadataPropertyKey);
    }

    public int getColumnCount() {
        return metadata.length;
    }
    
    public String getStringValue(int columnIndex, Integer metadataPropertyKey) throws SQLException {
        return (String) getValue(columnIndex, metadataPropertyKey);
    }

    public int getIntValue(int columnIndex, Integer metadataPropertyKey) throws SQLException {
    	return getIntValue(columnIndex, metadataPropertyKey, 0);
    }
    
    public int getIntValue(int columnIndex, Integer metadataPropertyKey, int defaultValue) throws SQLException {
    	Integer val = (Integer) getValue(columnIndex, metadataPropertyKey);
    	if (val == null) {
    		return defaultValue;
    	}
    	return val; 
    }

    public boolean getBooleanValue(int columnIndex, Integer metadataPropertyKey) throws SQLException {
        return ((Boolean) getValue(columnIndex, metadataPropertyKey)).booleanValue();
    }

}
