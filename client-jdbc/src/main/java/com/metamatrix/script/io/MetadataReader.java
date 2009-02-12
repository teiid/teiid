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

package com.metamatrix.script.io;

import java.io.IOException;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;


public class MetadataReader extends StringLineReader {

	ResultSetMetaData source;
    String delimiter = "    "; //$NON-NLS-1$

    boolean firsttime = true;
	int currentColumn = 0;
    
	public MetadataReader(ResultSetMetaData metadata, String delimiter) {
		this.source = metadata;
		this.delimiter = delimiter;
	}
	
	@Override
	protected String nextLine() throws IOException {
		if (firsttime) {
			this.firsttime = false;
			return firstLine();
		}
		
		try {
			int count = this.source.getColumnCount();
			if (this.currentColumn < count) {
				this.currentColumn++;
				return getNextRow();
			}
		} catch (SQLException e) {
			 throw new IOException(e.getMessage());
		}
		return null;
	}
	
    String firstLine() {
        StringBuffer sb = new StringBuffer();
        sb.append("ColumnName").append(delimiter);
        sb.append("ColumnType").append(delimiter);
        sb.append("ColumnTypeName").append(delimiter);
        sb.append("ColumnClassName").append(delimiter);
        sb.append("isNullable").append(delimiter);
        sb.append("TableName").append(delimiter);
        sb.append("SchemaName").append(delimiter);
        sb.append("CatalogName").append(delimiter);
        sb.append("\n"); //$NON-NLS-1$
        return sb.toString();        
    }	
	
	String getNextRow() throws SQLException {
		StringBuffer sb = new StringBuffer();
		
		sb.append(source.getColumnName(currentColumn)).append(delimiter);
		sb.append(source.getColumnType(currentColumn)).append(delimiter);
		sb.append(source.getColumnTypeName(currentColumn)).append(delimiter);
		sb.append(source.getColumnClassName(currentColumn)).append(delimiter);
		sb.append(source.isNullable(currentColumn)).append(delimiter);
		sb.append(source.getTableName(currentColumn)).append(delimiter);
		sb.append(source.getSchemaName(currentColumn)).append(delimiter);
		sb.append(source.getCatalogName(currentColumn)).append(delimiter);
		sb.append("\n"); //$NON-NLS-1$
		
		return sb.toString();
	}	
}
