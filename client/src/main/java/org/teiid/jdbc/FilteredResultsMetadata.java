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


/**
 */
public class FilteredResultsMetadata extends WrapperImpl implements ResultSetMetaData {

    private ResultSetMetaData delegate; 
    private int actualColumnCount;
    
    FilteredResultsMetadata(ResultSetMetaData rsmd, int actualColumnCount) {
        this.delegate = rsmd;
        this.actualColumnCount = actualColumnCount;
    }
    
    public int getColumnCount() throws SQLException {
        return actualColumnCount;
    }
    
    private void verifyColumnIndex(int index) throws SQLException {
        if(index > actualColumnCount) {
            throw new SQLException(JDBCPlugin.Util.getString("StaticMetadataProvider.Invalid_column", index)); //$NON-NLS-1$
        }
    }

    public boolean isAutoIncrement(int column) throws SQLException {
        verifyColumnIndex(column);
        return this.delegate.isAutoIncrement(column);
    }

    public boolean isCaseSensitive(int column) throws SQLException {
        verifyColumnIndex(column);
        return this.delegate.isCaseSensitive(column);
    }

    public boolean isSearchable(int column) throws SQLException {
        verifyColumnIndex(column);
        return this.delegate.isSearchable(column);
    }

    public boolean isCurrency(int column) throws SQLException {
        verifyColumnIndex(column);
        return this.delegate.isCurrency(column);
    }

    public int isNullable(int column) throws SQLException {
        verifyColumnIndex(column);
        return this.delegate.isNullable(column);
    }

    public boolean isSigned(int column) throws SQLException {
        verifyColumnIndex(column);
        return this.delegate.isSigned(column);
    }

    public int getColumnDisplaySize(int column) throws SQLException {
        verifyColumnIndex(column);
        return this.delegate.getColumnDisplaySize(column);
    }

    public String getColumnLabel(int column) throws SQLException {
        verifyColumnIndex(column);
        return this.delegate.getColumnLabel(column);
    }

    public String getColumnName(int column) throws SQLException {
        verifyColumnIndex(column);
        return this.delegate.getColumnName(column);
    }

    public String getSchemaName(int column) throws SQLException {
        verifyColumnIndex(column);
        return this.delegate.getSchemaName(column);
    }

    public int getPrecision(int column) throws SQLException {
        verifyColumnIndex(column);
        return this.delegate.getPrecision(column);
    }

    public int getScale(int column) throws SQLException {
        verifyColumnIndex(column);
        return this.delegate.getScale(column);
    }

    public String getTableName(int column) throws SQLException {
        verifyColumnIndex(column);
        return this.delegate.getTableName(column);
    }

    public String getCatalogName(int column) throws SQLException {
        verifyColumnIndex(column);
        return this.delegate.getCatalogName(column);
    }

    public int getColumnType(int column) throws SQLException {
        verifyColumnIndex(column);
        return this.delegate.getColumnType(column);
    }

    public String getColumnTypeName(int column) throws SQLException {
        verifyColumnIndex(column);
        return this.delegate.getColumnTypeName(column);
    }

    public boolean isReadOnly(int column) throws SQLException {
        verifyColumnIndex(column);
        return this.delegate.isReadOnly(column);
    }

    public boolean isWritable(int column) throws SQLException {
        verifyColumnIndex(column);
        return this.delegate.isWritable(column);
    }

    public boolean isDefinitelyWritable(int column) throws SQLException {
        verifyColumnIndex(column);
        return this.delegate.isDefinitelyWritable(column);
    }

    public String getColumnClassName(int column) throws SQLException {
        verifyColumnIndex(column);
        return this.delegate.getColumnClassName(column);
    }

}
