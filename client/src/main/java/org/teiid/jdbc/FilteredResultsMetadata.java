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
