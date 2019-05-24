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

import java.io.IOException;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;

import org.teiid.script.io.StringLineReader;


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
        sb.append("ColumnName").append(delimiter); //$NON-NLS-1$
        sb.append("ColumnType").append(delimiter); //$NON-NLS-1$
        sb.append("ColumnTypeName").append(delimiter); //$NON-NLS-1$
        sb.append("ColumnClassName").append(delimiter); //$NON-NLS-1$
        sb.append("isNullable").append(delimiter); //$NON-NLS-1$
        sb.append("TableName").append(delimiter); //$NON-NLS-1$
        sb.append("SchemaName").append(delimiter); //$NON-NLS-1$
        sb.append("CatalogName").append(delimiter); //$NON-NLS-1$
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
