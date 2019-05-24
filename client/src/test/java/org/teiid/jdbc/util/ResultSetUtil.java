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

package org.teiid.jdbc.util;

import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.io.Writer;
import java.lang.reflect.Method;
import java.sql.Blob;
import java.sql.Clob;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.SQLXML;


/**
 * @since 4.2
 */
@SuppressWarnings("nls")
public class ResultSetUtil {

    public static final int DEFAULT_MAX_COL_WIDTH = 29;
    private static final String SPACER = "  "; //$NON-NLS-1$
    private static final String NULL = "<null>"; //$NON-NLS-1$
    private static final String MORE = "$ ";

    public static void printResultSet(ResultSet rs) throws SQLException, IOException {
        PrintWriter pw = new PrintWriter(System.out);
        printResultSet(rs, 100, true, pw);
        pw.flush();
    }

    /**
     * Prints the ResultSet (and optionally the ResultSetMetaData) to a stream. If you're using the stream from getPrintStream(),
     * then you can also compare data with expected results.
     * @param rs
     * @param maxColWidth the max width a column is allowed to have. The column will be wider than this value only if the column name is longer.
     * @param printMetadata
     * @param out
     * @throws SQLException
     * @throws IOException
     * @since 4.2
     */
    public static void printResultSet(ResultSet rs, int maxColWidth, boolean printMetadata, Writer out) throws SQLException, IOException {
        if (maxColWidth < 0) {
            maxColWidth = DEFAULT_MAX_COL_WIDTH;
        }
        ResultSetMetaData rsmd = rs.getMetaData();
        int count = rsmd.getColumnCount();
        int[] sizes = new int[count];
        StringWriter types = new StringWriter();
        StringWriter columns = new StringWriter();
        for (int i = 1; i <= count; i++) {
            String columnName = rsmd.getColumnLabel(i);
            String typeName = rsmd.getColumnTypeName(i);
            if (maxColWidth == 0) {
                // Sets the width of the column to the wider of the column name and the column type name.
                sizes[i-1] = Math.max(columnName.length(), typeName.length());
            } else {
                // Sets the width of the column to the wider of the column name and the column display size (which cannot exceed maxColWidth).
                sizes[i-1] = Math.max(Math.max(columnName.length(), typeName.length()), // takes into account the type name width
                                      Math.min(rsmd.getColumnDisplaySize(i), maxColWidth));
            }
            types.write(resizeString(typeName, sizes[i-1]));
            columns.write(resizeString(columnName, sizes[i-1]));
            if (i != count) {
                types.write(SPACER);
                columns.write(SPACER);
            }
        }
        out.append(types.toString()).append("\n");
        out.append(columns.toString()).append("\n");
        int totalRows = 0;
        while (rs.next()) {
            for (int j = 1; j <= count; j++) {
                Object obj = rs.getObject(j);
                if (obj instanceof SQLXML) {
                    obj = ((SQLXML)obj).getString();
                } else if (obj instanceof Clob) {
                    obj = "Clob[" + ((Clob)obj).length() + "]"; //$NON-NLS-1$ //$NON-NLS-2$
                } else if (obj instanceof Blob) {
                    obj = "Blob[" + ((Blob)obj).length() + "]"; //$NON-NLS-1$ //$NON-NLS-2$
                }
                if (maxColWidth == 0) {
                    out.append(obj == null ? NULL : obj.toString());
                    if (j != count) out.append(SPACER);
                } else {
                    String resizedString = resizeString(obj, sizes[j-1]);
                    out.append(resizedString);
                    if (j != count && resizedString.length() <= sizes[j-1]) {
                        out.append(SPACER);
                    }
                }
            }
            out.append("\n");
            totalRows++;
        }
        out.append("Row Count : " + totalRows).append("\n"); //$NON-NLS-1$
        if (printMetadata) printResultSetMetadata(rsmd, out);
    }

    private static String[] METADATA_METHODS = {
        "getColumnName", //$NON-NLS-1$
        "getColumnType", //$NON-NLS-1$
        "getCatalogName", //$NON-NLS-1$
        "getColumnClassName", //$NON-NLS-1$
        "getColumnLabel", //$NON-NLS-1$
        "getColumnTypeName", //$NON-NLS-1$
        "getSchemaName", //$NON-NLS-1$
        "getTableName", //$NON-NLS-1$
        "getColumnDisplaySize", //$NON-NLS-1$
        "getPrecision", //$NON-NLS-1$
        "getScale", //$NON-NLS-1$
        "isAutoIncrement", //$NON-NLS-1$
        "isCaseSensitive", //$NON-NLS-1$
        "isCurrency", //$NON-NLS-1$
        "isDefinitelyWritable", //$NON-NLS-1$
        "isNullable", //$NON-NLS-1$
        "isReadOnly", //$NON-NLS-1$
        "isSearchable", //$NON-NLS-1$
        "isSigned", //$NON-NLS-1$
        "isWritable", //$NON-NLS-1$
    };

    /**
     * Prints the ResultSetMetaData values for each column
     * @param rsmd
     * @param out
     * @throws SQLException
     * @throws IOException
     * @since 4.2
     */
    public static void printResultSetMetadata(ResultSetMetaData rsmd, Writer out) throws SQLException, IOException {
        int columns = rsmd.getColumnCount();
        Class RSMD = ResultSetMetaData.class;
        Class[] params = {int.class};
        int numMethods = METADATA_METHODS.length;
        String[][] metadataStrings = new String[columns][numMethods];
        // Init the widths of the columns
        int[] maxColWidths = new int[numMethods];
        for (int i = 0; i < numMethods; i++) {
            maxColWidths[i] = METADATA_METHODS[i].length();
        }
        // Buffer the metadata
        for (int col = 1; col <= columns; col++) {
            Object [] columnParam = {new Integer(col)};
            for (int i = 0; i < numMethods; i++) {
                try {
                    Method m = RSMD.getMethod(METADATA_METHODS[i], params);
                    Object obj = m.invoke(rsmd, columnParam);
                    String stringVal = (obj == null) ? NULL : obj.toString();
                    metadataStrings[col - 1][i] = stringVal;
                    if (maxColWidths[i] < stringVal.length()) {
                        maxColWidths[i] = stringVal.length();
                    }
                } catch (Throwable t) {

                }
            }
        }
        // Print the header
        for (int i = 0; i < numMethods; i++) {
            out.append(resizeString(METADATA_METHODS[i], maxColWidths[i]));
            if (i != numMethods) {
                out.append(SPACER);
            }
        }
        out.append("\n");
        // Print the metadata from the buffer
        for (int col = 0; col < columns; col++) {
            for (int i = 0; i < numMethods; i++) {
                out.append(resizeString(metadataStrings[col][i], maxColWidths[i]));
                if (i != numMethods) {
                    out.append(SPACER);
                }
            }
            out.append("\n");
        }
    }

    private static String resizeString(Object obj, int size) {
        if (obj == null) {
            return resizeString(NULL, size);
        }
        String str = obj.toString();
        if (str.length() == size) {
            return str;
        } else if (str.length() < size) {
            return pad(str, size - str.length());
        } else {
            return str.substring(0, size) + MORE;
        }
    }

    private static String pad(String str, int padding) {
        StringBuffer buf = new StringBuffer(str);
        for (int i = 0; i < padding; i++) {
            buf.append(' ');
        }
        return buf.toString();
    }
}
