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

package org.teiid.jdbc.util;

import java.io.PrintStream;
import java.io.StringWriter;
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

    /**
     * Prints the ResultSet (and optionally the ResultSetMetaData) to a stream. If you're using the stream from getPrintStream(),
     * then you can also compare data with expected results.
     * @param rs
     * @param maxColWidth the max width a column is allowed to have. The column will be wider than this value only if the column name is longer.
     * @param printMetadata
     * @param out
     * @throws SQLException
     * @since 4.2
     */
    public static void printResultSet(ResultSet rs, int maxColWidth, boolean printMetadata, PrintStream out) throws SQLException {
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
        out.println(types.toString());
        out.println(columns.toString());
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
                    out.print(obj == null ? NULL : obj); 
                    if (j != count) out.print(SPACER);
                } else {
                    String resizedString = resizeString(obj, sizes[j-1]);
                    out.print(resizedString);
                    if (j != count && resizedString.length() <= sizes[j-1]) {
                        out.print(SPACER);
                    }
                }
            }
            out.println();
            totalRows++;
        }
        out.println("Row Count : " + totalRows); //$NON-NLS-1$
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
     * @since 4.2
     */
    public static void printResultSetMetadata(ResultSetMetaData rsmd, PrintStream out) throws SQLException {
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
            out.print(resizeString(METADATA_METHODS[i], maxColWidths[i]));
            if (i != numMethods) {
                out.print(SPACER);
            }
        }
        out.println();
        // Print the metadata from the buffer
        for (int col = 0; col < columns; col++) {
            for (int i = 0; i < numMethods; i++) {
                out.print(resizeString(metadataStrings[col][i], maxColWidths[i]));
                if (i != numMethods) {
                    out.print(SPACER);
                }
            }
            out.println();
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
