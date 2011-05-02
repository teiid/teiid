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

import java.io.BufferedReader;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.OutputStream;
import java.io.PrintStream;
import java.io.StringWriter;
import java.lang.reflect.Method;
import java.sql.Blob;
import java.sql.Clob;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.SQLXML;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;


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
     * Gets a PrintStream implementation that uses the input parameters as underlying streams 
     * @param resultsOutput an output file for result data. If null, results will only be written to the defaul stream.
     * @param expectedResultsInput the reader for expected data. If null, actual data is never compared against expected results.
     * @param defaultPrintStream if not null, this utility will always write to this stream. Typically this is System.out
     * @return the single PrintStream that wraps all the input streams for writing and comparison.
     * @since 4.2
     */
    public static PrintStream getPrintStream(OutputStream resultsOutput, BufferedReader expectedResultsInput, PrintStream defaultPrintStream) {
        PrintStream out = null;
        if (defaultPrintStream == null) {
            defaultPrintStream = new PrintStream(new OutputStream () {
                                                     public void write(int b) throws IOException {}
                                                 });
        }
        if (resultsOutput == null && expectedResultsInput == null) {
            out = defaultPrintStream;
        } else if (resultsOutput == null && expectedResultsInput != null) {
            out = new ComparingPrintStream(defaultPrintStream, expectedResultsInput);
        } else if (resultsOutput!= null && expectedResultsInput == null) {
            PrintStream filePrintStream = new PrintStream(resultsOutput);
            out = new MuxingPrintStream(new PrintStream[] {defaultPrintStream, filePrintStream});
        } else {
            PrintStream filePrintStream = new PrintStream(resultsOutput);
            out = new ComparingPrintStream(new MuxingPrintStream(new PrintStream[] {defaultPrintStream, filePrintStream}), expectedResultsInput);
        }
        return out;
    }
    
    /**
     *  Compares the actual results with the expected results.
     * @param updateCount the result of the execution
     * @param resultsFile output file to which the results will be written. Can be null.
     * @param expectedResultsFile expected results file with which to compare the results. Can be null.
     * @param printToConsole writes to System.out if true
     * @return The List of line numbers which differ between the actual and expected results.
     * @throws IOException
     * @since 4.3
     */
    public static List writeAndCompareUpdateCount(int updateCount, File resultsFile, File expectedResultsFile, boolean printToConsole) throws IOException  {
        FileOutputStream resultsOutputStream = null;
        if (resultsFile != null) {
            resultsOutputStream = new FileOutputStream(resultsFile);
        }
        BufferedReader expectedResultsReader = null;
        if (expectedResultsFile != null && expectedResultsFile.exists() && expectedResultsFile.canRead()) {
            expectedResultsReader = new BufferedReader(new FileReader(expectedResultsFile));
        }
        return writeAndCompareUpdateCount(updateCount, resultsOutputStream, expectedResultsReader, printToConsole ? System.out : null);
    }

    /**
     *  Compares the actual results with the expected results.
     * @param updateCount the result of the execution
     * @param resultsOutput OutputStream to which the results will be written. Can be null.
     * @param expectedResultsInput reader with which the expected results are read. Can be null.
     * @param defaultPrintStream the default stream to which to write the results. Can be null.
     * @return The List of line numbers which differ between the actual and expected results.
     * @since 4.3
     */
    public static List writeAndCompareUpdateCount(int updateCount, OutputStream resultsOutput, BufferedReader expectedResultsInput, PrintStream defaultPrintStream) {
        PrintStream out = getPrintStream(resultsOutput, expectedResultsInput, defaultPrintStream);
        printUpdateCount(updateCount, out);
        return getUnequalLines(out);
    }
    
    /**
     *  Compares the actual results with the expected results.
     * @param counts the result of the execution
     * @param resultsFile output file to which the results will be written. Can be null.
     * @param expectedResultsFile expected results file with which to compare the results. Can be null.
     * @param printToConsole writes to System.out if true
     * @return The List of line numbers which differ between the actual and expected results.
     * @throws IOException
     * @since 4.3
     */
    public static List writeAndCompareBatchedUpdateCounts(int[] counts, File resultsFile, File expectedResultsFile, boolean printToConsole) throws IOException  {
        FileOutputStream resultsOutputStream = null;
        if (resultsFile != null) {
            resultsOutputStream = new FileOutputStream(resultsFile);
        }
        BufferedReader expectedResultsReader = null;
        if (expectedResultsFile != null && expectedResultsFile.exists() && expectedResultsFile.canRead()) {
            expectedResultsReader = new BufferedReader(new FileReader(expectedResultsFile));
        }
        return writeAndCompareBatchedUpdateCounts(counts, resultsOutputStream, expectedResultsReader, printToConsole ? System.out : null);
    }

    /**
     *  Compares the actual results with the expected results.
     * @param counts the result of the execution
     * @param resultsOutput OutputStream to which the results will be written. Can be null.
     * @param expectedResultsInput reader with which the expected results are read. Can be null.
     * @param defaultPrintStream the default stream to which to write the results. Can be null.
     * @return The List of line numbers which differ between the actual and expected results.
     * @since 4.3
     */
    public static List writeAndCompareBatchedUpdateCounts(int[] counts, OutputStream resultsOutput, BufferedReader expectedResultsInput, PrintStream defaultPrintStream) {
        PrintStream out = getPrintStream(resultsOutput, expectedResultsInput, defaultPrintStream);
        printBatchedUpdateCounts(counts, out);
        return getUnequalLines(out);
    }
    
    /**
     *  Compares the actual results with the expected results.
     * @param rs the result of the execution
     * @param maxColWidth the max width a column is allowed to have
     * @param printMetadata writes the metadata if true
     * @param resultsFile output file to which the results will be written. Can be null.
     * @param expectedResultsFile expected results file with which to compare the results. Can be null.
     * @param printToConsole writes to System.out if true
     * @return The List of line numbers which differ between the actual and expected results.
     * @throws IOException
     * @throws SQLException
     * @since 4.3
     */
    public static List writeAndCompareResultSet(ResultSet rs, int maxColWidth, boolean printMetadata, File resultsFile, File expectedResultsFile, boolean printToConsole) throws IOException, SQLException  {
        FileOutputStream resultsOutputStream = null;
        if (resultsFile != null) {
            resultsOutputStream = new FileOutputStream(resultsFile);
        }
        BufferedReader expectedResultsReader = null;
        if (expectedResultsFile != null && expectedResultsFile.exists() && expectedResultsFile.canRead()) {
            expectedResultsReader = new BufferedReader(new FileReader(expectedResultsFile));
        }
        return writeAndCompareResultSet(rs, maxColWidth, printMetadata, resultsOutputStream, expectedResultsReader, printToConsole ? System.out : null);
    }
    
    /**
     *  Compares the actual results with the expected results.
     * @param rs the result of the execution
     * @param maxColWidth the max width a column is allowed to have
     * @param printMetadata writes the metadata if true
     * @param resultsOutput OutputStream to which the results will be written. Can be null.
     * @param expectedResultsInput reader with which the expected results are read. Can be null.
     * @param defaultPrintStream the default stream to which to write the results. Can be null.
     * @return The List of line numbers which differ between the actual and expected results.
     * @throws SQLException
     * @since 4.3
     */
    public static List writeAndCompareResultSet(ResultSet rs, int maxColWidth, boolean printMetadata, OutputStream resultsOutput, BufferedReader expectedResultsInput, PrintStream defaultPrintStream) throws SQLException {
        PrintStream out = getPrintStream(resultsOutput, expectedResultsInput, defaultPrintStream);
        printResultSet(rs, maxColWidth, printMetadata, out);
        return getUnequalLines(out);
    }
    
    public static List getUnequalLines(PrintStream out) {
        if (out instanceof ComparingPrintStream) {
            return ((ComparingPrintStream)out).getUnequalLines();
        }
        return Collections.EMPTY_LIST;
    }
    
    public static List writeAndCompareThrowable(Throwable t, File resultsFile, File expectedResultsFile, boolean printToConsole) throws IOException, SQLException  {
        FileOutputStream resultsOutputStream = null;
        if (resultsFile != null) {
            resultsOutputStream = new FileOutputStream(resultsFile);
        }
        BufferedReader expectedResultsReader = null;
        if (expectedResultsFile != null && expectedResultsFile.exists() && expectedResultsFile.canRead()) {
            expectedResultsReader = new BufferedReader(new FileReader(expectedResultsFile));
        }
        return writeAndCompareThrowable(t, resultsOutputStream, expectedResultsReader, printToConsole ? System.out : null);
    }
    
    public static List writeAndCompareThrowable(Throwable t, OutputStream resultsOutput, BufferedReader expectedResultsInput, PrintStream defaultPrintStream) throws SQLException {
        PrintStream out = getPrintStream(resultsOutput, expectedResultsInput, defaultPrintStream);
        printThrowable(t, out);
        return getUnequalLines(out);
    }
    
    public static void printThrowable(Throwable t, PrintStream out) {
        out.println(t.getClass().getName() + " : " + t.getMessage()); //$NON-NLS-1$
    }
    
    public static void printUpdateCount(int updateCount, PrintStream out) {
        out.println("Update Count : " + updateCount); //$NON-NLS-1$
    }
    
    public static void printBatchedUpdateCounts(int[] counts, PrintStream out) {
        out.println("Batched Update Counts :"); //$NON-NLS-1$
        for (int i = 0; i < counts.length; i++) {
            out.println(counts[i]);
        }
        out.println("Total Batched Commands : " + counts.length); //$NON-NLS-1$
    }
    
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
            String columnName = rsmd.getColumnName(i);
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
    
    /**
     * Used to write the same data to more than one output stream. 
     * @since 4.2
     */
    private static final class MuxingPrintStream extends PrintStream {
        private PrintStream[] streams;
        private MuxingPrintStream(PrintStream[] streams) {
            super(streams[0]);
            this.streams = new PrintStream[streams.length];
            System.arraycopy(streams, 0, this.streams, 0, streams.length);
        }
        public void close() {
            for (int i = 0; i < streams.length; i++) {
                streams[i].close();
            }
        }
        public void flush() {
            for (int i = 0; i < streams.length; i++) {
                streams[i].close();
            }
        }
        public void print(boolean b) {
            for (int i = 0; i < streams.length; i++) {
                streams[i].print(b);
            }
        }
        public void print(char c) {
            for (int i = 0; i < streams.length; i++) {
                streams[i].print(c);
            }
        }
        public void print(char[] s) {
            for (int i = 0; i < streams.length; i++) {
                streams[i].print(s);
            }
        }
        public void print(double d) {
            for (int i = 0; i < streams.length; i++) {
                streams[i].print(d);
            }
        }
        public void print(float f) {
            for (int i = 0; i < streams.length; i++) {
                streams[i].print(f);
            }
        }
        public void print(int b) {
            for (int i = 0; i < streams.length; i++) {
                streams[i].print(b);
            }
        }
        public void print(long l) {
            for (int i = 0; i < streams.length; i++) {
                streams[i].print(l);
            }
        }
        public void print(Object obj) {
            for (int i = 0; i < streams.length; i++) {
                streams[i].print(obj);
            }
        }
        public void print(String s) {
            for (int i = 0; i < streams.length; i++) {
                streams[i].print(s);
            }
        }
        public void println() {
            for (int i = 0; i < streams.length; i++) {
                streams[i].println();
            }
        }
        public void println(boolean x) {
            for (int i = 0; i < streams.length; i++) {
                streams[i].println(x);
            }
        }
        public void println(char x) {
            for (int i = 0; i < streams.length; i++) {
                streams[i].println(x);
            }
        }
        public void println(char[] x) {
            for (int i = 0; i < streams.length; i++) {
                streams[i].println(x);
            }
        }
        public void println(double x) {
            for (int i = 0; i < streams.length; i++) {
                streams[i].println(x);
            }
        }
        public void println(float x) {
            for (int i = 0; i < streams.length; i++) {
                streams[i].println(x);
            }
        }
        public void println(int x) {
            for (int i = 0; i < streams.length; i++) {
                streams[i].println(x);
            }
        }
        public void println(long x) {
            for (int i = 0; i < streams.length; i++) {
                streams[i].println(x);
            }
        }
        public void println(Object x) {
            for (int i = 0; i < streams.length; i++) {
                streams[i].println(x);
            }
        }
        public void println(String x) {
            for (int i = 0; i < streams.length; i++) {
                streams[i].println(x);
            }
        }
        public void write(byte[] buf, int off, int len) {
            for (int i = 0; i < streams.length; i++) {
                streams[i].write(buf, off, len);
            }
        }
        public void write(int b) {
            for (int i = 0; i < streams.length; i++) {
                streams[i].write(b);
            }
        }
        public void write(byte[] b) throws IOException {
            for (int i = 0; i < streams.length; i++) {
                streams[i].write(b);
            }
        }
    }
    
    /**
     * Used to compare (per line) the data being written to the output stream with
     * some expected data read from an input stream
     * @since 4.2
     */
    private static final class ComparingPrintStream extends PrintStream {
        private ByteArrayOutputStream byteStream = new ByteArrayOutputStream(2048);
        private PrintStream buf = new PrintStream(byteStream);
        private BufferedReader in;
        private int line = 0;
        
        private ArrayList unequalLines = new ArrayList();
        
        private ComparingPrintStream(OutputStream out, BufferedReader in) {
            super(out);
            this.in = in;
        }
        
        public void print(boolean b) {
            super.print(b);
            buf.print(b);
        }
        public void print(char c) {
            super.print(c);
            buf.print(c);
        }
        public void print(char[] s) {
            super.print(s);
            buf.print(s);
        }
        public void print(double d) {
            super.print(d);
            buf.print(d);
        }
        public void print(float f) {
            super.print(f);
            buf.print(f);
        }
        public void print(int i) {
            super.print(i);
            buf.print(i);
        }
        public void print(long l) {
            super.print(l);
            buf.print(l);
        }
        public void print(Object obj) {
            super.print(obj);
            buf.print(obj);
        }
        public void print(String s) {
            super.print(s);
            buf.print(s);
        }
        public void println() {
            super.println();
            compareLines();
        }
        public void println(boolean x) {
            super.println(x);
            compareLines();
        }
        public void println(char x) {
            super.println(x);
            compareLines();
        }
        public void println(char[] x) {
            super.println(x);
            compareLines();
        }
        public void println(double x) {
            super.println(x);
            compareLines();
        }
        public void println(float x) {
            super.println(x);
            compareLines();
        }
        public void println(int x) {
            super.println(x);
            compareLines();
        }
        public void println(long x) {
            super.println(x);
            compareLines();
        }
        public void println(Object x) {
            super.println(x);
            compareLines();
        }
        public void println(String x) {
            super.println(x);
            compareLines();
        }
        
        private void compareLines() {
            line++;
            buf.flush();
            String bufferedLine = byteStream.toString();
            byteStream.reset();
            try {
                String expectedLine = in.readLine();
                if (!bufferedLine.equals(expectedLine)) {
                    unequalLines.add("\n" + new Integer(line) + ":" + bufferedLine );
                }
            } catch (IOException e) {
                
            }
        }
        
        public List getUnequalLines() {
            return unequalLines;
        }
    }
}
