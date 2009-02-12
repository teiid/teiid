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

package com.metamatrix.core.commandshell;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.StringReader;
import java.util.Iterator;
import java.util.List;

import com.metamatrix.core.MetaMatrixRuntimeException;
import com.metamatrix.core.util.StringUtil;

/**
 * Provides utility methods for manipulating the results of executing commands against a connector.
 */
public class ConnectorResultUtility {
    private static final String ERROR_MESSAGE_PREFIX = "CompareResults Error: "; //$NON-NLS-1$
    private static final String NULL_STRING = "<null>"; //$NON-NLS-1$
    private static final String COLUMN_SEPARATOR = "\t"; //$NON-NLS-1$
    private static final String NOROWS_STRING = "No rows returned."; //$NON-NLS-1$

    /**
     * Converts a List of Lists, where each sub-list is a row of data, into a tab delimited String where each line is
     * a row of data.  Each object is converted to a String.
     * @param results a List of Lists of data typcially obtained by executing a command on a connector through the ConnectorHost
     * @return the results converted to a tab delimited String
     */
    public static String resultsToString(List results, String[] columnNames) {
        if (results == null || results.isEmpty()) {
            return NOROWS_STRING;
        }
        StringBuffer result = new StringBuffer();
        int columnIndex = 0;
        for (Iterator iterator = results.iterator(); iterator.hasNext();) {

            List row = (List) iterator.next();
            boolean firstColumn = true;
            columnIndex = 0;
            for (Iterator j = row.iterator(); j.hasNext(); columnIndex++) {
                Object next = j.next();
                String value = null;
                if (next == null) {
                    value = NULL_STRING;
                } else {
                    value = next.toString();
                    value = StringUtil.replaceAll(value, "\t", "\\t"); //$NON-NLS-1$ //$NON-NLS-2$
                    value = StringUtil.replaceAll(value, "\n", "\\n"); //$NON-NLS-1$ //$NON-NLS-2$
                }
                if (firstColumn) {
                    firstColumn = false;
                } else {
                    result.append(COLUMN_SEPARATOR);
                }
                result.append(value);
            }
            result.append(StringUtil.LINE_SEPARATOR);
        }

        if (columnNames == null || columnNames.length == 0) {
            columnNames = new String[columnIndex];
            for (int i = 0; i < columnIndex; i++) {
                columnNames[i] = "col" + (i+1); //$NON-NLS-1$
            }
        }
        StringBuffer header = new StringBuffer();
        boolean firstValue = true;
        for (int i=0; i < columnNames.length; i++) {
            String label = null;
            if (columnNames[i] == null) {
                label = NULL_STRING;
            } else {
                label = columnNames[i];
                
                int delimiterIndex = columnNames[i].lastIndexOf('.');
                if (delimiterIndex != -1) {
                    label = columnNames[i].substring(delimiterIndex + 1);
                }
            }
            if (firstValue) {
                firstValue = false;
            } else {
                header.append(COLUMN_SEPARATOR);
            }
            header.append(label);
        }
        header.append(StringUtil.LINE_SEPARATOR);
        return header.toString() + result.toString();
    }
    
    public static String resultsToString(List results) {
        return resultsToString(results, null);
    }

    private static int getRowCount(String text) throws IOException {
        BufferedReader reader = new BufferedReader(new StringReader(text));
        int rowCount = 0;
        String line = reader.readLine();
        while (line != null) {
            rowCount++;
            line = reader.readLine();
        }
        return rowCount;
    }

    /**
     * Compares two sets of results to determine if they are identical.
     * The results are in the string form provided by the resultsToString method.
     * @param expected String form of the expected results
     * @param actual String form of the actual results
     * @return null if the String are identical or a description of the first difference if they are different
     */
    public static String compareResultsStrings(String expected, String actual) {
        if (expected.equals(actual)) {
            return null;
        }
        try {
            BufferedReader expectedReader = new BufferedReader(new StringReader(expected));
            BufferedReader actualReader = new BufferedReader(new StringReader(actual));

            int expectedRowCount = getRowCount(expected);
            int actualRowCount = getRowCount(actual);

            if (expectedRowCount > actualRowCount) {
                return (ERROR_MESSAGE_PREFIX + "Expected " + expectedRowCount + //$NON-NLS-1$
                " records but received only " + actualRowCount); //$NON-NLS-1$
            } else if (actualRowCount > expectedRowCount) {
                // Check also for less records than expected
                return (ERROR_MESSAGE_PREFIX + "Expected " + expectedRowCount + //$NON-NLS-1$
                " records but received " + actualRowCount); //$NON-NLS-1$
            }

            String expectedLine = expectedReader.readLine();
            int rowIndex = 0;
            while (expectedLine != null) {
                String actualLine = actualReader.readLine();

                List actualRow = StringUtil.split(actualLine, COLUMN_SEPARATOR);
                List expectedRow = StringUtil.split(expectedLine, COLUMN_SEPARATOR);
                int actualColumnCount = actualRow.size();
                int expectedColumnCount = expectedRow.size();

                //TODO: i18n the messages
                
                // Get actual value
                if (actualColumnCount != expectedColumnCount) {
                    return("Incorrect number of columns at row = " + rowIndex + ", expected = " + expectedColumnCount + ", actual = " //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
                                                    + actualColumnCount);
                }

                // Compare sorted actual record with sorted expected record column by column
                for (int columnIndex = 0; columnIndex < expectedColumnCount; columnIndex++) {
                    // Get expected value
                    Object expectedValue = expectedRow.get(columnIndex);
                    Object actualValue = actualRow.get(columnIndex);

                    // Compare these values
                    if (expectedValue == null) {
                        if (actualValue != null) {
                            return getMismatchMessage(rowIndex, columnIndex, expectedValue, actualValue);
                        }
                    } else {
                        if (!expectedValue.equals(actualValue)) {
                            return getMismatchMessage(rowIndex, columnIndex, expectedValue, actualValue);
                        }
                    }
                } // end loop through columns
                expectedLine = expectedReader.readLine();

                rowIndex++;
            } // end loop through rows    }
        } catch (IOException e) {
            //this should not happen because the code is simply reading from Strings in memory
            throw new MetaMatrixRuntimeException(e);
        }
        return null;
    }

    private static String getMismatchMessage(int rowIndex, int columnIndex, Object expectedValue, Object actualValue) {
        return (ERROR_MESSAGE_PREFIX + "Value mismatch at row " + rowIndex //$NON-NLS-1$
        + " and column " + columnIndex //$NON-NLS-1$
        + ": expected = " //$NON-NLS-1$
        + expectedValue + ", actual = " //$NON-NLS-1$
        + actualValue);
    }
}
