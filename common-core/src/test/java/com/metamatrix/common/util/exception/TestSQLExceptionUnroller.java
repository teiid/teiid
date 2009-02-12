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

package com.metamatrix.common.util.exception;

import java.sql.SQLException;
import java.util.Random;

import junit.framework.TestCase;

/**
 * @version 	1.0
 * @author
 */
public class TestSQLExceptionUnroller extends TestCase {

    /**
     * Constructor for TestSQLExceptionUnroller.
     * @param name
     */
    public TestSQLExceptionUnroller(String name) {
        super(name);
    }

    //  ********* H E L P E R   M E T H O D S  *********

    private void helpTestUnroll(SQLException sqlEx, SQLException mmEx) {
        // Get the original SQLException chain
        StringBuffer expectedResult = new StringBuffer(getSQLErrorMsg(sqlEx));
        SQLException sqlChild = sqlEx.getNextException();
        while ( sqlChild != null ) {
            expectedResult.append("|" + getSQLErrorMsg(sqlChild)); //$NON-NLS-1$
            sqlChild = sqlChild.getNextException();
        }

        // Get the "unrolled" chain
        StringBuffer result = new StringBuffer(getSQLErrorMsg(mmEx));
        sqlChild = mmEx.getNextException();
        while ( sqlChild != null ) {
            result.append("|" + getSQLErrorMsg(sqlChild)); //$NON-NLS-1$
            sqlChild = sqlChild.getNextException();
        }

        assertEquals("Unexpected unroll result!\n", expectedResult.toString(), result.toString()); //$NON-NLS-1$
    }

    private static String getSQLErrorMsg(SQLException e) {
        StringBuffer buf = new StringBuffer(e.getMessage());
        buf.append("|" + e.getSQLState()); //$NON-NLS-1$
        buf.append("|" + e.getErrorCode()); //$NON-NLS-1$
        // DEBUG:
//        System.out.println("Emsg: " + buf.toString());
        return buf.toString();
    }

    private static SQLException helpTestRoll(int limit) {
        String sqlState = "confusion %^D"; //$NON-NLS-1$
        int vendorCode = -1;

        // Create a chain of SQLExceptions
        final SQLException e = new SQLException("Unroll 0", sqlState, vendorCode); //$NON-NLS-1$

        Random gen = new Random();
        for ( int i = 0; i < limit - 1; i++ ) {
            sqlState = String.valueOf(gen.nextDouble());
            vendorCode = gen.nextInt();
            SQLException e2 = null;
            if ( vendorCode % 2 == 0 ) {
                e2 = new SQLException("Unroll " + (i + 1), sqlState, vendorCode); //$NON-NLS-1$
            } else {
                // No SQLState (null) or vendorCode (0) should appear
                // in SQLException when vendorCode is odd.
                e2 = new SQLException("Unroll " + (i + 1)); //$NON-NLS-1$
            }
            e.setNextException(e2);
        }

        return e;
    }

    //  ********* T E S T   S U I T E   M E T H O D S  *********

    public void testUnroll_1() {
        // Create a SQLException
        final SQLException e = helpTestRoll(1);

        // Unroll the exception
        SQLException mmEx = SQLExceptionUnroller.unRollException(e);

        // Make sure they're the same.
        helpTestUnroll(e, mmEx);
    }

    public void testUnroll_10() {
        // Create a SQLException
        final SQLException e = helpTestRoll(10);

        // Unroll the exception
        SQLException mmEx = SQLExceptionUnroller.unRollException(e);

        // Make sure they're the same.
        helpTestUnroll(e, mmEx);
    }
}
