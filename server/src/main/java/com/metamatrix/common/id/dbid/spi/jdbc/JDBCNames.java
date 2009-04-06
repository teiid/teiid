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

package com.metamatrix.common.id.dbid.spi.jdbc;

import com.metamatrix.common.jdbc.JDBCReservedWords;

public class JDBCNames {

  // minimum is used to specify the starting poing for the initial insert
  // into the IDTable.  It is set initially at 1000 just in case there
  // are already used ids for a given context.
	  public static final long START_ID = 1000;
    private static final String INSERT      = JDBCReservedWords.INSERT + " "; //$NON-NLS-1$
    private static final String UPDATE      = JDBCReservedWords.UPDATE + " "; //$NON-NLS-1$
//    private static final String DELETE      = JDBCReservedWords.DELETE + " ";
    private static final String SELECT      = JDBCReservedWords.SELECT + " "; //$NON-NLS-1$
    private static final String FROM        = " " + JDBCReservedWords.FROM + " "; //$NON-NLS-1$ //$NON-NLS-2$
    private static final String WHERE       = " " + JDBCReservedWords.WHERE + " "; //$NON-NLS-1$ //$NON-NLS-2$
//    private static final String ORDER_BY    = " " + JDBCReservedWords.ORDER_BY + " ";
    private static final String SET         = " " + JDBCReservedWords.SET + " "; //$NON-NLS-1$ //$NON-NLS-2$
//    private static final String ON          = " " + JDBCReservedWords.ON + " ";
    private static final String INTO        = " " + JDBCReservedWords.INTO + " "; //$NON-NLS-1$ //$NON-NLS-2$
//    private static final String INNER_JOIN  = " " + JDBCReservedWords.INNER_JOIN + " ";
//    private static final String DISTINCT    = " " + JDBCReservedWords.DISTINCT + " ";
//    private static final String VALUES      = " " + JDBCReservedWords.VALUES + " ";
    private static final String AND         = " " + JDBCReservedWords.AND + " "; //$NON-NLS-1$ //$NON-NLS-2$
//    private static final String LIKE        = " " + JDBCReservedWords.LIKE + " ";



    /**
     * This class defines the columns in the TransactionID table.
     */
    public static class IDTable {
        public static final String TABLE_NAME = "IDTABLE"; //$NON-NLS-1$
        public static class ColumnName {
            public static final String ID_CONTEXT = "IDCONTEXT"; //$NON-NLS-1$
            public static final String NEXT_ID = "NEXTID"; //$NON-NLS-1$
        }
    }

    // ---------------------------------------------------------------------------------
    //                     S E L E C T    S T A T E  M E N T S
    // ---------------------------------------------------------------------------------


    /** Sql statement use to select an ID from the TransactionID table */
    public static final String SELECT_ID_BLOCK
                                    = SELECT
                                    +   JDBCNames.IDTable.ColumnName.NEXT_ID
                                    + FROM
                                    +   JDBCNames.IDTable.TABLE_NAME
                                    + WHERE
                                    +   JDBCNames.IDTable.ColumnName.ID_CONTEXT + "= ?"; //$NON-NLS-1$

    /** Sql statement use to update an ID in the TransactionID table */
    public static final String UPDATE_ID_BLOCK
                                    = UPDATE
                                    + JDBCNames.IDTable.TABLE_NAME
                                    + SET
                                    +   JDBCNames.IDTable.ColumnName.NEXT_ID + "= ?" //$NON-NLS-1$
                                    + WHERE
                                    +   JDBCNames.IDTable.ColumnName.ID_CONTEXT + "= ?"; //$NON-NLS-1$

    /** Sql statement use to insert an ID in the TransactionID table */
    public static final String INSERT_ID_BLOCK
                                    = INSERT + INTO
                                    + JDBCNames.IDTable.TABLE_NAME
                                    + " (" //$NON-NLS-1$
                                    +   JDBCNames.IDTable.ColumnName.ID_CONTEXT + ", " //$NON-NLS-1$
                                    +   JDBCNames.IDTable.ColumnName.NEXT_ID
                                    + ") VALUES ( ?, ? )"; //$NON-NLS-1$

}

