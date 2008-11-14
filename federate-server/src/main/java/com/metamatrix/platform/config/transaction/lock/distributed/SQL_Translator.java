/*
 * JBoss, Home of Professional Open Source.
 * Copyright (C) 2008 Red Hat, Inc.
 * Copyright (C) 2000-2007 MetaMatrix, Inc.
 * Licensed to Red Hat, Inc. under one or more contributor 
 * license agreements.  See the copyright.txt file in the
 * distribution for a full listing of individual contributors.
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

package com.metamatrix.platform.config.transaction.lock.distributed;

import com.metamatrix.common.jdbc.JDBCReservedWords;

public class SQL_Translator {
    	
	    private static final String BLANK = " "; //$NON-NLS-1$
	    private static final String COMMA = ", "; //$NON-NLS-1$
	    private static final String PARAM = "= ? "; //$NON-NLS-1$
//	    private static final String PERIOD = ".";
	
	    private static final String INSERT      = JDBCReservedWords.INSERT + BLANK;
	    private static final String UPDATE      = JDBCReservedWords.UPDATE + BLANK;
	    private static final String DELETE      = JDBCReservedWords.DELETE + BLANK;
	    private static final String SELECT      = JDBCReservedWords.SELECT + BLANK;
	    private static final String FROM        = BLANK + JDBCReservedWords.FROM + BLANK;
	    private static final String WHERE       = BLANK + JDBCReservedWords.WHERE + BLANK;
//	    private static final String ORDER_BY    = BLANK + JDBCReservedWords.ORDER_BY + BLANK;
	    private static final String SET         = BLANK + JDBCReservedWords.SET + BLANK;
//	    private static final String ON          = BLANK + JDBCReservedWords.ON + BLANK;
	    private static final String INTO        = BLANK + JDBCReservedWords.INTO + BLANK;
//	    private static final String EQUAL       = BLANK + "=" + BLANK;
//	    private static final String DISTINCT    = BLANK + JDBCReservedWords.DISTINCT + BLANK;
	    private static final String VALUES      = BLANK + JDBCReservedWords.VALUES + BLANK;
//	    private static final String AND         = BLANK + JDBCReservedWords.AND + BLANK;
	    
    /*================================================
     *
     *      T A B L E      D E F I N I T I O N S
     *
     *================================================*/

    /**
     */
    public static class ConfigurationLockTable {
        public static final String TABLE_NAME               = "CFG_LOCK"; //$NON-NLS-1$
        public static class ColumnName {
            public static final String HOST                 = "HOST"; //$NON-NLS-1$
            public static final String USER_NAME	          = "USER_NAME"; //$NON-NLS-1$
            public static final String DATETIME_ACQUIRED    = "DATETIME_ACQUIRED"; //$NON-NLS-1$
            public static final String DATETIME_EXPIRE      = "DATETIME_EXPIRE"; //$NON-NLS-1$
            public static final String LOCK_TYPE	          = "LOCK_TYPE"; //$NON-NLS-1$
            
        }
    }
	    
    
 	public static final String SELECT_LOCK
                                    = SELECT
                                    +   ConfigurationLockTable.ColumnName.HOST + COMMA
                                    +   ConfigurationLockTable.ColumnName.USER_NAME + COMMA
                                    +   ConfigurationLockTable.ColumnName.DATETIME_ACQUIRED + COMMA
                                    +   ConfigurationLockTable.ColumnName.DATETIME_EXPIRE + COMMA 
                                    +   ConfigurationLockTable.ColumnName.LOCK_TYPE                                     
                                    + FROM
                                    +   ConfigurationLockTable.TABLE_NAME
                                    ;   


 	public static final String SELECT_LOCK_WHERE_EQUAL_USERNAME
                                    = SELECT
                                    +   ConfigurationLockTable.ColumnName.HOST + COMMA
                                    +   ConfigurationLockTable.ColumnName.USER_NAME + COMMA
                                    +   ConfigurationLockTable.ColumnName.DATETIME_ACQUIRED + COMMA
                                    +   ConfigurationLockTable.ColumnName.DATETIME_EXPIRE + COMMA
                                    +   ConfigurationLockTable.ColumnName.LOCK_TYPE                                     
                                    + FROM
                                    +   ConfigurationLockTable.TABLE_NAME
                                    + WHERE
                                    +   ConfigurationLockTable.ColumnName.USER_NAME + PARAM 
                                    ;   


    public static final String INSERT_LOCK
                                    = INSERT + INTO + ConfigurationLockTable.TABLE_NAME + "(" //$NON-NLS-1$
                                        + ConfigurationLockTable.ColumnName.HOST + "," //$NON-NLS-1$
                                        + ConfigurationLockTable.ColumnName.USER_NAME + "," //$NON-NLS-1$
                                        + ConfigurationLockTable.ColumnName.DATETIME_ACQUIRED + "," //$NON-NLS-1$
                                        + ConfigurationLockTable.ColumnName.DATETIME_EXPIRE + "," //$NON-NLS-1$
                                        + ConfigurationLockTable.ColumnName.LOCK_TYPE + ")" //$NON-NLS-1$
                                        + VALUES
                                        + "(?,?,?,?,?)"; //$NON-NLS-1$
                                        
     public static final String DELETE_LOCK
     								= DELETE + FROM + ConfigurationLockTable.TABLE_NAME;
                                       
     public static final String UPDATE_LOCK
     								= UPDATE + ConfigurationLockTable.TABLE_NAME
     								+ SET
                                    +   ConfigurationLockTable.ColumnName.HOST + "= ?" + COMMA //$NON-NLS-1$
                                    +   ConfigurationLockTable.ColumnName.USER_NAME + "= ?" + COMMA //$NON-NLS-1$
                                    +   ConfigurationLockTable.ColumnName.DATETIME_ACQUIRED + "= ?" + COMMA //$NON-NLS-1$
                                    +   ConfigurationLockTable.ColumnName.DATETIME_EXPIRE + "= ?" + COMMA //$NON-NLS-1$
                                    +   ConfigurationLockTable.ColumnName.LOCK_TYPE + "= ?"                                   //$NON-NLS-1$
									;

}
