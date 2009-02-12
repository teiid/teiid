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

package com.metamatrix.platform.config.persistence.impl.jdbc;

import java.sql.ResultSet;
import java.sql.SQLException;

import com.metamatrix.common.config.StartupStateController;
import com.metamatrix.common.jdbc.JDBCReservedWords;
import com.metamatrix.core.util.DateUtil;

public class SQL_Translator {
    	
	    private static final String BLANK = " "; //$NON-NLS-1$
	    private static final String COMMA = ", "; //$NON-NLS-1$
//	    private static final String PARAM = "= ? ";
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
        
        private static final String OPEN_PAREN = "("; //$NON-NLS-1$
        private static final String CLOSED_PAREN = ")"; //$NON-NLS-1$
        
	    
    /*================================================
     *
     *      T A B L E      D E F I N I T I O N S
     *
     *================================================*/
     
   public static final String SELECT_STARTUP_STATE
                                    = SELECT
                                    +   StartupStateTable.ColumnName.STATE + COMMA
                                    +   StartupStateTable.ColumnName.LAST_CHANGED
                                    + FROM
                                    +   StartupStateTable.TABLE_NAME;
     
    public static final String UPDATE_STARTUP_STATE_UNCONDITIONAL
                                    = UPDATE
                                    +   StartupStateTable.TABLE_NAME + " " //$NON-NLS-1$
                                    + SET
                                    +   StartupStateTable.ColumnName.STATE + "= ?" + COMMA //$NON-NLS-1$
                                    +   StartupStateTable.ColumnName.LAST_CHANGED + "= ?"; //$NON-NLS-1$

    public static final String UPDATE_STARTUP_STATE_CONDITIONAL
                                    = UPDATE
                                    +   StartupStateTable.TABLE_NAME + " " //$NON-NLS-1$
                                    + SET
                                    +   StartupStateTable.ColumnName.STATE + "= ?" + COMMA //$NON-NLS-1$
                                    +   StartupStateTable.ColumnName.LAST_CHANGED + "= ?" //$NON-NLS-1$
                                    + WHERE
                                    +   StartupStateTable.ColumnName.STATE + "= ? "; //$NON-NLS-1$
    
    
    public static final String EMPTY_CS_SYSTEM_PROPERTY_TABLE
                                = DELETE
                                + FROM
                                +   SystemProperties.TABLE_NAME;
    
    public static final String INSERT_INTO_CS_SYSTEM_PROPERTY_TABLE
                                =  INSERT + INTO + SystemProperties.TABLE_NAME + OPEN_PAREN
                                + SystemProperties.ColumnName.PROPERTY_NAME + "," //$NON-NLS-1$
                                + SystemProperties.ColumnName.Property_VALUE + CLOSED_PAREN
                                + VALUES
                                + "(?,?)"; //$NON-NLS-1$

                                
    
    


    /**
     * @see StartupStateTable
     */
    static java.util.Date getServerStartupTime( ResultSet results) throws SQLException {

        java.util.Date timestamp = null;

        if ( results.next() ) {

            int state = results.getInt(StartupStateTable.ColumnName.STATE);
            if (state == StartupStateController.STATE_STARTED) {
                String lastChanged = results.getString(StartupStateTable.ColumnName.LAST_CHANGED);
                try {
                    timestamp = DateUtil.convertStringToDate(lastChanged);
                } catch (Exception e) {
                    // bogus date so just ignore and return null.
                }
            }
        }
        return timestamp;
    }
    
    static int getStartupState(ResultSet results) throws SQLException {
        return results.getInt(StartupStateTable.ColumnName.STATE);
    }
    


    /**
     * The table definition for system startup state
     */
    public static class StartupStateTable {
        public static final String TABLE_NAME                 = "CFG_STARTUP_STATE"; //$NON-NLS-1$
        public static class ColumnName {
            public static final String STATE                  = "STATE"; //$NON-NLS-1$
            public static final String LAST_CHANGED           = "LASTCHANGED"; //$NON-NLS-1$
        }
    }

    public static class SystemProperties {
        public static final String TABLE_NAME                 = "CS_SYSTEM_PROPS"; //$NON-NLS-1$
        public static class ColumnName {
            public static final String PROPERTY_NAME            = "PROPERTY_NAME"; //$NON-NLS-1$
            public static final String Property_VALUE           = "Property_VALUE"; //$NON-NLS-1$
        }
    }    
	    

}
