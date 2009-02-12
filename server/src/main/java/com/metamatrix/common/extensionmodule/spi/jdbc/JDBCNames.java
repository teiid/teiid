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

package com.metamatrix.common.extensionmodule.spi.jdbc;



/**
 *   The JDBCNames provides the binding JDBC table and column names used
 *   for the JDBC implementation
 *   </p>
 *   There are 2 sections to this class:
 *       a.  Table Definitions - defines the physical table layouts
 *       b.  Update Definitions - defines the table, columns, and keys using
 *               the DataBaseView that will be used when performing
 *               updates.
 */
public class JDBCNames {


    /*================================================
     *
     *      T A B L E      D E F I N I T I O N S
     *
     *================================================*/

    /**
     * The Extension Files table definition
     */
    public static class ExtensionFilesTable {
        public static final String TABLE_NAME               = "CS_EXT_FILES"; //$NON-NLS-1$
        public static class ColumnName {
            public static final String UID                  = "FILE_UID"; //$NON-NLS-1$
            public static final String CHECKSUM             = "CHKSUM"; //$NON-NLS-1$
            public static final String FILE_NAME            = "FILE_NAME"; //$NON-NLS-1$
            public static final String FILE_CONTENTS        = "FILE_CONTENTS"; //$NON-NLS-1$
            public static final String CONFIG_CONTENTS        = "CONFIG_CONTENTS"; //$NON-NLS-1$
            public static final String SEARCH_POSITION      = "SEARCH_POS"; //$NON-NLS-1$
            public static final String IS_ENABLED           = "IS_ENABLED"; //$NON-NLS-1$
            public static final String FILE_DESCRIPTION     = "FILE_DESC"; //$NON-NLS-1$
            public static final String CREATION_DATE        = "CREATION_DATE"; //$NON-NLS-1$
            public static final String CREATED_BY           = "CREATED_BY"; //$NON-NLS-1$
            public static final String UPDATED              = "UPDATE_DATE"; //$NON-NLS-1$
            public static final String UPDATED_BY           = "UPDATED_BY"; //$NON-NLS-1$
            public static final String FILE_TYPE            = "FILE_TYPE"; //$NON-NLS-1$
        }
    }


    /*================================================
     *
     *      U P D A T E      D E F I N I T I O N S
     *
     *================================================*/

    /**
     * The Extension Files update definition
     */
//    static class ExtensionFilesUpdate {
//
//        static class Columns {
//            static final DatabaseColumn UID                  = new DatabaseColumn(ExtensionFilesTable.ColumnName.UID,Integer.class,null,false,false);
//       	    static final DatabaseColumn CHECKSUM             = new DatabaseColumn(ExtensionFilesTable.ColumnName.CHECKSUM,Integer.class,null,false,false);
//            static final DatabaseColumn FILE_NAME            = new DatabaseColumn(ExtensionFilesTable.ColumnName.FILE_NAME,String.class,null,false,false);
//            //static final DatabaseColumn FILE_CONTENTS        = new DatabaseColumn(ExtensionFilesTable.ColumnName.FILE_CONTENTS,??.class,null,false,false);
//            static final DatabaseColumn SEARCH_POSITION      = new DatabaseColumn(ExtensionFilesTable.ColumnName.SEARCH_POSITION,Integer.class,null,false,false);
//            static final DatabaseColumn IS_ENABLED           = new DatabaseColumn(ExtensionFilesTable.ColumnName.IS_ENABLED,Boolean.class,Boolean.TRUE,false,false);
//            static final DatabaseColumn FILE_DESCRIPTION     = new DatabaseColumn(ExtensionFilesTable.ColumnName.FILE_DESCRIPTION,String.class,null,true,false);
//            static final DatabaseColumn CREATION_DATE        = new DatabaseColumn(ExtensionFilesTable.ColumnName.CREATION_DATE,String.class,null,true,false);
//            static final DatabaseColumn CREATED_BY           = new DatabaseColumn(ExtensionFilesTable.ColumnName.CREATED_BY,String.class,null,true,false);
//            static final DatabaseColumn UPDATED              = new DatabaseColumn(ExtensionFilesTable.ColumnName.UPDATED,String.class,null,true,false);
//            static final DatabaseColumn UPDATED_BY           = new DatabaseColumn(ExtensionFilesTable.ColumnName.UPDATED_BY,String.class,null,true,false);
//            static final DatabaseColumn FILE_TYPE            = new DatabaseColumn(ExtensionFilesTable.ColumnName.FILE_TYPE, String.class, null,false,false);
//        }
//        static class Keys {
//            static final DatabaseKey UID_KEY         = new DatabaseKey( new Object[] { ExtensionFilesUpdate.Columns.UID }, true );
//        }
//
//        static final DatabaseTable TABLE = new DatabaseTable(ExtensionFilesTable.TABLE_NAME,
//                                                new Object[] { ExtensionFilesUpdate.Columns.UID,
//                                                               ExtensionFilesUpdate.Columns.CHECKSUM,
//                                                               ExtensionFilesUpdate.Columns.FILE_NAME,
//                                                               //ExtensionFilesUpdate.Columns.FILE_CONTENTS,
//                                                               ExtensionFilesUpdate.Columns.SEARCH_POSITION,
//                                                               ExtensionFilesUpdate.Columns.IS_ENABLED,
//                                                               ExtensionFilesUpdate.Columns.FILE_DESCRIPTION,
//                                                               ExtensionFilesUpdate.Columns.CREATION_DATE,
//                                                               ExtensionFilesUpdate.Columns.CREATED_BY,
//                                                               ExtensionFilesUpdate.Columns.UPDATED,
//                                                               ExtensionFilesUpdate.Columns.UPDATED_BY,
//                                                               ExtensionFilesUpdate.Columns.FILE_TYPE},
//                                                new Object[] { ExtensionFilesUpdate.Keys.UID_KEY} );
//
//    }

}

