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

import com.metamatrix.common.jdbc.JDBCReservedWords;

/**
* The JDBCConfigurationTranslator provides the binding between the JDBC data source
* and the Java object.
*
*/

final class JDBCExtensionModuleTranslator {
    private static final String COMMA = ", "; //$NON-NLS-1$

    private static final String BLANK = " "; //$NON-NLS-1$
    private static final String PARAM = "= ? "; //$NON-NLS-1$
    private static final String OPEN_PAREN = "("; //$NON-NLS-1$
    private static final String CLOSED_PAREN = ")"; //$NON-NLS-1$

    private static final String DELETE      = JDBCReservedWords.DELETE + BLANK;

    private static final String INSERT      = JDBCReservedWords.INSERT + BLANK;
    private static final String UPDATE      = JDBCReservedWords.UPDATE + BLANK;
    private static final String SELECT      = JDBCReservedWords.SELECT + BLANK;
    private static final String FROM        = BLANK + JDBCReservedWords.FROM + BLANK;
    private static final String WHERE       = BLANK + JDBCReservedWords.WHERE + BLANK;
    private static final String ORDER_BY    = BLANK + JDBCReservedWords.ORDER_BY + BLANK;
    private static final String SET         = BLANK + JDBCReservedWords.SET + BLANK;
    private static final String INTO        = BLANK + JDBCReservedWords.INTO + BLANK;
    private static final String VALUES      = BLANK + JDBCReservedWords.VALUES + BLANK;
    private static final String MAX         = BLANK + "MAX"; //$NON-NLS-1$
    private static final String COUNT       = BLANK + "COUNT(*)"; //$NON-NLS-1$

    // ---------------------------------------------------------------------------------
    //                     S E L E C T    S T A T E  M E N T S
    // ---------------------------------------------------------------------------------

/************************************

   M U L T I P L E   R O W   R E T U R N   S Q L

************************************/

    public static final String SELECT_ALL_SOURCE_NAMES
                                   = SELECT
                                    +   JDBCNames.ExtensionFilesTable.ColumnName.FILE_NAME
                                    + FROM
                                    +   JDBCNames.ExtensionFilesTable.TABLE_NAME
                                    + ORDER_BY
                                    +   JDBCNames.ExtensionFilesTable.ColumnName.SEARCH_POSITION
                                    ;



/************************************

   S I N G L E   R O W  R E T U R N  S Q L

************************************/

    public static final String SELECT_SOURCE_FILE_DATA
                                   = SELECT
                                    +   JDBCNames.ExtensionFilesTable.ColumnName.FILE_CONTENTS
                                    + FROM
                                    +   JDBCNames.ExtensionFilesTable.TABLE_NAME
                                    + WHERE
                                    +  JDBCNames.ExtensionFilesTable.ColumnName.UID + PARAM
                                    ;

    public static final String SELECT_SOURCE_FILE_DATA_BY_NAME
                                   = SELECT
                                    +   JDBCNames.ExtensionFilesTable.ColumnName.FILE_CONTENTS
                                    + FROM
                                    +   JDBCNames.ExtensionFilesTable.TABLE_NAME
                                    + WHERE
                                    +  JDBCNames.ExtensionFilesTable.ColumnName.FILE_NAME + PARAM
                                    ;
                                    
    
    public static final String SELECT_SOURCE_CHECKSUM_AND_TYPE_BY_NAME
                                   = SELECT
                                    +   JDBCNames.ExtensionFilesTable.ColumnName.CHECKSUM + COMMA                                    
                                    +   JDBCNames.ExtensionFilesTable.ColumnName.FILE_TYPE
                                    + FROM
                                    +   JDBCNames.ExtensionFilesTable.TABLE_NAME
                                    + WHERE
                                    +  JDBCNames.ExtensionFilesTable.ColumnName.FILE_NAME + PARAM
                                    ;
    
    public static final String SELECT_MAX_SEARCH_POSITION
                                   = SELECT
                                    +   MAX + OPEN_PAREN
                                    +   JDBCNames.ExtensionFilesTable.ColumnName.SEARCH_POSITION
                                    +   CLOSED_PAREN
                                    + FROM
                                    +   JDBCNames.ExtensionFilesTable.TABLE_NAME
                                    ;

    public static final String SELECT_ROW_COUNT
                                   = SELECT
                                    +   COUNT
                                    + FROM
                                    +   JDBCNames.ExtensionFilesTable.TABLE_NAME
                                    ;


    public static final String SELECT_FILE_UID_BY_NAME
                                   = SELECT
                                    +   JDBCNames.ExtensionFilesTable.ColumnName.UID
                                    + FROM
                                    +   JDBCNames.ExtensionFilesTable.TABLE_NAME
                                    + WHERE
                                    +  JDBCNames.ExtensionFilesTable.ColumnName.FILE_NAME + PARAM
                                    ;
                                    

    public static final String SELECT_DESCRIPTOR_INFO
                                 = SELECT
                                 +    JDBCNames.ExtensionFilesTable.ColumnName.FILE_NAME + COMMA 
                                +    JDBCNames.ExtensionFilesTable.ColumnName.FILE_DESCRIPTION + COMMA 
                                +    JDBCNames.ExtensionFilesTable.ColumnName.FILE_TYPE + COMMA 
                                +    JDBCNames.ExtensionFilesTable.ColumnName.SEARCH_POSITION + COMMA 
                                +    JDBCNames.ExtensionFilesTable.ColumnName.IS_ENABLED + COMMA 
                                +    JDBCNames.ExtensionFilesTable.ColumnName.CREATED_BY + COMMA 
                                +    JDBCNames.ExtensionFilesTable.ColumnName.CREATION_DATE + COMMA 
                                +    JDBCNames.ExtensionFilesTable.ColumnName.UPDATED_BY + COMMA 
                                +    JDBCNames.ExtensionFilesTable.ColumnName.UPDATED + COMMA 
                                +    JDBCNames.ExtensionFilesTable.ColumnName.CHECKSUM + " " //$NON-NLS-1$                                
                                 + FROM
                                 +    JDBCNames.ExtensionFilesTable.TABLE_NAME + " " //$NON-NLS-1$
                                 + WHERE
                                 +    JDBCNames.ExtensionFilesTable.ColumnName.FILE_NAME + " = ? " //$NON-NLS-1$
                                 ;


    public static final String SELECT_ALL_DESCRIPTORS_INFO_BY_TYPE
                                 = SELECT
                                 +    JDBCNames.ExtensionFilesTable.ColumnName.FILE_NAME + COMMA 
                                +    JDBCNames.ExtensionFilesTable.ColumnName.FILE_DESCRIPTION + COMMA 
                                +    JDBCNames.ExtensionFilesTable.ColumnName.FILE_TYPE + COMMA 
                                +    JDBCNames.ExtensionFilesTable.ColumnName.SEARCH_POSITION + COMMA 
                                +    JDBCNames.ExtensionFilesTable.ColumnName.IS_ENABLED + COMMA 
                                +    JDBCNames.ExtensionFilesTable.ColumnName.CREATED_BY + COMMA 
                                +    JDBCNames.ExtensionFilesTable.ColumnName.CREATION_DATE + COMMA 
                                +    JDBCNames.ExtensionFilesTable.ColumnName.UPDATED_BY + COMMA 
                                +    JDBCNames.ExtensionFilesTable.ColumnName.UPDATED + COMMA 
                                +    JDBCNames.ExtensionFilesTable.ColumnName.CHECKSUM + " " //$NON-NLS-1$                                
                                 + FROM
                                 +    JDBCNames.ExtensionFilesTable.TABLE_NAME + " " //$NON-NLS-1$
                                + WHERE
                                +    JDBCNames.ExtensionFilesTable.ColumnName.FILE_TYPE + " = ? " //$NON-NLS-1$
                                ;


    public static final String SELECT_ALL_DESCRIPTORS_INFO
                                 = SELECT
                                 +    JDBCNames.ExtensionFilesTable.ColumnName.FILE_NAME + COMMA 
                                +    JDBCNames.ExtensionFilesTable.ColumnName.FILE_DESCRIPTION + COMMA 
                                +    JDBCNames.ExtensionFilesTable.ColumnName.FILE_TYPE + COMMA 
                                +    JDBCNames.ExtensionFilesTable.ColumnName.SEARCH_POSITION + COMMA 
                                +    JDBCNames.ExtensionFilesTable.ColumnName.IS_ENABLED + COMMA 
                                +    JDBCNames.ExtensionFilesTable.ColumnName.CREATED_BY + COMMA 
                                +    JDBCNames.ExtensionFilesTable.ColumnName.CREATION_DATE + COMMA 
                                +    JDBCNames.ExtensionFilesTable.ColumnName.UPDATED_BY + COMMA 
                                +    JDBCNames.ExtensionFilesTable.ColumnName.UPDATED + COMMA 
                                +    JDBCNames.ExtensionFilesTable.ColumnName.CHECKSUM + " " //$NON-NLS-1$                                
                                 + FROM
                                 +    JDBCNames.ExtensionFilesTable.TABLE_NAME + " " //$NON-NLS-1$
                                 ;


//***********************************
//
//      U N I Q U E    I D     S Q L
//
//***********************************



//*******************************
//
//   U P D A T E      S Q L
//
//*******************************

  public static final String ADD_SOURCE_FILE_DATA
                                   = INSERT + INTO + JDBCNames.ExtensionFilesTable.TABLE_NAME + OPEN_PAREN
                                       + JDBCNames.ExtensionFilesTable.ColumnName.UID + "," //$NON-NLS-1$
                                       + JDBCNames.ExtensionFilesTable.ColumnName.CHECKSUM + "," //$NON-NLS-1$
                                       + JDBCNames.ExtensionFilesTable.ColumnName.FILE_NAME + "," //$NON-NLS-1$
                                       + JDBCNames.ExtensionFilesTable.ColumnName.SEARCH_POSITION + "," //$NON-NLS-1$
                                       + JDBCNames.ExtensionFilesTable.ColumnName.IS_ENABLED + "," //$NON-NLS-1$
                                       + JDBCNames.ExtensionFilesTable.ColumnName.FILE_DESCRIPTION + "," //$NON-NLS-1$
                                       + JDBCNames.ExtensionFilesTable.ColumnName.CREATED_BY + "," //$NON-NLS-1$
                                       + JDBCNames.ExtensionFilesTable.ColumnName.CREATION_DATE + "," //$NON-NLS-1$
                                       + JDBCNames.ExtensionFilesTable.ColumnName.UPDATED_BY + "," //$NON-NLS-1$
                                       + JDBCNames.ExtensionFilesTable.ColumnName.UPDATED + "," //$NON-NLS-1$
                                        + JDBCNames.ExtensionFilesTable.ColumnName.FILE_TYPE + CLOSED_PAREN
                                       // + JDBCNames.ExtensionFilesTable.ColumnName.FILE_CONTENTS + CLOSED_PAREN
                                       + VALUES
                                       ;

    public static final String ADD_SOURCE_FILE_DATA_PARAMS
                                        = "(?,?,?,?,?,?,?,?,?,?,?)"; //$NON-NLS-1$
    public static final String ADD_SOURCE_FILE_DATA_ORACLE_PARAMS
                                        = "(?,?,?,?,?,?,?,?,?,?,?,empty_blob())"; //$NON-NLS-1$

    public static final String UPDATE_SOURCE_FILE_DATA_DEFAULT
                                    = UPDATE
                                    +   JDBCNames.ExtensionFilesTable.TABLE_NAME + " " //$NON-NLS-1$
                                    + SET
                                    +   JDBCNames.ExtensionFilesTable.ColumnName.UPDATED_BY + "= ?, " //$NON-NLS-1$
                                    +   JDBCNames.ExtensionFilesTable.ColumnName.UPDATED + "= ?, " //$NON-NLS-1$
                                    +   JDBCNames.ExtensionFilesTable.ColumnName.CHECKSUM + "= ?, " //$NON-NLS-1$
                                    +   JDBCNames.ExtensionFilesTable.ColumnName.FILE_CONTENTS + "= ? " //$NON-NLS-1$
                                    + WHERE
                                    +   JDBCNames.ExtensionFilesTable.ColumnName.FILE_NAME + "= ? "; //$NON-NLS-1$

	public static final String UPDATE_SOURCE_FILE_DATA_ORACLE
                                    = UPDATE
                                    +   JDBCNames.ExtensionFilesTable.TABLE_NAME + " " //$NON-NLS-1$
                                    + SET
                                    +   JDBCNames.ExtensionFilesTable.ColumnName.UPDATED_BY + "= ?, " //$NON-NLS-1$
                                    +   JDBCNames.ExtensionFilesTable.ColumnName.UPDATED + "= ?, " //$NON-NLS-1$
                                    +   JDBCNames.ExtensionFilesTable.ColumnName.CHECKSUM + "= ?, " //$NON-NLS-1$
                                    +   JDBCNames.ExtensionFilesTable.ColumnName.FILE_CONTENTS + "= empty_blob() " //$NON-NLS-1$
                                    + WHERE
                                    +   JDBCNames.ExtensionFilesTable.ColumnName.FILE_NAME + "= ? "; //$NON-NLS-1$
                                    
    public static final String UPDATE_SOURCE_NAME
                                    = UPDATE
                                    +   JDBCNames.ExtensionFilesTable.TABLE_NAME + " " //$NON-NLS-1$
                                    + SET
                                    +   JDBCNames.ExtensionFilesTable.ColumnName.FILE_NAME + "= ?, " //$NON-NLS-1$
                                    +   JDBCNames.ExtensionFilesTable.ColumnName.UPDATED_BY + "= ?, " //$NON-NLS-1$
                                    +   JDBCNames.ExtensionFilesTable.ColumnName.UPDATED + "= ? " //$NON-NLS-1$
                                    + WHERE
                                    +   JDBCNames.ExtensionFilesTable.ColumnName.FILE_NAME + "= ? "; //$NON-NLS-1$

    public static final String UPDATE_SOURCE_DESCRIPTION
                                    = UPDATE
                                    +   JDBCNames.ExtensionFilesTable.TABLE_NAME + " " //$NON-NLS-1$
                                    + SET
                                    +   JDBCNames.ExtensionFilesTable.ColumnName.FILE_DESCRIPTION + "= ?, " //$NON-NLS-1$
                                    +   JDBCNames.ExtensionFilesTable.ColumnName.UPDATED_BY + "= ?, " //$NON-NLS-1$
                                    +   JDBCNames.ExtensionFilesTable.ColumnName.UPDATED + "= ? " //$NON-NLS-1$
                                    + WHERE
                                    +   JDBCNames.ExtensionFilesTable.ColumnName.FILE_NAME + "= ? "; //$NON-NLS-1$

    public static final String UPDATE_SOURCE_SEARCH_POSITION
                                    = UPDATE
                                    +   JDBCNames.ExtensionFilesTable.TABLE_NAME + " " //$NON-NLS-1$
                                    + SET
                                    +   JDBCNames.ExtensionFilesTable.ColumnName.SEARCH_POSITION + "= ?, " //$NON-NLS-1$
                                    +   JDBCNames.ExtensionFilesTable.ColumnName.UPDATED_BY + "= ?, " //$NON-NLS-1$
                                    +   JDBCNames.ExtensionFilesTable.ColumnName.UPDATED + "= ? " //$NON-NLS-1$
                                    + WHERE
                                    +   JDBCNames.ExtensionFilesTable.ColumnName.FILE_NAME + "= ? "; //$NON-NLS-1$





//*******************************
//
//   D E L E T E      S Q L
//
//*******************************

  public static final String DELETE_SOURCE
                                  = DELETE + FROM + JDBCNames.ExtensionFilesTable.TABLE_NAME
                                      + WHERE
                                      + JDBCNames.ExtensionFilesTable.ColumnName.FILE_NAME + "=?"; //$NON-NLS-1$



}

