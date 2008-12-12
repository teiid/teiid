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

package com.metamatrix.metadata.runtime.spi.jdbc;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import com.metamatrix.common.jdbc.JDBCReservedWords;
import com.metamatrix.common.vdb.api.ModelInfo;
import com.metamatrix.core.MetaMatrixRuntimeException;
import com.metamatrix.core.util.DateUtil;
import com.metamatrix.core.vdb.VDBStatus;
import com.metamatrix.metadata.runtime.RuntimeMetadataPlugin;
import com.metamatrix.metadata.runtime.api.MetaBaseInfo;
import com.metamatrix.metadata.runtime.api.MetadataID;
import com.metamatrix.metadata.runtime.api.Model;
import com.metamatrix.metadata.runtime.api.ModelID;
import com.metamatrix.metadata.runtime.api.VirtualDatabase;
import com.metamatrix.metadata.runtime.api.VirtualDatabaseID;
import com.metamatrix.metadata.runtime.model.BasicMetaBaseInfo;
import com.metamatrix.metadata.runtime.model.BasicModel;
import com.metamatrix.metadata.runtime.model.BasicModelID;
import com.metamatrix.metadata.runtime.model.BasicVirtualDatabase;
import com.metamatrix.metadata.runtime.model.BasicVirtualDatabaseID;
import com.metamatrix.metadata.util.ErrorMessageKeys;

public class JDBCTranslator {
    public static long NA_LONG = 0;
    public static int NA_INT = 0;
    public static short NA_SHORT = 0;
    public static String PLATFORM_DEPENDENT_MARK = "??";
    
    
    private static String IS_TRUE = "1";
    

//    private static final String DELIMITER = ".";
//    private static final String BLANK = MetadataConstants.BLANK;
    private static final String MAX = "MAX";
    private static final String INSERT      = JDBCReservedWords.INSERT + " ";
    private static final String UPDATE      = JDBCReservedWords.UPDATE + " ";
    private static final String DELETE      = JDBCReservedWords.DELETE + " ";
    private static final String SELECT      = JDBCReservedWords.SELECT + " ";
    private static final String FROM        = " " + JDBCReservedWords.FROM + " ";
    private static final String WHERE       = " " + JDBCReservedWords.WHERE + " ";
//    private static final String LIKE        = " " + JDBCReservedWords.LIKE + " ";
    private static final String ORDER_BY    = " " + JDBCReservedWords.ORDER_BY + " ";
    private static final String SET         = " " + JDBCReservedWords.SET + " ";
//    private static final String ON          = " " + JDBCReservedWords.ON + " ";
    private static final String INTO        = " " + JDBCReservedWords.INTO + " ";
    private static final String DISTINCT    = " " + JDBCReservedWords.DISTINCT + " ";
    private static final String VALUES      = " " + JDBCReservedWords.VALUES + " ";
    private static final String AND         = " " + JDBCReservedWords.AND + " ";
    private static final String IN          = " " + JDBCReservedWords.IN + " ";
    private static final String NOT_IN      = " " + JDBCReservedWords.NOT + " " + JDBCReservedWords.IN + " ";
    private static final String OR         = " " + JDBCReservedWords.OR + " ";
//    private static final String IS         = " " + JDBCReservedWords.IS + " ";
//    private static final String IS_NOT_NULL = IS + " " + JDBCReservedWords.NOT + " " + JDBCReservedWords.NULL;

//    private static final String UNIQUE_KEY_TYPE_ID  = "Unique";
//    private static final String FOREIGN_KEY_TYPE_ID = "Foreign";

    // ---------------------------------------------------------------------------------
    //                     S E L E C T    S T A T E  M E N T S
    // ---------------------------------------------------------------------------------
    //select IDs
    public static final String SELECT_ACTIVE_VIRTUAL_DATABASE_ID
                                    = SELECT + JDBCNames.VirtualDatabases.ColumnName.VDB_UID + ","
                                    	+ JDBCNames.VirtualDatabases.ColumnName.VDB_NM + ","
                                    	+ JDBCNames.VirtualDatabases.ColumnName.VDB_VERSION
                                        + FROM
                                        + JDBCNames.VirtualDatabases.TABLE_NAME
                                        + WHERE
                                        + PLATFORM_DEPENDENT_MARK + "=?"
                                        + AND
                                        + JDBCNames.VirtualDatabases.ColumnName.VDB_VERSION + "=?"
                                        + AND + "(" + JDBCNames.VirtualDatabases.ColumnName.VDB_STATUS + "=" + VDBStatus.ACTIVE 
                                        + OR + JDBCNames.VirtualDatabases.ColumnName.VDB_STATUS + "=" + VDBStatus.ACTIVE_DEFAULT
                                        + ")";

    public static final String SELECT_VIRTUAL_DATABASE_ID
                                    = SELECT + JDBCNames.VirtualDatabases.ColumnName.VDB_UID + ","
                                    	+ JDBCNames.VirtualDatabases.ColumnName.VDB_NM + ","
                                    	+ JDBCNames.VirtualDatabases.ColumnName.VDB_VERSION
                                        + FROM
                                        + JDBCNames.VirtualDatabases.TABLE_NAME
                                        + WHERE
                                        + PLATFORM_DEPENDENT_MARK + "=?"
                                        + AND
                                        + JDBCNames.VirtualDatabases.ColumnName.VDB_VERSION + "=?";

    public static final String SELECT_ACTIVE_VIRTUAL_DATABASE_ID_LV
                                    = SELECT + JDBCNames.VirtualDatabases.ColumnName.VDB_UID + ","
                                    	+ JDBCNames.VirtualDatabases.ColumnName.VDB_NM + ","
                                        + JDBCNames.VirtualDatabases.ColumnName.VDB_VERSION
                                        + FROM
                                        + JDBCNames.VirtualDatabases.TABLE_NAME
                                        + WHERE
                                        + PLATFORM_DEPENDENT_MARK + "=?"
                                        + AND + JDBCNames.VirtualDatabases.ColumnName.VERSION_DATE + "=("
                                        + SELECT + MAX + "("
                                        + JDBCNames.VirtualDatabases.ColumnName.VERSION_DATE +")"
                                        + FROM
                                        + JDBCNames.VirtualDatabases.TABLE_NAME
                                        + WHERE
                                        + PLATFORM_DEPENDENT_MARK + "=?"
                                        + AND
                                        + JDBCNames.VirtualDatabases.ColumnName.VDB_STATUS + "="
                                        + "(" + SELECT + MAX + "(" + JDBCNames.VirtualDatabases.ColumnName.VDB_STATUS + ")" 
                                        + FROM 
                                        + JDBCNames.VirtualDatabases.TABLE_NAME
                                        + WHERE
                                        + PLATFORM_DEPENDENT_MARK + "=?"
                                        + AND + "(" + JDBCNames.VirtualDatabases.ColumnName.VDB_STATUS + "=" + VDBStatus.ACTIVE 
                                        + OR + JDBCNames.VirtualDatabases.ColumnName.VDB_STATUS + "=" + VDBStatus.ACTIVE_DEFAULT
                                        +")))";

    public static final String SELECT_VIRTUAL_DATABASE_ID_LV
                                    = SELECT + JDBCNames.VirtualDatabases.ColumnName.VDB_UID + ","
                                    	+ JDBCNames.VirtualDatabases.ColumnName.VDB_NM + ","
                                        + JDBCNames.VirtualDatabases.ColumnName.VDB_VERSION
                                        + FROM
                                        + JDBCNames.VirtualDatabases.TABLE_NAME
                                        + WHERE
                                        + PLATFORM_DEPENDENT_MARK + "=?"
                                        + AND + JDBCNames.VirtualDatabases.ColumnName.VERSION_DATE + "=("
                                        + SELECT + MAX + "("
                                        + JDBCNames.VirtualDatabases.ColumnName.VERSION_DATE +")"
                                        + FROM
                                        + JDBCNames.VirtualDatabases.TABLE_NAME
                                        + WHERE
                                        + PLATFORM_DEPENDENT_MARK + "=?)";

    //select objects
    public static final String SELECT_VIRTUAL_DATABASES
                                    = SELECT + JDBCNames.VirtualDatabases.ColumnName.VDB_NM + ","
                                        + JDBCNames.VirtualDatabases.ColumnName.VDB_VERSION + ","
                                        + JDBCNames.VirtualDatabases.ColumnName.VDB_UID + ","
                                        + JDBCNames.VirtualDatabases.ColumnName.DESCRIPTION + ","
                                        + JDBCNames.VirtualDatabases.ColumnName.PROJECT_GUID +","
                                        + JDBCNames.VirtualDatabases.ColumnName.VDB_STATUS +","
                                        + JDBCNames.VirtualDatabases.ColumnName.WSDL_DEFINED +","
                                        + JDBCNames.VirtualDatabases.ColumnName.VERSION_BY +","
                                        + JDBCNames.VirtualDatabases.ColumnName.VERSION_DATE +","
                                        + JDBCNames.VirtualDatabases.ColumnName.CREATED_BY +","
                                        + JDBCNames.VirtualDatabases.ColumnName.CREATION_DATE+","
                                        + JDBCNames.VirtualDatabases.ColumnName.UPDATED_BY+","
                                        + JDBCNames.VirtualDatabases.ColumnName.UPDATED_DATE+","
                                        + JDBCNames.VirtualDatabases.ColumnName.VDB_FILE_NAME
                                        + FROM
                                        + JDBCNames.VirtualDatabases.TABLE_NAME;


	public static final String SELECT_SCHEMA_UIDS
                                    = SELECT + DISTINCT
                                    	+ JDBCNames.VDBModels.ColumnName.MDL_UID
                                    	+ FROM
                                    	+ JDBCNames.VDBModels.TABLE_NAME
                                    	+ WHERE
                                    	+ JDBCNames.VDBModels.ColumnName.VDB_UID + "=?";

    public static final String SELECT_METABASE_INFO
                                    = SELECT + JDBCNames.MetaBaseVersions.ColumnName.VERSION_DT + ","
                                        + JDBCNames.MetaBaseVersions.ColumnName.MB_UID + ","
                                        + JDBCNames.MetaBaseVersions.ColumnName.VERSION
                                        + FROM
                                        + JDBCNames.MetaBaseVersions.TABLE_NAME
                                        + WHERE
                                        + JDBCNames.MetaBaseVersions.ColumnName.VERSION_DT +"=("
                                        + SELECT + MAX + "("
                                        + JDBCNames.MetaBaseVersions.ColumnName.VERSION_DT + ")"
                                        + FROM
                                        + JDBCNames.MetaBaseVersions.TABLE_NAME + ")";

    public static final String SELECT_DELETED_VIRTUAL_DATABASES
                                    = SELECT + JDBCNames.VirtualDatabases.ColumnName.VDB_NM + ","
                                        + JDBCNames.VirtualDatabases.ColumnName.VDB_VERSION + ","
                                        + JDBCNames.VirtualDatabases.ColumnName.VDB_UID
                                        + FROM
                                        + JDBCNames.VirtualDatabases.TABLE_NAME
                                        + WHERE
                                        + JDBCNames.VirtualDatabases.ColumnName.VDB_STATUS + "=4";

    public static final String SELECT_VIRTUAL_DATABASE
                                    = SELECT + JDBCNames.VirtualDatabases.ColumnName.DESCRIPTION + ","
                                        + JDBCNames.VirtualDatabases.ColumnName.PROJECT_GUID +","
                                        + JDBCNames.VirtualDatabases.ColumnName.VDB_STATUS +","
                                        + JDBCNames.VirtualDatabases.ColumnName.WSDL_DEFINED +","
                                        + JDBCNames.VirtualDatabases.ColumnName.VERSION_BY +","
                                        + JDBCNames.VirtualDatabases.ColumnName.VERSION_DATE +","
                                        + JDBCNames.VirtualDatabases.ColumnName.CREATED_BY +","
                                        + JDBCNames.VirtualDatabases.ColumnName.CREATION_DATE+","
                                        + JDBCNames.VirtualDatabases.ColumnName.UPDATED_BY+","
                                        + JDBCNames.VirtualDatabases.ColumnName.UPDATED_DATE + ","
                                        + JDBCNames.VirtualDatabases.ColumnName.VDB_FILE_NAME
                                        + FROM
                                        + JDBCNames.VirtualDatabases.TABLE_NAME
                                        + WHERE
                                        + JDBCNames.VirtualDatabases.ColumnName.VDB_UID + "=?";

    public static final String SELECT_MODELS
                                    = SELECT + JDBCNames.VDBModels.TABLE_NAME + "."
                                        + JDBCNames.Models.ColumnName.MDL_UID +","
                                        + JDBCNames.Models.ColumnName.MDL_NM +","
                                        + JDBCNames.Models.ColumnName.MDL_VERSION +","
                                        + JDBCNames.Models.ColumnName.MDL_UUID +","
                                        + JDBCNames.Models.ColumnName.MDL_TYPE +","
                                        + JDBCNames.Models.ColumnName.MDL_URI +","
                                        + JDBCNames.Models.ColumnName.DESCRIPTION +","
                                        + JDBCNames.Models.ColumnName.IS_PHYSICAL +","
                                        + JDBCNames.Models.ColumnName.MULTI_SOURCED +","
                                        + JDBCNames.Models.ColumnName.VISIBILITY +","
                                        + JDBCNames.VDBModels.ColumnName.CNCTR_BNDNG_NM 
                                        + FROM + JDBCNames.Models.TABLE_NAME + ","
                                        + JDBCNames.VDBModels.TABLE_NAME
                                        + WHERE
                                        + JDBCNames.VDBModels.TABLE_NAME + "."
                                        + JDBCNames.VDBModels.ColumnName.MDL_UID + "="
                                        + JDBCNames.Models.TABLE_NAME + "."
                                        + JDBCNames.Models.ColumnName.MDL_UID
                                        + AND
                                        + JDBCNames.VDBModels.ColumnName.VDB_UID + "=?"
                                        + ORDER_BY
                                        + JDBCNames.Models.ColumnName.MDL_NM
                                        ;

    public static final String SELECT_CB_FOR_RT_MODELS
                                    = SELECT + JDBCNames.Models.ColumnName.MDL_NM +","
                                        + JDBCNames.VDBModels.TABLE_NAME + "."
                                        + JDBCNames.Models.ColumnName.MDL_UID +","
                                        + JDBCNames.Models.ColumnName.MDL_VERSION +","
                                        + JDBCNames.VDBModels.ColumnName.CNCTR_BNDNG_NM
                                        + FROM + JDBCNames.Models.TABLE_NAME + ","
                                        + JDBCNames.VDBModels.TABLE_NAME
                                        + WHERE
                                        + JDBCNames.Models.TABLE_NAME + "."
                                        + JDBCNames.Models.ColumnName.MDL_UID +"="
                                        + JDBCNames.VDBModels.TABLE_NAME + "."
                                        + JDBCNames.VDBModels.ColumnName.MDL_UID + AND
                                        + JDBCNames.Models.ColumnName.MDL_NM +"=?";

//    public static final String SELECT_VISIBILITY_FOR_RT_MODELS
//                                    = SELECT + JDBCNames.Models.ColumnName.MDL_NM +","
//                                        + JDBCNames.VDBModels.TABLE_NAME + "."
//                                        + JDBCNames.Models.ColumnName.MDL_UID +","
//                                        + JDBCNames.Models.ColumnName.MDL_VERSION +","
//                                        + JDBCNames.VDBModels.ColumnName.VISIBILITY
//                                        + FROM + JDBCNames.Models.TABLE_NAME + ","
//                                        + JDBCNames.VDBModels.TABLE_NAME
//                                        + WHERE
//                                        + JDBCNames.Models.TABLE_NAME + "."
//                                        + JDBCNames.Models.ColumnName.MDL_UID +"="
//                                        + JDBCNames.VDBModels.TABLE_NAME + "."
//                                        + JDBCNames.VDBModels.ColumnName.MDL_UID + AND
//                                        + JDBCNames.Models.ColumnName.MDL_NM +"=?";

    public static final String SELECT_MODEL_IDS
                                    = SELECT + JDBCNames.Models.ColumnName.MDL_UID +","
                                        + JDBCNames.Models.ColumnName.MDL_VERSION +","
                                        + JDBCNames.Models.ColumnName.MDL_NM +","
                                        + JDBCNames.Models.ColumnName.MDL_UUID
                                        + FROM + JDBCNames.Models.TABLE_NAME;

    public static final String SELECT_MODEL_IDS_ONLY_IN_VDB
                                    = SELECT + JDBCNames.Models.TABLE_NAME + "."
                                        + JDBCNames.Models.ColumnName.MDL_UID +","
                                        + JDBCNames.Models.ColumnName.MDL_VERSION +","
                                        + JDBCNames.Models.ColumnName.MDL_NM +","
                                        + JDBCNames.Models.ColumnName.MDL_UUID
                                        + FROM + JDBCNames.Models.TABLE_NAME
                                        + WHERE
                                        + JDBCNames.Models.ColumnName.MDL_UID
                                        + NOT_IN +"("
                                        + SELECT + JDBCNames.Models.TABLE_NAME + "."
                                        + JDBCNames.Models.ColumnName.MDL_UID
                                        + FROM + JDBCNames.Models.TABLE_NAME + ","
                                        + JDBCNames.VDBModels.TABLE_NAME
                                        + WHERE
                                        + JDBCNames.VDBModels.TABLE_NAME + "."
                                        + JDBCNames.VDBModels.ColumnName.MDL_UID + "="
                                        + JDBCNames.Models.TABLE_NAME + "."
                                        + JDBCNames.Models.ColumnName.MDL_UID
                                        + AND
                                        + JDBCNames.VDBModels.ColumnName.VDB_UID + " !=?)";


 
    //a special one
    public static final String SELECT_PROPERTIES
                                    = SELECT + JDBCNames.VCommonProperties.ColumnName.PRP_NM + ","
                                        + JDBCNames.VCommonProperties.ColumnName.PRP_VL
                                        + FROM + "?"
                                        + WHERE
                                        + "="
                                        + AND
                                        + "?=?"
                                        + ORDER_BY
                                        + JDBCNames.VCommonProperties.ColumnName.PRP_NM + ","
                                        + JDBCNames.VCommonProperties.ColumnName.PART_ID;

    //insert statement
    public static final String INSERT_VIRTUAL_DATABASE
                                    = INSERT + INTO + JDBCNames.VirtualDatabases.TABLE_NAME + "("
                                        + JDBCNames.VirtualDatabases.ColumnName.VDB_UID + ","
                                        + JDBCNames.VirtualDatabases.ColumnName.VDB_VERSION + ","
                                        + JDBCNames.VirtualDatabases.ColumnName.VDB_NM + ","
                                        + JDBCNames.VirtualDatabases.ColumnName.DESCRIPTION + ","
                                        + JDBCNames.VirtualDatabases.ColumnName.PROJECT_GUID +","
                                        + JDBCNames.VirtualDatabases.ColumnName.VDB_STATUS +","
                                        + JDBCNames.VirtualDatabases.ColumnName.WSDL_DEFINED +","                                        
                                        + JDBCNames.VirtualDatabases.ColumnName.VERSION_BY +","
                                        + JDBCNames.VirtualDatabases.ColumnName.VERSION_DATE +","
                                        + JDBCNames.VirtualDatabases.ColumnName.CREATED_BY +","
                                        + JDBCNames.VirtualDatabases.ColumnName.CREATION_DATE+","
                                        + JDBCNames.VirtualDatabases.ColumnName.UPDATED_BY+","
                                        + JDBCNames.VirtualDatabases.ColumnName.UPDATED_DATE+","
                                        + JDBCNames.VirtualDatabases.ColumnName.VDB_FILE_NAME + ")"
                                        + VALUES
                                        + "(?,?,?,?,?,?,?,?,?,?,?,?,?,?)";

    public static final String INSERT_MODELS
                                    = INSERT + INTO + JDBCNames.Models.TABLE_NAME + "("
                                        + JDBCNames.Models.ColumnName.MDL_UID +","
                                        + JDBCNames.Models.ColumnName.MDL_NM +","
                                        + JDBCNames.Models.ColumnName.MDL_VERSION +","
                                        + JDBCNames.Models.ColumnName.DESCRIPTION +","
                                        + JDBCNames.Models.ColumnName.IS_PHYSICAL +","
                                        + JDBCNames.Models.ColumnName.MULTI_SOURCED +","
                                        + JDBCNames.Models.ColumnName.VISIBILITY +","
                                        + JDBCNames.Models.ColumnName.MDL_UUID +","
                                        + JDBCNames.Models.ColumnName.MDL_TYPE +","
                                        + JDBCNames.Models.ColumnName.MDL_URI + ")"
                                        + VALUES
                                        + "(?,?,?,?,?,?,?,?,?,?)";


    public static final String INSERT_VDB_MODELS
                                    = INSERT + INTO + JDBCNames.VDBModels.TABLE_NAME + "("
                                        + JDBCNames.VDBModels.ColumnName.VDB_UID + ","
                                        + JDBCNames.VDBModels.ColumnName.MDL_UID + ")"
                                        + VALUES
                                        + "(?,?)";
    
    public static final String INSERT_VDB_MODELS_WITH_BINDING
                                        = INSERT + INTO + JDBCNames.VDBModels.TABLE_NAME + "("
                                            + JDBCNames.VDBModels.ColumnName.VDB_UID + ","
                                            + JDBCNames.VDBModels.ColumnName.MDL_UID + ","
                                            + JDBCNames.VDBModels.ColumnName.CNCTR_BNDNG_NM + ")"
                                            + VALUES
                                            + "(?,?,?)";
                                        

    public static final String INSERT_PROP_NAMES
                                    = INSERT + INTO + "?" + "("
                                        + JDBCNames.VCommonProperties.ColumnName.PRP_UID + ","
                                        + "?," + JDBCNames.VCommonProperties.ColumnName.PRP_NM + ")"
                                        + VALUES
                                        + "(?,?,?)";

    public static final String INSERT_PROP_VALUES
                                    = INSERT + INTO + "?" + "("
                                        + JDBCNames.VCommonProperties.ColumnName.PRP_UID + ","
                                        + JDBCNames.VCommonProperties.ColumnName.PART_ID + ","
                                        + JDBCNames.VCommonProperties.ColumnName.PRP_VL + ")"
                                        + VALUES
                                        + "(?,?,?)";

    //update statement
    public static final String UPDATE_SET_STATUS
                                    = UPDATE + JDBCNames.VirtualDatabases.TABLE_NAME
                                        + SET
                                        + JDBCNames.VirtualDatabases.ColumnName.VDB_STATUS + "=?,"
                                        + JDBCNames.VirtualDatabases.ColumnName.UPDATED_BY + "=?,"
                                        + JDBCNames.VirtualDatabases.ColumnName.UPDATED_DATE + "=?"
                                        + WHERE
                                        + JDBCNames.VirtualDatabases.ColumnName.VDB_UID + "=?";

    public static final String UPDATE_CONNECTOR_BINGING_NAME
                                    = UPDATE + JDBCNames.VDBModels.TABLE_NAME
                                        + SET
                                        + JDBCNames.VDBModels.ColumnName.CNCTR_BNDNG_NM + "=?"
                                        + WHERE
                                        + JDBCNames.VDBModels.ColumnName.VDB_UID + "=?"
                                        + AND
                                        + JDBCNames.VDBModels.ColumnName.MDL_UID + "=?";

//    public static final String UPDATE_VISIBILITY_LEVELS
//                                    = UPDATE + JDBCNames.VDBModels.TABLE_NAME
//                                        + SET
//                                        + JDBCNames.VDBModels.ColumnName.VISIBILITY + "=?"
//                                        + WHERE
//                                        + JDBCNames.VDBModels.ColumnName.VDB_UID + "=?"
//                                        + AND
//                                        + JDBCNames.VDBModels.ColumnName.MDL_UID + "=?";

    public static final String UPDATE_VDB
                                    = UPDATE + JDBCNames.VirtualDatabases.TABLE_NAME
                                        + SET
                                        + JDBCNames.VirtualDatabases.ColumnName.UPDATED_BY + "=?,"
                                        + JDBCNames.VirtualDatabases.ColumnName.UPDATED_DATE + "=?"
                                        + WHERE
                                        + JDBCNames.VirtualDatabases.ColumnName.VDB_UID + "=?";

    public static final String UPDATE_VDB_VERSION
                                    = UPDATE + JDBCNames.VirtualDatabases.TABLE_NAME
                                        + SET
                                        + JDBCNames.VirtualDatabases.ColumnName.VDB_VERSION + "=?"
                                        + WHERE
                                        + JDBCNames.VirtualDatabases.ColumnName.VDB_UID + "=?";

    public static final String UPDATE_VDB_MODELS
                                    = UPDATE + JDBCNames.VDBModels.TABLE_NAME
                                        + SET
                                        + JDBCNames.VDBModels.ColumnName.MDL_UID + "=?"
                                        + WHERE
                                        + JDBCNames.VDBModels.ColumnName.VDB_UID + "=?"
                                        + AND
                                        + JDBCNames.VDBModels.ColumnName.MDL_UID + "=?";

    public static final String UPDATE_SYS_MODEL
                                    = UPDATE + JDBCNames.VDBModels.TABLE_NAME
                                        + SET
                                        + JDBCNames.VDBModels.ColumnName.MDL_UID + "=?,"
                                        + JDBCNames.VDBModels.ColumnName.CNCTR_BNDNG_NM + "=?,"
                       //                 + JDBCNames.VDBModels.ColumnName.VISIBILITY + "=?"
                                        + WHERE
                                        + JDBCNames.VDBModels.ColumnName.MDL_UID + "=?";

    public static final String UPDATE_MODEL_NAME
                                    = UPDATE + JDBCNames.Models.TABLE_NAME
                                        + SET
                                        + JDBCNames.Models.ColumnName.MDL_NM + "=?"
                                        + WHERE
                                        + JDBCNames.Models.ColumnName.MDL_UID + "=?";



    public static final String DELETE_MODEL_PROP_VALS
                                    = DELETE + FROM + JDBCNames.ModelPropValues.TABLE_NAME
                                        + WHERE
                                        + JDBCNames.ModelPropValues.ColumnName.PRP_UID
                                        + IN + "("
                                        + SELECT
                                        + JDBCNames.ModelPropValues.ColumnName.PRP_UID
                                        + FROM
                                        + JDBCNames.ModelPropNames.TABLE_NAME
                                        + WHERE
                                        + JDBCNames.ModelPropNames.ColumnName.MDL_UID +"=?)";

    public static final String DELETE_MODEL_PROP_NMS
                                    = DELETE + FROM + JDBCNames.ModelPropNames.TABLE_NAME
                                        + WHERE
                                        + JDBCNames.ModelPropNames.ColumnName.MDL_UID + "=?";

    public static final String DELETE_VDB_MODELS
                                    = DELETE + FROM + JDBCNames.VDBModels.TABLE_NAME
                                        + WHERE
                                        + JDBCNames.VDBModels.ColumnName.VDB_UID + "=?";

    public static final String DELETE_VDB_MODEL
                                    = DELETE + FROM + JDBCNames.VDBModels.TABLE_NAME
                                        + WHERE
                                        + JDBCNames.VDBModels.ColumnName.VDB_UID + "=?"
                                        + AND
                                        + JDBCNames.VDBModels.ColumnName.MDL_UID + "=?";

    public static final String DELETE_MODEL
                                    = DELETE + FROM + JDBCNames.Models.TABLE_NAME
                                        + WHERE
                                        + JDBCNames.Models.ColumnName.MDL_UID + "=?";


    public static final String DELETE_VDB
                                    = DELETE + FROM + JDBCNames.VirtualDatabases.TABLE_NAME
                                        + WHERE
                                        + JDBCNames.VirtualDatabases.ColumnName.VDB_UID + "=?";




    public static int getCount(ResultSet resultSet) throws SQLException {
        if(resultSet.next()){
            return resultSet.getInt(1);
        }
        return 0;
    }

    public static Collection getVirtualDatabases(ResultSet resultSet) throws SQLException  {
        Collection result = new HashSet();
        try{
            while(resultSet.next()){
                String name = resultSet.getString(JDBCNames.VirtualDatabases.ColumnName.VDB_NM);
                String version = resultSet.getString(JDBCNames.VirtualDatabases.ColumnName.VDB_VERSION);
                long uid = resultSet.getLong(JDBCNames.VirtualDatabases.ColumnName.VDB_UID);
                BasicVirtualDatabaseID vdbID = new BasicVirtualDatabaseID(name, version, uid);
                BasicVirtualDatabase vdb = new BasicVirtualDatabase(vdbID);
                vdb.setDescription(resultSet.getString(JDBCNames.VirtualDatabases.ColumnName.DESCRIPTION));
                vdb.setGUID(resultSet.getString(JDBCNames.VirtualDatabases.ColumnName.PROJECT_GUID));
                vdb.setStatus(resultSet.getShort(JDBCNames.VirtualDatabases.ColumnName.VDB_STATUS));

                String v = resultSet.getString(JDBCNames.VirtualDatabases.ColumnName.WSDL_DEFINED);

                vdb.setHasWSDLDefined((v!=null && v.equals(IS_TRUE)? Boolean.TRUE.booleanValue(): Boolean.FALSE.booleanValue()) );
                
                vdb.setVersionBy(resultSet.getString(JDBCNames.VirtualDatabases.ColumnName.VERSION_BY));
                vdb.setVersionDate(DateUtil.convertStringToDate(resultSet.getString(JDBCNames.VirtualDatabases.ColumnName.VERSION_DATE)));
                vdb.setCreatedBy(resultSet.getString(JDBCNames.VirtualDatabases.ColumnName.CREATED_BY));
                vdb.setCreationDate(DateUtil.convertStringToDate(resultSet.getString(JDBCNames.VirtualDatabases.ColumnName.CREATION_DATE)));
                vdb.setUpdatedBy(resultSet.getString(JDBCNames.VirtualDatabases.ColumnName.UPDATED_BY));
                vdb.setUpdateDate(DateUtil.convertStringToDate(resultSet.getString(JDBCNames.VirtualDatabases.ColumnName.UPDATED_DATE)));
                vdb.setFileName(resultSet.getString(JDBCNames.VirtualDatabases.ColumnName.VDB_FILE_NAME));
                result.add(vdb);
            }
        }catch(ParseException pe){
            throw new MetaMatrixRuntimeException(ErrorMessageKeys.JDBCT_0001, RuntimeMetadataPlugin.Util.getString(ErrorMessageKeys.JDBCT_0001) );
        }
        return result;
    }

    public static Collection getVirtualDatabaseIDs(ResultSet resultSet) throws SQLException  {
        Collection result = new HashSet();
        while(resultSet.next()){
            String name = resultSet.getString(JDBCNames.VirtualDatabases.ColumnName.VDB_NM);
            String version = resultSet.getString(JDBCNames.VirtualDatabases.ColumnName.VDB_VERSION);
            long uid = resultSet.getLong(JDBCNames.VirtualDatabases.ColumnName.VDB_UID);
            BasicVirtualDatabaseID vdbID = new BasicVirtualDatabaseID(name, version, uid);

            result.add(vdbID);
        }
        return result;
    }

    public static VirtualDatabase getVirtualDatabase(ResultSet resultSet, VirtualDatabaseID vdbID) throws SQLException  {
        BasicVirtualDatabase vdb = new BasicVirtualDatabase((BasicVirtualDatabaseID)vdbID);
        try{
            vdb.setDescription(resultSet.getString(JDBCNames.VirtualDatabases.ColumnName.DESCRIPTION));
            vdb.setGUID(resultSet.getString(JDBCNames.VirtualDatabases.ColumnName.PROJECT_GUID));
            vdb.setStatus(resultSet.getShort(JDBCNames.VirtualDatabases.ColumnName.VDB_STATUS));

            String v = resultSet.getString(JDBCNames.VirtualDatabases.ColumnName.WSDL_DEFINED);

            vdb.setHasWSDLDefined((v!=null && v.equals(IS_TRUE)? Boolean.TRUE.booleanValue(): Boolean.FALSE.booleanValue()) );
            
            vdb.setVersionBy(resultSet.getString(JDBCNames.VirtualDatabases.ColumnName.VERSION_BY));
            vdb.setVersionDate(DateUtil.convertStringToDate(resultSet.getString(JDBCNames.VirtualDatabases.ColumnName.VERSION_DATE)));
            vdb.setCreatedBy(resultSet.getString(JDBCNames.VirtualDatabases.ColumnName.CREATED_BY));
            vdb.setCreationDate(DateUtil.convertStringToDate(resultSet.getString(JDBCNames.VirtualDatabases.ColumnName.CREATION_DATE)));
            vdb.setUpdatedBy(resultSet.getString(JDBCNames.VirtualDatabases.ColumnName.UPDATED_BY));
            vdb.setUpdateDate(DateUtil.convertStringToDate(resultSet.getString(JDBCNames.VirtualDatabases.ColumnName.UPDATED_DATE)));
            vdb.setFileName(resultSet.getString(JDBCNames.VirtualDatabases.ColumnName.VDB_FILE_NAME));
        }catch(ParseException pe){
            throw new MetaMatrixRuntimeException(ErrorMessageKeys.JDBCT_0001, RuntimeMetadataPlugin.Util.getString(ErrorMessageKeys.JDBCT_0001) );
        }
        return vdb;
    }

    public static VirtualDatabaseID getVirtualDatabaseID(ResultSet resultSet) throws SQLException  {
        long uid = resultSet.getLong(JDBCNames.VirtualDatabases.ColumnName.VDB_UID);
        String name = resultSet.getString(JDBCNames.VirtualDatabases.ColumnName.VDB_NM);
        String version = resultSet.getString(JDBCNames.VirtualDatabases.ColumnName.VDB_VERSION);
        return new BasicVirtualDatabaseID(name, version, uid);
    }

    public static Collection getModels(ResultSet resultSet, VirtualDatabaseID vdbID, Collection smUids) throws SQLException  {
        Collection result = new HashSet();
        String currentModelName = null;
        BasicModelID modelID = null;
        BasicModel model = null;
        
        // there can be multiple rows for the same model because of the table join
        // between the RT_MDLS and RT_VDB_MDLS tables.  Because a model can 
        // support multisource bindings, therefore a model may have multiple
        // binding rows.
        while(resultSet.next()){
            String name = resultSet.getString(JDBCNames.Models.ColumnName.MDL_NM);
            if (currentModelName ==null || !currentModelName.equals(name)) {
                String version = resultSet.getString(JDBCNames.Models.ColumnName.MDL_VERSION);
                long uid = resultSet.getLong(JDBCNames.Models.ColumnName.MDL_UID);
                modelID = new BasicModelID(name, version, uid);

                modelID.setUuid(resultSet.getString(JDBCNames.Models.ColumnName.MDL_UUID));
                model = new BasicModel(modelID, (BasicVirtualDatabaseID)vdbID);
                // set physical is controlled by the model type
//                model.seti(resultSet.getBoolean(JDBCNames.Models.ColumnName.IS_PHYSICAL));
                model.setDescription(resultSet.getString(JDBCNames.Models.ColumnName.DESCRIPTION));
                model.setModelType(resultSet.getInt(JDBCNames.Models.ColumnName.MDL_TYPE));
                model.setModelURI(resultSet.getString(JDBCNames.Models.ColumnName.MDL_URI));
                
               
                String v = resultSet.getString(JDBCNames.Models.ColumnName.MULTI_SOURCED);
                model.enableMutliSourceBindings((v!=null && v.equals(IS_TRUE)? Boolean.TRUE.booleanValue(): Boolean.FALSE.booleanValue()) );                
                
                if(resultSet.getObject(JDBCNames.Models.ColumnName.VISIBILITY) == null){
                    model.setIsVisible(true);
                }else{
                    short vis = resultSet.getShort(JDBCNames.Models.ColumnName.VISIBILITY);
                    if (vis == ModelInfo.PUBLIC) {
                        model.setIsVisible(true);
                    } else {
                        model.setIsVisible(false);
                    }
                }           
                
                result.add(model);
               
                currentModelName = name;
            }
            
            String cbName = resultSet.getString(JDBCNames.VDBModels.ColumnName.CNCTR_BNDNG_NM);
            if (cbName != null && cbName.trim().length() > 0) {
                model.addConnectorBindingName(cbName);
            }

        }
        return result;
    }

    public static Map getCBNamesForRTModels(ResultSet resultSet) throws SQLException{
        Map result = new HashMap();
        while(resultSet.next()){
            String name = resultSet.getString(JDBCNames.Models.ColumnName.MDL_NM);
            String version = resultSet.getString(JDBCNames.Models.ColumnName.MDL_VERSION);
            long uid = resultSet.getLong(JDBCNames.Models.ColumnName.MDL_UID);
            BasicModelID modelID = new BasicModelID(name, version, uid);
            String cbName = resultSet.getString(JDBCNames.VDBModels.ColumnName.CNCTR_BNDNG_NM);
            if(cbName != null){
                //only one physical runtime model. Version is not checked. Assumed there is only one version.
                result.put(modelID, cbName);
                break;
            }
        }
        return result;
    }

//    public static Map getVNamesForRTModels(ResultSet resultSet) throws SQLException{
//        Map result = new HashMap();
//        while(resultSet.next()){
//            String name = resultSet.getString(JDBCNames.Models.ColumnName.MDL_NM);
//            String version = resultSet.getString(JDBCNames.Models.ColumnName.MDL_VERSION);
//            long uid = resultSet.getLong(JDBCNames.Models.ColumnName.MDL_UID);
//            BasicModelID modelID = new BasicModelID(name, version, uid);
//            Object obj = resultSet.getObject(JDBCNames.VDBModels.ColumnName.VISIBILITY);
//            if(obj != null){
//                result.put(modelID, new Short(resultSet.getShort(JDBCNames.VDBModels.ColumnName.VISIBILITY)));
//                break;
//            }
//        }
//        return result;
//    }

     public static List getModelIDs(ResultSet resultSet) throws SQLException  {
        List result = new ArrayList();
        while(resultSet.next()){
            String name = resultSet.getString(JDBCNames.Models.ColumnName.MDL_NM);
            String version = resultSet.getString(JDBCNames.Models.ColumnName.MDL_VERSION);
            long uid = resultSet.getLong(JDBCNames.Models.ColumnName.MDL_UID);
            BasicModelID modelID = new BasicModelID(name, version, uid);
            modelID.setUuid(resultSet.getString(JDBCNames.Models.ColumnName.MDL_UUID));
            result.add(modelID);
        }
        return result;
    }

    public static Model getModel(ResultSet resultSet) throws SQLException  {
        return null;
    }
//    public static Collection getGroups(ResultSet resultSet) throws SQLException  {
//        return null;
//    }

//    public static Group getGroup(ResultSet resultSet, BasicGroup group) throws SQLException  {
//        //path column is changed to fully qualified name
//        String path = group.getID().getFullName();
//        boolean gotObject = false;
//        do{
//            String cPath = resultSet.getString(JDBCNames.Groups.ColumnName.PATH);
//            if(!path.equalsIgnoreCase(cPath))
//                continue;
//
//            gotObject = true;
//            //group.setPath(path);
//            if(path.equals(cPath)){
//                ((BasicGroupID)group.getID()).setUID(resultSet.getLong(JDBCNames.Groups.ColumnName.GRP_UID));
//            }else{
//                BasicGroupID gID = new BasicGroupID(cPath, resultSet.getLong(JDBCNames.Groups.ColumnName.GRP_UID));
//                gID.setModelID(((GroupID)group.getID()).getModelID());
//                group = new BasicGroup(gID, (BasicVirtualDatabaseID)group.getVirtualDatabaseID());
//            }
//            group.setDescription(resultSet.getString(JDBCNames.Groups.ColumnName.DESCRIPTION));
//            group.setAlias(resultSet.getString(JDBCNames.Groups.ColumnName.ALIAS));
//            short tType = resultSet.getShort(JDBCNames.Groups.ColumnName.TABLE_TYPE);
//            if(tType != NA_SHORT)
//                group.setTableType(tType);
//            else
//                group.setTableType(MetadataConstants.NOT_DEFINED_SHORT);
//            group.setIsPhysical(resultSet.getBoolean(JDBCNames.Groups.ColumnName.IS_PHYSICAL));
//            group.setHasQueryPlan(resultSet.getBoolean(JDBCNames.Groups.ColumnName.HAS_QUERY_PLAN));
//            if(resultSet.getObject(JDBCNames.Groups.ColumnName.SUP_UPDATE) == null){
//                if(group.isPhysical())
//                    group.setSupportsUpdate(true);
//            }else
//                group.setSupportsUpdate(resultSet.getBoolean(JDBCNames.Groups.ColumnName.SUP_UPDATE));
//            break;
//        }while(resultSet.next());
//        if(gotObject)
//            return group;
//        return null;
//    }

//    public static Collection getKeysInModel(ResultSet resultSet, VirtualDatabaseID vdbID, String modelName) throws SQLException  {
//        Collection result = new HashSet();
//        while(resultSet.next()){
//            String cPath = resultSet.getString(JDBCNames.Groups.ColumnName.PATH);
//            if(cPath == null)
//                cPath = BLANK;
//            long uid = resultSet.getLong(JDBCNames.KeyIndex.ColumnName.KEY_UID);
//            String pName = null;
//            if(!cPath.equals(BLANK)) {
////                pName = modelName + DELIMITER + cPath + DELIMITER + resultSet.getString(JDBCNames.KeyIndex.ColumnName.KEY_NM);
//                pName = cPath + DELIMITER + resultSet.getString(JDBCNames.KeyIndex.ColumnName.KEY_NM);
//            } else {
////                pName = modelName + DELIMITER + resultSet.getString(JDBCNames.KeyIndex.ColumnName.KEY_NM);
//                pName = resultSet.getString(JDBCNames.KeyIndex.ColumnName.KEY_NM);
//            }
//            BasicKeyID keyID = new BasicKeyID(pName, uid);
//            BasicKey key = new BasicKey(keyID, (BasicVirtualDatabaseID)vdbID);
//            key.setDescription(resultSet.getString(JDBCNames.KeyIndex.ColumnName.DESCRIPTION));
//            key.setAlias(resultSet.getString(JDBCNames.KeyIndex.ColumnName.ALIAS));
//            key.setKeyType(resultSet.getShort(JDBCNames.KeyIndex.ColumnName.KEY_TYPE));
//            key.setIsIndexed(resultSet.getBoolean(JDBCNames.KeyIndex.ColumnName.IS_INDEXED));
//            key.setReferencedKeyUID(resultSet.getLong(JDBCNames.KeyIndex.ColumnName.REF_KEY_UID));
//            short mType = resultSet.getShort(JDBCNames.KeyIndex.ColumnName.MATCH_TYPE);
//            if(mType != NA_SHORT)
//                key.setMatchType(mType);
//            else
//                key.setMatchType(MetadataConstants.NOT_DEFINED_SHORT);
//            result.add(key);
//        }
//        return result;
//    }

//    public static Collection getKeysInGroup(ResultSet resultSet, VirtualDatabaseID vdbID, GroupID groupID) throws SQLException  {
//        Collection result = new HashSet();
//        while(resultSet.next()){
//            long uid = resultSet.getLong(JDBCNames.KeyIndex.ColumnName.KEY_UID);
//            String kName = groupID.getFullName() + DELIMITER + resultSet.getString(JDBCNames.KeyIndex.ColumnName.KEY_NM);
//            BasicKeyID keyID = new BasicKeyID(kName, uid);
//            BasicKey key = new BasicKey(keyID, (BasicVirtualDatabaseID)vdbID);
//            key.setDescription(resultSet.getString(JDBCNames.KeyIndex.ColumnName.DESCRIPTION));
//            key.setAlias(resultSet.getString(JDBCNames.KeyIndex.ColumnName.ALIAS));
//            key.setKeyType(resultSet.getShort(JDBCNames.KeyIndex.ColumnName.KEY_TYPE));
//            key.setIsIndexed(resultSet.getBoolean(JDBCNames.KeyIndex.ColumnName.IS_INDEXED));
//            key.setReferencedKeyUID(resultSet.getLong(JDBCNames.KeyIndex.ColumnName.REF_KEY_UID));
//            short mType = resultSet.getShort(JDBCNames.KeyIndex.ColumnName.MATCH_TYPE);
//            if(mType != NA_SHORT)
//                key.setMatchType(mType);
//            else
//                key.setMatchType(MetadataConstants.NOT_DEFINED_SHORT);
//            result.add(key);
//        }
//        return result;
//    }

//    public static Key getKey(ResultSet resultSet, BasicKey key) throws SQLException  {
//        String path = ((KeyID)key.getID()).getGroupID().getFullName();
//        boolean gotObject = false;
//
//        do{
//            String cPath = resultSet.getString(JDBCNames.Groups.ColumnName.PATH);
//            if(!path.equals(cPath))
//                continue;
//
//            gotObject = true;
//            //key.setPath(path);
//            ((BasicKeyID)key.getID()).setUID(resultSet.getLong(JDBCNames.KeyIndex.ColumnName.KEY_UID));
//            key.setDescription(resultSet.getString(JDBCNames.KeyIndex.ColumnName.DESCRIPTION));
//            key.setAlias(resultSet.getString(JDBCNames.KeyIndex.ColumnName.ALIAS));
//            key.setKeyType(resultSet.getShort(JDBCNames.KeyIndex.ColumnName.KEY_TYPE));
//            key.setIsIndexed(resultSet.getBoolean(JDBCNames.KeyIndex.ColumnName.IS_INDEXED));
//            key.setReferencedKeyUID(resultSet.getLong(JDBCNames.KeyIndex.ColumnName.REF_KEY_UID));
//            short mType = resultSet.getShort(JDBCNames.KeyIndex.ColumnName.MATCH_TYPE);
//            if(mType != NA_SHORT)
//                key.setMatchType(mType);
//            else
//                key.setMatchType(MetadataConstants.NOT_DEFINED_SHORT);
//            break;
//        }while(resultSet.next());
//
//        if(gotObject)
//            return key;
//        return null;
//    }

//    public static KeyID getKeyID(ResultSet resultSet) throws SQLException  {
//        //String cPath = resultSet.getString(JDBCNames.Groups.ColumnName.PATH);
//        //if(cPath == null)
//        //    cPath = BLANK;
//        String name = resultSet.getString(JDBCNames.KeyIndex.ColumnName.KEY_NM);
//        String groupFullName = resultSet.getString(JDBCNames.Groups.ColumnName.PATH);
//        long uid = resultSet.getLong(JDBCNames.KeyIndex.ColumnName.KEY_UID);
//        resultSet.getLong(JDBCNames.Groups.ColumnName.MDL_UID);
//        //Iterator iter = models.iterator();
//        //BasicModelID mID = null;
//        //while(iter.hasNext()){
//        //    mID = (BasicModelID)((Model)iter.next()).getID();
//        //    if(modelUid == mID.getUID())
//        //        break;
//        //}
//        //String modelName = mID.getName();
//        //String fKeyName = null;
//        //if(!cPath.equals(BLANK))
//        String fKeyName = groupFullName + DELIMITER + name;
//        //else
//        //    fKeyName = modelName+ DELIMITER + groupName + DELIMITER + name;
//        return new BasicKeyID(fKeyName, uid);
//    }

//    public static Collection getGroupIDsInModel(ResultSet resultSet, ModelID modelID) throws SQLException{
//        Collection result = new HashSet();
//        while(resultSet.next()){
//            long uid = resultSet.getLong(JDBCNames.Groups.ColumnName.GRP_UID);
//            String groupFullName = resultSet.getString(JDBCNames.Groups.ColumnName.PATH);
//            BasicGroupID groupID = new BasicGroupID(groupFullName, uid);
//            groupID.setModelID(modelID);
//            result.add(groupID);
//        }
//        return result;
//    }

//    public static Collection getGroupNames(ResultSet resultSet) throws SQLException{
//        Collection result = new HashSet();
//        while(resultSet.next()){
//            final String groupFullName = resultSet.getString(JDBCNames.Groups.ColumnName.PATH);
//            result.add(groupFullName);
//        }
//        return result;
//    }

//    public static List getElements(ResultSet resultSet, GroupID groupID, VirtualDatabaseID vdbID, Collection dataTypes) throws SQLException {
//        List result = new ArrayList();
//        while(resultSet.next()){
//            long uid = resultSet.getLong(JDBCNames.Elements.ColumnName.ELMNT_UID);
//            String eName = resultSet.getString(JDBCNames.Elements.ColumnName.ELMNT_NM);
//            BasicElementID elementID = new BasicElementID(groupID.getFullName(), eName, uid);
//            BasicElement element = new BasicElement(elementID, (BasicVirtualDatabaseID)vdbID);
//            element.setDescription(resultSet.getString(JDBCNames.Elements.ColumnName.DESCRIPTION));
//            element.setAlias(resultSet.getString(JDBCNames.Elements.ColumnName.ALIAS));
//            element.setLabel(resultSet.getString(JDBCNames.Elements.ColumnName.LABEL));
//            long dtUid = resultSet.getLong(JDBCNames.Elements.ColumnName.DT_UID);
//            Iterator iter = dataTypes.iterator();
//            DataType dt = null;
//            while(iter.hasNext()){
//                DataType thisDT = (DataType)iter.next();
//                 if(dtUid == ((BasicDataTypeID)thisDT.getID()).getUID()){
//                    dt = thisDT;
//                    break;
//                 }
//            }
//            if(dt == null){
//            	I18nLogManager.logError(LogRuntimeMetadataConstants.CTX_RUNTIME_METADATA, ErrorMessageKeys.JDBCT_0003, new Object[]{new Long(dtUid), elementID.getFullName()} );
//            }
//            element.setDataType(dt);
//            element.setScale(resultSet.getInt(JDBCNames.Elements.ColumnName.SCALE));
//            element.setLength(resultSet.getInt(JDBCNames.Elements.ColumnName.LENGTH));
//            int tmp = resultSet.getInt(JDBCNames.Elements.ColumnName.PRCSN_LNGTH);
//            if (tmp != NA_INT)
//                element.setPrecisionLength(tmp);
//            else
//                element.setPrecisionLength(MetadataConstants.NOT_DEFINED_INT);
//            tmp = resultSet.getInt(JDBCNames.Elements.ColumnName.RADIX);
//            if (tmp != NA_INT)
//                element.setRadix(tmp);
//            else
//                element.setRadix(MetadataConstants.NOT_DEFINED_INT);
//            tmp = resultSet.getInt(JDBCNames.Elements.ColumnName.CHR_OCTCT_LNGTH);
//            if (tmp != NA_INT)
//                element.setCharOctetLength(tmp);
//            else
//                element.setCharOctetLength(MetadataConstants.NOT_DEFINED_INT);
//            element.setPhysical(resultSet.getBoolean(JDBCNames.Elements.ColumnName.IS_PHYSICAL));
//            element.setIsLengthFixed(resultSet.getBoolean(JDBCNames.Elements.ColumnName.IS_LENGTH_FIXED));
//            element.setNullType(resultSet.getShort(JDBCNames.Elements.ColumnName.NULL_TYPE));
//            element.setSupportsSelect(resultSet.getBoolean(JDBCNames.Elements.ColumnName.SUP_SELECT));
//            element.setSupportsSubscription(resultSet.getBoolean(JDBCNames.Elements.ColumnName.SUP_SUBSCRIPTION));
//            element.setSupportsUpdate(resultSet.getBoolean(JDBCNames.Elements.ColumnName.SUP_UPDATE));
//            element.setIsCaseSensitive(resultSet.getBoolean(JDBCNames.Elements.ColumnName.IS_CASE_SENSITIVE));
//            element.setIsSigned(resultSet.getBoolean(JDBCNames.Elements.ColumnName.IS_SIGNED));
//            element.setIsCurrency(resultSet.getBoolean(JDBCNames.Elements.ColumnName.IS_CURRENCY));
//            element.setIsAutoIncrement(resultSet.getBoolean(JDBCNames.Elements.ColumnName.IS_AUTOINCREMENT));
//            element.setMinimumRange(resultSet.getString(JDBCNames.Elements.ColumnName.MIN_RANGE));
//            element.setMaximumRange(resultSet.getString(JDBCNames.Elements.ColumnName.MAX_RANGE));
//            element.setSearchType(resultSet.getShort(JDBCNames.Elements.ColumnName.SEARCH_TYPE));
//            element.setFormat(resultSet.getString(JDBCNames.Elements.ColumnName.FORMAT));
//            String mp = resultSet.getString(JDBCNames.Elements.ColumnName.MULTIPLICITY);
//            try{
//                element.setMultplicity(Multiplicity.getInstance(mp));
//            }catch(MultiplicityExpressionException mee){
//                I18nLogManager.logError(LogRuntimeMetadataConstants.CTX_RUNTIME_METADATA, ErrorMessageKeys.JDBCT_0002, mee, new Object[]{mp});
//                throw new MetaMatrixRuntimeException(ErrorMessageKeys.JDBCT_0002, RuntimeMetadataPlugin.Util.getString(ErrorMessageKeys.JDBCT_0002, mp) );
//            }
//            element.setDefaultValue(resultSet.getString(JDBCNames.Elements.ColumnName.DEFAULT_VL));
//            result.add(element);
//        }
//        return result;
//    }
//
//    public static List getElementIDsInKey(ResultSet resultSet, List elementIDs) throws SQLException  {
//        List result = new ArrayList();
//
//        while(resultSet.next()){
//            long uid = resultSet.getLong(JDBCNames.KeyIndexElements.ColumnName.ELMNT_UID);
//            Iterator iter = elementIDs.iterator();
//            BasicElementID eID = null;
//            while(iter.hasNext()){
//                 eID = (BasicElementID)iter.next();
//                 if(uid == eID.getUID())
//                    break;
//            }
//            result.add(eID);
//        }
//        return result;
//    }
//
//    public static Element getElement(ResultSet resultSet) throws SQLException  {
//        return null;
//    }
//    public static Collection getProcedures(ResultSet resultSet, VirtualDatabaseID vdbID, String modelName) throws SQLException  {
//        Collection result = new HashSet();
//        while(resultSet.next()){
//            String pName = resultSet.getString(JDBCNames.Procedures.ColumnName.PATH);
//            long uid = resultSet.getLong(JDBCNames.Procedures.ColumnName.PROC_UID);
//            BasicProcedureID procID = new BasicProcedureID(pName, uid);
//            BasicProcedure procedure = new BasicProcedure(procID, (BasicVirtualDatabaseID)vdbID);
//            //procedure.setPath(cPath);
//            procedure.setDescription(resultSet.getString(JDBCNames.Procedures.ColumnName.DESCRIPTION));
//            procedure.setAlias(resultSet.getString(JDBCNames.Procedures.ColumnName.ALIAS));
//            procedure.setReturnsResults(resultSet.getBoolean(JDBCNames.Procedures.ColumnName.RETURNS_RESULTS));
//            procedure.setProcedureType(resultSet.getShort(JDBCNames.Procedures.ColumnName.PROC_TYPE));
//
//            result.add(procedure);
//        }
//        return result;
//    }
//
//    public static Procedure getProcedure(ResultSet resultSet, BasicProcedure procedure) throws SQLException  {
//        String path = procedure.getFullName();
//        boolean gotObject = false;
//
//        do{
//            String cPath = resultSet.getString(JDBCNames.Procedures.ColumnName.PATH);
//            if(!path.equals(cPath))
//                continue;
//
//            gotObject = true;
//            //procedure.setPath(path);
//            ((BasicProcedureID)procedure.getID()).setUID(resultSet.getLong(JDBCNames.Procedures.ColumnName.PROC_UID));
//            procedure.setDescription(resultSet.getString(JDBCNames.Procedures.ColumnName.DESCRIPTION));
//            procedure.setAlias(resultSet.getString(JDBCNames.Procedures.ColumnName.ALIAS));
//            procedure.setReturnsResults(resultSet.getBoolean(JDBCNames.Procedures.ColumnName.RETURNS_RESULTS));
//            procedure.setProcedureType(resultSet.getShort(JDBCNames.Procedures.ColumnName.PROC_TYPE));
//            break;
//        }while(resultSet.next());
//        if(gotObject)
//            return procedure;
//        return null;
//    }
//
//    public static List getProcedureParameters(ResultSet resultSet, Collection dataTypes) throws SQLException{
//        List result = new ArrayList();
//        int rPosition, pPosition;
//        while(resultSet.next()){
//            BasicProcedureParameter pp = new BasicProcedureParameter(resultSet.getString(JDBCNames.ProcedureParameters.ColumnName.PARM_NM));
//            pp.setParameterType(resultSet.getShort(JDBCNames.ProcedureParameters.ColumnName.PARM_TYPE));
//            pp.setIsOptional(resultSet.getBoolean(JDBCNames.ProcedureParameters.ColumnName.OPTIONAL));
//            long dtUid = resultSet.getLong(JDBCNames.ProcedureParameters.ColumnName.DT_UID);
//            Iterator iter = dataTypes.iterator();
//            DataType dt = null;
//            while(iter.hasNext()){
//                dt = (DataType)iter.next();
//                 if(dtUid == ((BasicDataTypeID)dt.getID()).getUID())
//                    break;
//            }
//            pp.setDataType(dt);
//            rPosition = resultSet.getInt(JDBCNames.ProcedureParameters.ColumnName.RESULTSET_POSITION);
//            if(rPosition != NA_INT)
//                pp.setResultSetPosition(rPosition);
//            else
//                pp.setResultSetPosition(MetadataConstants.NOT_DEFINED_INT);
//            pPosition = resultSet.getInt(JDBCNames.ProcedureParameters.ColumnName.PARM_POSITION);
//            if(pPosition != NA_INT)
//                pp.setPosition(pPosition);
//            else
//                pp.setPosition(MetadataConstants.NOT_DEFINED_INT);
//            result.add(pp);
//        }
//        return result;
//    }

    public static Properties getProperties(ResultSet resultSet) throws SQLException{
        Properties result = null;
        String name = null;
        String value = null;
        if(resultSet.next()){
            name = resultSet.getString(JDBCNames.VCommonProperties.ColumnName.PRP_NM);
            value = resultSet.getString(JDBCNames.VCommonProperties.ColumnName.PRP_VL);
        }else
            return result;
        result = new Properties();
        while(resultSet.next()){
            String nextName = resultSet.getString(JDBCNames.VCommonProperties.ColumnName.PRP_NM);
            if (nextName.equals(name)){
                value += resultSet.getString(JDBCNames.VCommonProperties.ColumnName.PRP_VL);
            }else{
                result.put(name, value);
                name = nextName;
                value = resultSet.getString(JDBCNames.VCommonProperties.ColumnName.PRP_VL);
            }
        }
        result.put(name, value);
        return result;
    }

//    public static Collection getDataTypes(ResultSet resultSet, VirtualDatabaseID vdbID, Collection models, boolean mmTypesOnly) throws SQLException, VirtualDatabaseException{
//        Collection result = new HashSet();
//        while(resultSet.next()){
//            long modelUid = resultSet.getLong(JDBCNames.DataTypes.ColumnName.MDL_UID);
//            BasicModelID mID = null;
//            if(modelUid != NA_LONG){
//            	//if no models are paased in, assume only want
//            	//runtime types and built-in types
//            	if(models == null){
//            		continue;
//            	}
//                Iterator iter = models.iterator();
//                while(iter.hasNext()){
//                	Object next = iter.next();
//                	if(next instanceof Model){
//                		mID = (BasicModelID)((Model)next).getID();
//                	}else{
//                		mID = (BasicModelID)next;
//                	}
//
//                    if(modelUid == mID.getUID()){
//                        break;
//                    }else{
//                    	mID = null;
//                    }
//                }
//            }
//
//            long uid = resultSet.getLong(JDBCNames.DataTypes.ColumnName.DT_UID);
//            String runtimeType = resultSet.getString(JDBCNames.DataTypes.ColumnName.RUNTIME_TYPE);
//
//            // Determine if the UUID for this datatype represents a built-in datatype
//            String dtUuidStr = resultSet.getString(JDBCNames.DataTypes.ColumnName.DT_UUID);
//            boolean isBuiltInDataType = false;
//            if (dtUuidStr != null) {
////              TODO: Removed for plugin port
////                ObjectID dtUuid = null;
////                try {
////                    dtUuid = IDGenerator.getInstance().stringToObject(dtUuidStr);
////                    isBuiltInDataType = (DefaultDatatypeConversions.getRuntimeDataTypeClassFromDatatypeUUID(dtUuid) != null);
////                } catch (InvalidIDException e) {
////                     The UUID String could not be interpretted ...
////                }
//            }
//
//            if(mmTypesOnly){
//            	//only MetaMatrix standard types
//                boolean isMMType = false;
//
//            	//assume only MetaMatrix standard types do not have a runtimeType
//                if(runtimeType == null) {
//                    isMMType = true;
//                }
//
//                //built-in MetaMatrix datatype
//            	if(isBuiltInDataType){
//                    isMMType = true;
//            	}
//                //not a MetaMatrix standard or built-in datatype
//                if(!isMMType) {
//                    continue;
//                }
//            }
//
//            String pName = resultSet.getString(JDBCNames.DataTypes.ColumnName.PATH);
//            String uuid = resultSet.getString(JDBCNames.DataTypes.ColumnName.DT_UUID);
//            BasicDataTypeID dtID = new BasicDataTypeID(pName, uid);
//            if(mID != null){
//                dtID.setModelID(mID);
//            }
//            BasicDataType dt = new BasicDataType(dtID, (BasicVirtualDatabaseID)vdbID);
//            if(uuid != null){
//            	dtID.setUuid(uuid);
//            }
//            dt.setDescription(resultSet.getString(JDBCNames.DataTypes.ColumnName.DESCRIPTION));
//            dt.setIsPhysical(resultSet.getBoolean(JDBCNames.DataTypes.ColumnName.IS_PHYSICAL));
//            dt.setIsStandard(resultSet.getBoolean(JDBCNames.DataTypes.ColumnName.IS_STANDARD));
//            dt.setType(resultSet.getShort(JDBCNames.DataTypes.ColumnName.TYPE));
//            dt.setScale(resultSet.getInt(JDBCNames.DataTypes.ColumnName.SCALE));
//            dt.setLength(resultSet.getInt(JDBCNames.DataTypes.ColumnName.LENGTH));
//            dt.setPrecisionLength(resultSet.getInt(JDBCNames.DataTypes.ColumnName.PRCSN_LNGTH));
//            dt.setRadix(resultSet.getInt(JDBCNames.DataTypes.ColumnName.RADIX));
//            dt.setFixedPrecisionLength(resultSet.getBoolean(JDBCNames.DataTypes.ColumnName.FXD_PRCSN_LNGTH));
//            dt.setSearchType(resultSet.getShort(JDBCNames.DataTypes.ColumnName.SRCH_TYPE));
//            dt.setNullType(resultSet.getShort(JDBCNames.DataTypes.ColumnName.NULL_TYPE));
//            dt.setIsSigned(resultSet.getBoolean(JDBCNames.DataTypes.ColumnName.IS_SIGNED));
//            dt.setIsAutoIncrement(resultSet.getBoolean(JDBCNames.DataTypes.ColumnName.IS_AUTOINCREMENT));
//            dt.setIsCaseSensitive(resultSet.getBoolean(JDBCNames.DataTypes.ColumnName.IS_CASE_SENSITIVE));
//            dt.setSupportsSelect(resultSet.getBoolean(JDBCNames.DataTypes.ColumnName.SUP_SELECT));
//            dt.setMinimumRange(resultSet.getString(JDBCNames.DataTypes.ColumnName.MIN_RANGE));
//            dt.setMaximumRange(resultSet.getString(JDBCNames.DataTypes.ColumnName.MAX_RANGE));
//            dt.setNativeType(resultSet.getString(JDBCNames.DataTypes.ColumnName.NATIVE_TYPE));
//            String jName1 = resultSet.getString(JDBCNames.DataTypes.ColumnName.JAVA_CLASS_1);
//            if(jName1 != null){
//                dt.setJavaClassName(jName1);
//            }
//			String baseType = resultSet.getString(JDBCNames.DataTypes.ColumnName.BASE_TYPE);
//            if(baseType != null){
//                dt.setBaseTypeUUID(baseType);
//            }
//            if(runtimeType != null){
//                dt.setRTUUID(runtimeType);
//            }
//            result.add(dt);
//        }
//
//        return result;
//    }

//    public static List getDataTypeElements(ResultSet resultSet, DataTypeID dtID, VirtualDatabaseID vdbID)throws SQLException{
//        List result = new ArrayList();
//        while(resultSet.next()){
//            long uid = resultSet.getLong(JDBCNames.DataTypeElements.ColumnName.DT_ELMNT_UID);
//            BasicDataTypeElementID dteID = new BasicDataTypeElementID(resultSet.getString(JDBCNames.DataTypeElements.ColumnName.NM), uid);
//            dteID.setDataTypeID(dtID);
//            BasicDataTypeElement dte = new BasicDataTypeElement(dteID, (BasicVirtualDatabaseID)vdbID);
//            long dtUID = resultSet.getLong(JDBCNames.DataTypeElements.ColumnName.DT_UID);
//            if(dtUID != NA_LONG)
//                dte.setDataTypeUID(dtUID);
//            dte.setExcludeData(resultSet.getBoolean(JDBCNames.DataTypeElements.ColumnName.EXCLUDE_DATA));
//            dte.setScale(resultSet.getInt(JDBCNames.DataTypeElements.ColumnName.SCALE));
//            dte.setLength(resultSet.getInt(JDBCNames.DataTypeElements.ColumnName.LENGTH));
//            dte.setIsNullable(resultSet.getBoolean(JDBCNames.DataTypeElements.ColumnName.IS_NULLABLE));
//            dte.setPosition(resultSet.getInt(JDBCNames.DataTypeElements.ColumnName.POSITION));
//            result.add(dte);
//        }
//        return result;
//    }

//    public static GroupID getGroupID(ResultSet resultSet, GroupID groupID) {
//        return null;
//    }
//    public static KeyID getKeyID(ResultSet resultSet, KeyID keyID) {
//        return null;
//    }
//    public static ModelID getModelID(ResultSet resultSet, ModelID modelID) {
//        return null;
//    }
//    public static ProcedureID getProcedureID(ResultSet resultSet, ProcedureID procID) {
//        return null;
//    }
//    public static ElementID getElementID(ResultSet resultSet, ElementID elementID) {
//        return null;
//    }

    public static MetaBaseInfo getMetaBaseInfo(ResultSet resultSet) throws SQLException{
        String version = resultSet.getString(JDBCNames.MetaBaseVersions.ColumnName.VERSION);
        Date date = new Date(resultSet.getLong(JDBCNames.MetaBaseVersions.ColumnName.VERSION_DT));
        BasicMetaBaseInfo result = new BasicMetaBaseInfo(version, date);
        result.setUID(resultSet.getLong(JDBCNames.MetaBaseVersions.ColumnName.MB_UID));
        return result;
    }

//    public static void setQueryPlan(ResultSet resultSet, Group group) throws SQLException{
//        StringBuffer sb = new StringBuffer();
//        int queryPlanType   = -1;
//        int currentPlanType = -1;
//        while(resultSet.next()){
//            if(resultSet.getObject(JDBCNames.QueryPlans.ColumnName.QUERY_PLAN_TYPE) != null){
//                queryPlanType = resultSet.getInt(JDBCNames.QueryPlans.ColumnName.QUERY_PLAN_TYPE);
//            }
//            // Store the query plan and reset the StringBuffer
//            if (queryPlanType >= 0 && currentPlanType >= 0 && queryPlanType != currentPlanType) {
//                setQueryPlan(group,sb.toString(),currentPlanType);
//                sb.setLength(0);
//            }
//            sb.append(resultSet.getString(JDBCNames.QueryPlans.ColumnName.QUERY_PLAN));
//            currentPlanType = queryPlanType;
//        }
//        setQueryPlan(group,sb.toString(),queryPlanType);
//    }
//
//    private static void setQueryPlan(Group group, String queryPlan, int queryPlanType) {
//        if (queryPlanType >= 0 && queryPlan != null && queryPlan.length() > 0) {
//            // QUERY_PLAN_GROUP
//            if(queryPlanType == 1) {
//                ((BasicGroup)group).setQueryPlan(queryPlan);
//                ((BasicGroup)group).setHasQueryPlan(true);
//            }
//            // MAPPING_DEFN
//            else if(queryPlanType == 0){
//                ((BasicGroup)group).setMappingDocument(queryPlan);
//            }
//            // QUERY_PLAN_INSERT_QUERY
//            else if(queryPlanType == 3){
//                ((BasicGroup)group).setInsertQueryPlan(queryPlan);
//                ((BasicGroup)group).setHasInsertQueryPlan(true);
//            }
//            // QUERY_PLAN_UPDATE_QUERY
//            else if(queryPlanType == 4){
//                ((BasicGroup)group).setUpdateQueryPlan(queryPlan);
//                ((BasicGroup)group).setHasUpdateQueryPlan(true);
//            }
//            // QUERY_PLAN_DELETE_QUERY
//            else if(queryPlanType == 5){
//                ((BasicGroup)group).setDeleteQueryPlan(queryPlan);
//                ((BasicGroup)group).setHasDeleteQueryPlan(true);
//            }
//        }
//    }
//
//    public static void setQueryPlan(ResultSet resultSet, Procedure storedQuery) throws SQLException{
//        StringBuffer sb = new StringBuffer();
//        while(resultSet.next()){
//            sb.append(resultSet.getString(JDBCNames.QueryPlans.ColumnName.QUERY_PLAN));
//        }
//
//        ((BasicProcedure)storedQuery).setQueryPlan(sb.toString() );
//    }
//
//    public static List getDocumentSchemas(ResultSet resultSet) throws SQLException{
//        List schemas = new ArrayList();
//        StringBuffer sb = new StringBuffer();
//        long currentSchema = -1;
//        if(resultSet.next()){
//        	currentSchema = resultSet.getLong(JDBCNames.Schemas.ColumnName.SCHEMA_UID);
//        	sb.append(resultSet.getString(JDBCNames.Schemas.ColumnName.VALUE));
//        }
//        while(resultSet.next()){
//            long uid = resultSet.getLong(JDBCNames.Schemas.ColumnName.SCHEMA_UID);
//            if (uid != currentSchema){
//                schemas.add(sb.toString());
//                currentSchema = uid;
//                sb = new StringBuffer();
//                sb.append(resultSet.getString(JDBCNames.Schemas.ColumnName.VALUE));
//            }else{
//                sb.append(resultSet.getString(JDBCNames.Schemas.ColumnName.VALUE));
//            }
//        }
//
//      	if(sb.length() > 0){
//      		schemas.add(sb.toString());
//      	}
//
//        return schemas;
//    }

//	public static Collection getSchemaUIDs(ResultSet resultSet) throws SQLException{
//		Collection result = new HashSet();
//		while(resultSet.next()){
//            result.add(new Long(resultSet.getLong(1)));
//		}
//
//		LogManager.logTrace(LogRuntimeMetadataConstants.CTX_RUNTIME_METADATA, new Object[]{"Got schema uids:", result});
//		return result;
//	}

//	public static Collection getDataTypeUIDs(ResultSet resultSet) throws SQLException{
//		Collection result = new HashSet();
//		while(resultSet.next()){
//            result.add(new Long(resultSet.getLong(JDBCNames.DataTypes.ColumnName.DT_UID)));
//		}
//		return result;
//	}

//	public static Map getDataTypeAndParentUIDs(ResultSet resultSet) throws SQLException{
//		Map result = new HashMap();
//		//key: DT_UUID value: RT_UID
//		Map uuidAndUids = new HashMap();
//		//key: RT_UID value: btUUID
//		Map uidAndUuids = new HashMap();
//		while(resultSet.next()){
//			String btUUID = resultSet.getString(JDBCNames.DataTypes.ColumnName.BASE_TYPE);
//			String dtUUID = resultSet.getString(JDBCNames.DataTypes.ColumnName.DT_UUID);
//			if(dtUUID != null){
//				uuidAndUids.put(dtUUID, new Long(resultSet.getLong(JDBCNames.DataTypes.ColumnName.DT_UID)));
//			}
//            uidAndUuids.put(new Long(resultSet.getLong(JDBCNames.DataTypes.ColumnName.DT_UID)), btUUID);
//		}
//		Iterator iter = uidAndUuids.keySet().iterator();
//		while(iter.hasNext()){
//			Object uid = iter.next();
//			Object pUuid = uidAndUuids.get(uid);
//			if(pUuid != null){
//				Object puid = uuidAndUids.get(pUuid);
//				if(puid != null){
//					result.put(uid, puid);
//					continue;
//				}
//			}
//			//no basetype found
//			result.put(uid, new Long(MetadataConstants.NOT_DEFINED_LONG));
//		}
//
//		return result;
//	}

    static String getPropertyQuery(MetadataID id, int queryID){
        String sql = null;
        if(queryID == 1)
            sql = SELECT_PROPERTIES;
        else if(queryID == 2)
            sql = INSERT_PROP_NAMES;
        else if(queryID == 3)
            sql = INSERT_PROP_VALUES;

        int i;
        if(id instanceof ModelID){
            i = sql.indexOf("?");
            if(queryID == 1){
                sql = sql.substring(0,i) + JDBCNames.VModelProperties.TABLE_NAME + sql.substring(i+1);
                int j = sql.indexOf("=");
                sql = sql.substring(0,j) + JDBCNames.ModelPropNames.TABLE_NAME + "."
                    + JDBCNames.VCommonProperties.ColumnName.PRP_UID + "="
                    + JDBCNames.ModelPropValues.TABLE_NAME + "."
                    + JDBCNames.VCommonProperties.ColumnName.PRP_UID + sql.substring(j+1);
            }else if(queryID ==2)
                sql = sql.substring(0,i) + JDBCNames.ModelPropNames.TABLE_NAME + sql.substring(i+1);
            else
                sql = sql.substring(0,i) + JDBCNames.ModelPropValues.TABLE_NAME + sql.substring(i+1);
            if(queryID != 3){
                i = sql.indexOf("?");
                sql = sql.substring(0,i) + JDBCNames.VModelProperties.ColumnName.MDL_UID + sql.substring(i+1);
            }
        }
//        else if(id instanceof ElementID){
//            i = sql.indexOf("?");
//            if(queryID == 1){
//                sql = sql.substring(0,i) + JDBCNames.VElementProperties.TABLE_NAME + sql.substring(i+1);
//                int j = sql.indexOf("=");
//                sql = sql.substring(0,j) + JDBCNames.ElementPropNames.TABLE_NAME + "."
//                    + JDBCNames.VCommonProperties.ColumnName.PRP_UID + "="
//                    + JDBCNames.ElementPropValues.TABLE_NAME + "."
//                    + JDBCNames.VCommonProperties.ColumnName.PRP_UID + sql.substring(j+1);
//            }else if (queryID ==2)
//                sql = sql.substring(0,i) + JDBCNames.ElementPropNames.TABLE_NAME + sql.substring(i+1);
//            else
//                sql = sql.substring(0,i) + JDBCNames.ElementPropValues.TABLE_NAME + sql.substring(i+1);
//            if(queryID != 3){
//                i = sql.indexOf("?");
//                sql = sql.substring(0,i) + JDBCNames.VElementProperties.ColumnName.ELMNT_UID + sql.substring(i+1);
//            }
//        }
        else {
            return null;
        }
        return sql;
    }

//    static String getDeletePropertyQuery(MetadataID id){
//        if(id instanceof ModelID){
//            return DELETE + FROM + JDBCNames.ModelPropNames.TABLE_NAME + WHERE + JDBCNames.ModelPropNames.ColumnName.MDL_UID + "=?";
//        }
//        else if(id instanceof ElementID){
//            return DELETE + FROM + JDBCNames.ElementPropNames.TABLE_NAME + WHERE + JDBCNames.ElementPropNames.ColumnName.ELMNT_UID + "=?";
//        }
//        else{
//            return null;
//        }
//    }
//
//    static String getDeletePropertyValQuery(MetadataID id){
//        if(id instanceof ModelID){
//            return DELETE + FROM + JDBCNames.ModelPropValues.TABLE_NAME + WHERE + JDBCNames.ModelPropValues.ColumnName.PRP_UID + IN
//                + "(" + SELECT + JDBCNames.ModelPropNames.ColumnName.PRP_UID + FROM + JDBCNames.ModelPropNames.TABLE_NAME + WHERE + JDBCNames.ModelPropNames.ColumnName.MDL_UID + "=?)";
//        }
//        else if(id instanceof ElementID){
//            return DELETE + FROM + JDBCNames.ElementPropValues.TABLE_NAME + WHERE + JDBCNames.ElementPropValues.ColumnName.PRP_UID + IN
//                + "(" + SELECT + JDBCNames.ElementPropNames.ColumnName.PRP_UID + FROM + JDBCNames.ElementPropNames.TABLE_NAME + WHERE + JDBCNames.ElementPropNames.ColumnName.ELMNT_UID + "=?)";
//        }
//        else{
//            return null;
//        }
//    }
}

