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

package com.metamatrix.metadata.runtime.spi.jdbc;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Properties;

import com.metamatrix.common.jdbc.JDBCReservedWords;
import com.metamatrix.common.vdb.api.ModelInfo;
import com.metamatrix.core.MetaMatrixRuntimeException;
import com.metamatrix.core.util.DateUtil;
import com.metamatrix.core.vdb.VDBStatus;
import com.metamatrix.metadata.runtime.RuntimeMetadataPlugin;
import com.metamatrix.metadata.runtime.api.MetadataID;
import com.metamatrix.metadata.runtime.api.Model;
import com.metamatrix.metadata.runtime.api.ModelID;
import com.metamatrix.metadata.runtime.api.VirtualDatabase;
import com.metamatrix.metadata.runtime.api.VirtualDatabaseID;
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
        else {
            return null;
        }
        return sql;
    }

}

