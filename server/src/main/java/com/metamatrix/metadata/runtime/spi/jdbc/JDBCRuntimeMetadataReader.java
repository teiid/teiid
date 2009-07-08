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

/*
 *
 */
package com.metamatrix.metadata.runtime.spi.jdbc;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Properties;

import org.teiid.metadata.RuntimeMetadataPlugin;

import com.metamatrix.common.log.I18nLogManager;
import com.metamatrix.common.log.LogManager;
import com.metamatrix.core.CorePlugin;
import com.metamatrix.metadata.runtime.api.MetadataConstants;
import com.metamatrix.metadata.runtime.api.MetadataID;
import com.metamatrix.metadata.runtime.api.VirtualDatabase;
import com.metamatrix.metadata.runtime.api.VirtualDatabaseDoesNotExistException;
import com.metamatrix.metadata.runtime.api.VirtualDatabaseException;
import com.metamatrix.metadata.runtime.api.VirtualDatabaseID;
import com.metamatrix.metadata.runtime.model.BasicMetadataID;
import com.metamatrix.metadata.runtime.model.BasicModelID;
import com.metamatrix.metadata.runtime.model.BasicVirtualDatabaseID;
import com.metamatrix.metadata.runtime.util.LogRuntimeMetadataConstants;
import com.metamatrix.metadata.util.ErrorMessageKeys;
import com.metamatrix.metadata.util.LogMessageKeys;

public final class JDBCRuntimeMetadataReader {

//    private static String FAKE_NAME = "noneExistN";
//    private static String FAKE_VERSION = "noneExistV";
//    private static String IS_TRUE = "1";
//    private static String IS_FALSE = "0";
//    private static int MAX_COLUMN_WIDTH = 255;

    /**
     * returns the <code>VirtualDatabase</code> based on the virtual database id.
     * @param virtualDatabaseID is the VirtualDatabase to be returned.
     * @throws VirtualDatabaseException if an error occurs while trying to read the data.
     * @return VirtualDatabase
     */
    public static VirtualDatabase getVirtualDatabase(VirtualDatabaseID virtualDatabaseID, Connection jdbcConnection) throws VirtualDatabaseException {
        PreparedStatement statement = null;
        String sql = null;
        VirtualDatabase result = null;

        try{
            long uid = ((BasicVirtualDatabaseID)(virtualDatabaseID)).getUID();
            sql = JDBCTranslator.SELECT_VIRTUAL_DATABASE;
            LogManager.logDetail(LogRuntimeMetadataConstants.CTX_RUNTIME_METADATA, LogMessageKeys.JDBCR_0001, new Object[]{virtualDatabaseID.getFullName(), virtualDatabaseID.getVersion(), new Long(uid)});
            statement = jdbcConnection.prepareStatement(sql);
            statement.setLong(1, uid);
            if (! statement.execute()){
                throw new VirtualDatabaseException(ErrorMessageKeys.GEN_0007, RuntimeMetadataPlugin.Util.getString(ErrorMessageKeys.GEN_0007, sql) );
            }
            ResultSet results = statement.getResultSet();
            if(results.next())
                result = JDBCTranslator.getVirtualDatabase(results, virtualDatabaseID);
        }catch (SQLException se){
                throw new VirtualDatabaseException(se, ErrorMessageKeys.JDBCR_0001, RuntimeMetadataPlugin.Util.getString(ErrorMessageKeys.JDBCR_0001, sql) );
        }finally {
            if ( statement != null ) {
                try {
                    statement.close();
                } catch ( SQLException e ) {
                	LogManager.logDetail(LogRuntimeMetadataConstants.CTX_RUNTIME_METADATA, e, CorePlugin.Util.getString(ErrorMessageKeys.GEN_0008));
                }
            }
        }

        return result;
    }

    /**
     * returns the <code>VirtualDatabaseID</code> for the specified full name and version.  This method does validate the existance of the virtual database by reading from the persistance storage before creating the id.
     * @throws VirtualDatabaseDoesNotExistException exception if the virtual database does not exist
     * @throws VirtualDatabaseException if an error occurs while trying to read the data.
     * @return VirtualDatabaseID
     */
    public static  VirtualDatabaseID getVirtualDatabaseID(String fullName, String version, Connection jdbcConnection) throws VirtualDatabaseDoesNotExistException, VirtualDatabaseException {
        return getVirtualDatabaseID(fullName, version, false, jdbcConnection);
    }





	/**
	 * returns a <code>Collection</code> of type <code>VirtualDatabase</code> that represents all the virtual databases in the system.
	 * @return Collection of type VirtualDatabase
	 * @throws VirtualDatabaseException an error occurs while trying to read the data.
	 */
    public static  Collection getVirtualDatabases(Connection jdbcConnection) throws VirtualDatabaseException {
        Statement statement = null;
        String sql = null;
        Collection result = null;

        try{
            sql = JDBCTranslator.SELECT_VIRTUAL_DATABASES;
            statement = jdbcConnection.createStatement();
            if (! statement.execute(sql)){
                throw new VirtualDatabaseException(ErrorMessageKeys.GEN_0007, RuntimeMetadataPlugin.Util.getString(ErrorMessageKeys.GEN_0007, sql) );
            }
            ResultSet results = statement.getResultSet();
            result = JDBCTranslator.getVirtualDatabases(results);
        }catch (SQLException se){
            throw new VirtualDatabaseException(se, ErrorMessageKeys.JDBCR_0001, RuntimeMetadataPlugin.Util.getString(ErrorMessageKeys.JDBCR_0001, sql) );
        }finally {
            if ( statement != null ) {
                try {
                    statement.close();
                } catch ( SQLException e ) {
                    I18nLogManager.logError(LogRuntimeMetadataConstants.CTX_RUNTIME_METADATA, ErrorMessageKeys.GEN_0008, e);
                }
            }
        }

        return result;
    }

	/**
	 * returns a <code>Collection</code> of type <code>VirtualDatabaseID</code> that represents all the virtual databases marked for deletion in the system.
	 * @return Collection of type VirtualDatabase
	 * @throws VirtualDatabaseException an error occurs while trying to read the data.
	 */
    public static  Collection getDeletedVirtualDatabaseIDs(Connection jdbcConnection) throws VirtualDatabaseException {
        Statement statement = null;
        String sql = null;
        Collection result = null;

        try{
            sql = JDBCTranslator.SELECT_DELETED_VIRTUAL_DATABASES;
            statement = jdbcConnection.createStatement();
            if (! statement.execute(sql)){
                throw new VirtualDatabaseException(ErrorMessageKeys.GEN_0007, RuntimeMetadataPlugin.Util.getString(ErrorMessageKeys.GEN_0007, sql) );
            }
            ResultSet results = statement.getResultSet();
            result = JDBCTranslator.getVirtualDatabaseIDs(results);
        }catch (SQLException se){
            throw new VirtualDatabaseException(se, ErrorMessageKeys.JDBCR_0001, RuntimeMetadataPlugin.Util.getString(ErrorMessageKeys.JDBCR_0001, sql) );
        }finally {
            if ( statement != null ) {
                try {
                    statement.close();
                } catch ( SQLException e ) {
                    I18nLogManager.logError(LogRuntimeMetadataConstants.CTX_RUNTIME_METADATA, ErrorMessageKeys.GEN_0008, e);
                }
            }
        }

        return result;
    }

/**
 * returns a <code>Collection</code> of type <code>Model</code> that represents all the data sources tthat where deployed in the specified virtual database id
 * @param vdbID is the VirtualDatabaseID
 * @return Collection of type Model
 * @throws VirtualDatabaseException an error occurs while trying to read the data.
 */
    public static  Collection getModels(VirtualDatabaseID vdbID, Connection jdbcConnection) throws VirtualDatabaseException {
        PreparedStatement statement = null;
        String sql = null;
        Collection result = null;

        long uid = ((BasicVirtualDatabaseID)(vdbID)).getUID();

        //get uids for XML Schema Models
        Collection smUids = null;

        try{
            sql = JDBCTranslator.SELECT_MODELS;
            statement = jdbcConnection.prepareStatement(sql);
            statement.setLong(1, uid);
            if (! statement.execute()){
                throw new VirtualDatabaseException(ErrorMessageKeys.GEN_0007, RuntimeMetadataPlugin.Util.getString(ErrorMessageKeys.GEN_0007, sql) );
            }
            ResultSet results = statement.getResultSet();
            //smUids.addAll(dtmUids);
            result = JDBCTranslator.getModels(results, vdbID, smUids);
        }catch (SQLException se){
            throw new VirtualDatabaseException(se, ErrorMessageKeys.JDBCR_0001, RuntimeMetadataPlugin.Util.getString(ErrorMessageKeys.JDBCR_0001, sql) );
        }finally {
            if ( statement != null ) {
                try {
                    statement.close();
                } catch ( SQLException e ) {
                    I18nLogManager.logError(LogRuntimeMetadataConstants.CTX_RUNTIME_METADATA, ErrorMessageKeys.GEN_0008, e);
                }
            }
        }

        return result;
    }


    /**
     * Used only by the RuntimeMetadataCatalog to find the active id.
     */
    public static VirtualDatabaseID getActiveVirtualDatabaseID(String vdbName, String vdbVersion, Connection jdbcConnection) throws VirtualDatabaseException, VirtualDatabaseDoesNotExistException{
        return getVirtualDatabaseID(vdbName, vdbVersion, true, jdbcConnection);
    }

    /**
     * Return the properties for the metadata specified by the metadataID.
     * @param metadataID is the metadata id for which the properties are to be obtained.
     * @return properties. Return uull if there is no properties for the specified metadata.
     * @throws VirtualDatabaseException if an error occurs while trying to read the data.
     */
    public static Properties getProperties(MetadataID metadataID, Connection jdbcConnection) throws  VirtualDatabaseException{
        PreparedStatement statement = null;
        String sql = null;
        Properties result = null;

        try{
            sql = JDBCTranslator.getPropertyQuery(metadataID, 1);
            if(sql == null)
                return result;
            statement = jdbcConnection.prepareStatement(sql);
            statement.setLong(1, ((BasicMetadataID)metadataID).getUID());
            if (! statement.execute()){
                throw new VirtualDatabaseException(ErrorMessageKeys.GEN_0007, RuntimeMetadataPlugin.Util.getString(ErrorMessageKeys.GEN_0007, sql) );
            }
            ResultSet results = statement.getResultSet();
            result = JDBCTranslator.getProperties(results);
        }catch (SQLException se){
            throw new VirtualDatabaseException(se, ErrorMessageKeys.JDBCR_0001, RuntimeMetadataPlugin.Util.getString(ErrorMessageKeys.JDBCR_0001, sql) );
        }finally {
            if ( statement != null ) {
                try {
                    statement.close();
                } catch ( SQLException e ) {
                    I18nLogManager.logError(LogRuntimeMetadataConstants.CTX_RUNTIME_METADATA, ErrorMessageKeys.GEN_0008, e);
                }
            }
        }

        return result;
    }

    public static List getAllModelIDs(Connection jdbcConnection) throws VirtualDatabaseException{
        Statement statement = null;
        String sql = null;
        List result = null;

        try{
            sql = JDBCTranslator.SELECT_MODEL_IDS;
            statement = jdbcConnection.createStatement();
            if (! statement.execute(sql)){
                throw new VirtualDatabaseException(ErrorMessageKeys.GEN_0007, RuntimeMetadataPlugin.Util.getString(ErrorMessageKeys.GEN_0007, sql) );
            }
            ResultSet results = statement.getResultSet();
            result = JDBCTranslator.getModelIDs(results);
            //get timestamp for each model
            Iterator iter = result.iterator();
            while(iter.hasNext()){
            	BasicModelID mID = (BasicModelID)iter.next();
            	Properties props = getProperties(mID, jdbcConnection);
            	if(props != null){
	            	String versionDate = props.getProperty(MetadataConstants.VERSION_DATE);
	            	if(versionDate != null){
	            		mID.setVersionDate(versionDate);
	            	}
            	}
            }
        }catch (SQLException se){
            throw new VirtualDatabaseException(se, ErrorMessageKeys.JDBCR_0001, RuntimeMetadataPlugin.Util.getString(ErrorMessageKeys.JDBCR_0001, sql) );
        }finally {
            if ( statement != null ) {
                try {
                    statement.close();
                } catch ( SQLException e ) {
                    I18nLogManager.logError(LogRuntimeMetadataConstants.CTX_RUNTIME_METADATA, ErrorMessageKeys.GEN_0008, e);
                }
            }
        }

        return result;
    }

    public static Collection getModelIDsOnlyInVDB(VirtualDatabaseID vdbID, Connection jdbcConnection) throws VirtualDatabaseException{
        PreparedStatement statement = null;
        String sql = null;
        Collection result = null;

        try{
            long uid = ((BasicVirtualDatabaseID)(vdbID)).getUID();
            sql = JDBCTranslator.SELECT_MODEL_IDS_ONLY_IN_VDB;
            statement = jdbcConnection.prepareStatement(sql);
            statement.setLong(1, uid);
            if (! statement.execute()){
                throw new VirtualDatabaseException(ErrorMessageKeys.GEN_0007, RuntimeMetadataPlugin.Util.getString(ErrorMessageKeys.GEN_0007, sql) );
            }
            ResultSet results = statement.getResultSet();
            result = JDBCTranslator.getModelIDs(results);
        }catch (SQLException se){
            throw new VirtualDatabaseException(se, ErrorMessageKeys.JDBCR_0001, RuntimeMetadataPlugin.Util.getString(ErrorMessageKeys.JDBCR_0001, sql) );
        }finally {
            if ( statement != null ) {
                try {
                    statement.close();
                } catch ( SQLException e ) {
                    I18nLogManager.logError(LogRuntimeMetadataConstants.CTX_RUNTIME_METADATA, ErrorMessageKeys.GEN_0008, e);
                }
            }
        }

        return result;
    }

    protected static  VirtualDatabaseID getVirtualDatabaseID(String fullName, String version, boolean isActive, Connection jdbcConnection) throws VirtualDatabaseDoesNotExistException, VirtualDatabaseException {
        PreparedStatement statement = null;
        String sql = null;
        VirtualDatabaseID result = null;

        try{
            if(isActive){
                if(version == null){
                    sql = JDBCTranslator.SELECT_ACTIVE_VIRTUAL_DATABASE_ID_LV;
                }else{
                    sql = JDBCTranslator.SELECT_ACTIVE_VIRTUAL_DATABASE_ID;
                }
            }else{
                if(version == null){
                    sql = JDBCTranslator.SELECT_VIRTUAL_DATABASE_ID_LV;
                }else
                    sql = JDBCTranslator.SELECT_VIRTUAL_DATABASE_ID;
            }
            statement = jdbcConnection.prepareStatement(sql);
            statement.setString(1, fullName.toUpperCase());
            if(version != null){
                statement.setString(2, version);
            }
            else{
                statement.setString(2, fullName.toUpperCase());
            }
            if (! statement.execute()){
                throw new VirtualDatabaseException(ErrorMessageKeys.GEN_0007, RuntimeMetadataPlugin.Util.getString(ErrorMessageKeys.GEN_0007, sql) );
            }
            ResultSet results = statement.getResultSet();
            if(results.next()){
                result = JDBCTranslator.getVirtualDatabaseID(results);
            }
        }catch (SQLException se){
            throw new VirtualDatabaseException(se, ErrorMessageKeys.JDBCR_0001, RuntimeMetadataPlugin.Util.getString(ErrorMessageKeys.JDBCR_0001, sql) );
        }finally {
            if ( statement != null ) {
                try {
                    statement.close();
                } catch ( SQLException e ) {
                    I18nLogManager.logError(LogRuntimeMetadataConstants.CTX_RUNTIME_METADATA, ErrorMessageKeys.GEN_0008, e);
                }
            }
        }
        if (result != null) {
            LogManager.logDetail(LogRuntimeMetadataConstants.CTX_RUNTIME_METADATA, CorePlugin.Util.getString(LogMessageKeys.JDBCR_0002, new Object[]{result.getFullName(), result.getVersion(), new Long( ((BasicVirtualDatabaseID)result).getUID() )}) );
        } else {
            LogManager.logDetail(LogRuntimeMetadataConstants.CTX_RUNTIME_METADATA, CorePlugin.Util.getString(LogMessageKeys.JDBCR_0003, new Object[]{fullName}));
        }
        return result;
    }


    private static String replace(String oriString, String from, String to){
        int index = oriString.indexOf(from);
        if(index == -1)
            return oriString;
        StringBuffer temp = new StringBuffer(oriString);
        temp.replace(index, index + from.length(), to);
        return temp.toString();
    }



}


