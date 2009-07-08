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

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Collection;
import java.util.Iterator;
import java.util.Map;

import org.teiid.metadata.RuntimeMetadataPlugin;

import com.metamatrix.common.connection.BaseTransaction;
import com.metamatrix.common.connection.ManagedConnection;
import com.metamatrix.common.connection.ManagedConnectionException;
import com.metamatrix.common.connection.jdbc.JDBCMgdResourceConnection;
import com.metamatrix.common.log.I18nLogManager;
import com.metamatrix.common.log.LogManager;
import com.metamatrix.core.util.DateUtil;
import com.metamatrix.core.util.StringUtil;
import com.metamatrix.core.vdb.VDBStatus;
import com.metamatrix.metadata.runtime.api.ModelID;
import com.metamatrix.metadata.runtime.api.VirtualDatabase;
import com.metamatrix.metadata.runtime.api.VirtualDatabaseDoesNotExistException;
import com.metamatrix.metadata.runtime.api.VirtualDatabaseException;
import com.metamatrix.metadata.runtime.api.VirtualDatabaseID;
import com.metamatrix.metadata.runtime.exception.InvalidStateException;
import com.metamatrix.metadata.runtime.model.BasicModel;
import com.metamatrix.metadata.runtime.model.BasicModelID;
import com.metamatrix.metadata.runtime.model.BasicVirtualDatabaseID;
import com.metamatrix.metadata.runtime.spi.MetaBaseConnector;
import com.metamatrix.metadata.runtime.util.LogRuntimeMetadataConstants;
import com.metamatrix.metadata.util.ErrorMessageKeys;

public class JDBCConnector extends BaseTransaction implements MetaBaseConnector {

    private static String IS_TRUE = "1"; //$NON-NLS-1$
    private static String IS_FALSE = "0"; //$NON-NLS-1$
    private Connection jdbcConnection;
    /**
     * Create a new instance of a transaction for a managed connection.
     * @param connection the connection that should be used and that was created using this
     * factory's <code>createConnection</code> method (thus the transaction subclass may cast to the
     * type created by the <code>createConnection</code> method.
     * @param readonly true if the transaction is to be readonly, or false otherwise
     * @throws ManagedConnectionException if there is an error creating the connector.
     */
    JDBCConnector( ManagedConnection connection, boolean readonly ) throws ManagedConnectionException {
        super(connection,readonly);
        try {

            JDBCMgdResourceConnection jdbcManagedConnection = (JDBCMgdResourceConnection) connection;
            this.jdbcConnection = jdbcManagedConnection.getConnection();
        } catch ( Exception e ) {
            throw new ManagedConnectionException(ErrorMessageKeys.JDBCC_0002, RuntimeMetadataPlugin.Util.getString(ErrorMessageKeys.JDBCC_0002, JDBCMgdResourceConnection.class.getName() ) );
        }

    }

    /**
     * returns the <code>VirtualDatabase</code> based on the virtual database id.
     * @param virtualDatabaseID is the VirtualDatabase to be returned.
     * @throws VirtualDatabaseException if an error occurs while trying to read the data.
     * @return VirtualDatabase
     */
    public  VirtualDatabase getVirtualDatabase(VirtualDatabaseID virtualDatabaseID) throws VirtualDatabaseException {
        return JDBCRuntimeMetadataReader.getVirtualDatabase(virtualDatabaseID, jdbcConnection);
    }

    /**
     * returns the <code>VirtualDatabaseID</code> for the specified full name and version.  This method does validate the existance of the virtual database by reading from the persistance storage before creating the id.
     * @throws VirtualDatabaseDoesNotExistException exception if the virtual database does not exist
     * @throws VirtualDatabaseException if an error occurs while trying to read the data.
     * @return VirtualDatabaseID
     */
    public  VirtualDatabaseID getVirtualDatabaseID(String fullName, String version) throws VirtualDatabaseDoesNotExistException, VirtualDatabaseException {
        return getVirtualDatabaseID(fullName, version, false);
    }

/**
 * returns a <code>Collection</code> of type <code>VirtualDatabase</code> that represents all the virtual databases in the system.
 * @return Collection of type VirtualDatabase
 * @throws VirtualDatabaseException an error occurs while trying to read the data.
 */
    public  Collection getVirtualDatabases() throws VirtualDatabaseException {
        return JDBCRuntimeMetadataReader.getVirtualDatabases(jdbcConnection);
    }

/**
 * returns a <code>Collection</code> of type <code>VirtualDatabaseID</code> that represents all the virtual databases marked for deletion in the system.
 * @return Collection of type VirtualDatabase
 * @throws VirtualDatabaseException an error occurs while trying to read the data.
 */
    public  Collection getDeletedVirtualDatabaseIDs() throws VirtualDatabaseException {
        return JDBCRuntimeMetadataReader.getDeletedVirtualDatabaseIDs(jdbcConnection);
    }

/**
 * returns a <code>Collection</code> of type <code>Model</code> that represents all the data sources tthat where deployed in the specified virtual database id
 * @param vdbID is the VirtualDatabaseID
 * @return Collection of type Model
 * @throws VirtualDatabaseException an error occurs while trying to read the data.
 */
    public  Collection getModels(VirtualDatabaseID vdbID) throws VirtualDatabaseException {
        return JDBCRuntimeMetadataReader.getModels(vdbID, jdbcConnection);
    }

    /**
     * Used only by the RuntimeMetadataCatalog to find the active id.
     */
    public VirtualDatabaseID getActiveVirtualDatabaseID(String vdbName, String vdbVersion) throws VirtualDatabaseException, VirtualDatabaseDoesNotExistException{
        return getVirtualDatabaseID(vdbName, vdbVersion, true);
    }

    public Collection getModelIDsOnlyInVDB(VirtualDatabaseID vdbID) throws VirtualDatabaseException{
        return JDBCRuntimeMetadataReader.getModelIDsOnlyInVDB(vdbID, jdbcConnection);
    }

    /**
     * Updates the <code>VirtualDatabase</code> status.
     * @param virtualDBID represents the VirtualDatabase to be updated
     * @param status is the state the VirtualDatabase should be set to
     * @param userName of the person requesting the change
     * @exception VirtualDatabaseException if unable to perform update.
     * @exception InvalidStateException if the status to be set is invalid.
     */
    public void setStatus(VirtualDatabaseID virtualDBID, short status, String userName) throws InvalidStateException, VirtualDatabaseException {
        VirtualDatabase vdb = getVirtualDatabase(virtualDBID);

       if(status == vdb.getStatus())
            return;

       if(vdb.getStatus() == VDBStatus.DELETED){
            throw new InvalidStateException(ErrorMessageKeys.JDBCC_0008, RuntimeMetadataPlugin.Util.getString(ErrorMessageKeys.JDBCC_0008, virtualDBID) );
       }

       long uid = ((BasicVirtualDatabaseID) vdb.getVirtualDatabaseID()).getUID();

        JDBCRuntimeMetadataWriter.setStatus(virtualDBID, uid, status, userName, jdbcConnection);
    }

    public void insertModels(Collection metadataObjects, VirtualDatabaseID vdbID) throws VirtualDatabaseException {
        PreparedStatement statement = null;
        String sql = null;
        BasicModel model = null;
        Iterator iter = metadataObjects.iterator();
        try{
	        sql = JDBCTranslator.INSERT_MODELS;
	        statement = jdbcConnection.prepareStatement(sql);
            while(iter.hasNext()){
                statement.clearParameters();
                model = (BasicModel)iter.next();
                BasicModelID modelID = (BasicModelID)model.getID();
                
                statement.setLong(1, modelID.getUID());
                statement.setString(2, model.getName());
                statement.setString(3, ((ModelID)model.getID()).getVersion());
                statement.setString(4, adjustLengthToFit(model.getDescription()));
                statement.setString(5, model.isPhysical()? IS_TRUE : IS_FALSE);
                statement.setString(6, model.isMultiSourceBindingEnabled()? IS_TRUE : IS_FALSE);
                statement.setShort(7, model.getVisibility());
                statement.setString(8, ((BasicModelID)model.getID()).getUuid());
                statement.setInt(9, model.getModelType());
                statement.setString(10, model.getModelURI());

                
                if (statement.executeUpdate() != 1) {
	                throw new VirtualDatabaseException(ErrorMessageKeys.JDBCC_0009, RuntimeMetadataPlugin.Util.getString(ErrorMessageKeys.JDBCC_0009) );
                }
                LogManager.logTrace(LogRuntimeMetadataConstants.CTX_RUNTIME_METADATA,new Object[]{"Inserted Model with DBID ",new Long(modelID.getUID())}); //$NON-NLS-1$

                
//                Properties prop;
//                if((prop = model.getCurrentProperties()) != null)
//                    this.insertProperties(modelID, RuntimeMetadataIDContext.MODEL_PROP, prop);
            }
            insertVDBModels(metadataObjects, vdbID);
            
        }catch (SQLException se){
            BasicModelID modelID = (BasicModelID)model.getID();
            sql = StringUtil.replace(sql, "?", Long.toString(modelID.getUID()) ); //$NON-NLS-1$
            sql = StringUtil.replace(sql, "?", model.getName()); //$NON-NLS-1$
            sql = StringUtil.replace(sql, "?", ((ModelID)model.getID()).getVersion()); //$NON-NLS-1$
            sql = StringUtil.replace(sql, "?", (adjustLengthToFit(model.getDescription())==null?"NULL" : adjustLengthToFit(model.getDescription()))); //$NON-NLS-1$ //$NON-NLS-2$
            sql = StringUtil.replace(sql, "?", (model.isPhysical()? IS_TRUE : IS_FALSE)); //$NON-NLS-1$
            sql = StringUtil.replace(sql, "?", (model.isMultiSourceBindingEnabled()? IS_TRUE : IS_FALSE)); //$NON-NLS-1$
            sql = StringUtil.replace(sql, "?", (model.isVisible()? IS_TRUE : IS_FALSE)); //$NON-NLS-1$
            sql = StringUtil.replace(sql, "?", ((BasicModelID)model.getID()).getUuid()); //$NON-NLS-1$
            sql = StringUtil.replace(sql, "?", Integer.toString(model.getModelType())); //$NON-NLS-1$
            sql = StringUtil.replace(sql, "?", model.getModelURI()); //$NON-NLS-1$

            throw new VirtualDatabaseException(se, ErrorMessageKeys.JDBCC_0003, RuntimeMetadataPlugin.Util.getString(ErrorMessageKeys.JDBCC_0003, sql) );
        }finally {
            if ( statement != null ) {
                try {
                    statement.close();
                } catch ( SQLException e ) {
                    I18nLogManager.logError(LogRuntimeMetadataConstants.CTX_RUNTIME_METADATA, ErrorMessageKeys.GEN_0008, e);
                }
            }
        }
    }

    public void insertVirtualDatabase(VirtualDatabase vdb) throws VirtualDatabaseException  {
        PreparedStatement statement = null;
        String sql = null;

        try{
	        sql = JDBCTranslator.INSERT_VIRTUAL_DATABASE;
	        statement = jdbcConnection.prepareStatement(sql);
            statement.setLong(1, ((BasicVirtualDatabaseID)vdb.getID()).getUID());
            statement.setString(2, ((VirtualDatabaseID)vdb.getID()).getVersion());
            statement.setString(3, vdb.getName());
            statement.setString(4, adjustLengthToFit(vdb.getDescription()));
            statement.setString(5, vdb.getGUID());
            statement.setShort(6, vdb.getStatus());
            statement.setString(7, vdb.hasWSDLDefined()? IS_TRUE : IS_FALSE);
            
            statement.setString(8, vdb.getVersionBy());

            statement.setString(9, DateUtil.getDateAsString( vdb.getVersionDate() ));
//            statement.setLong(8, vdb.getVersionDate().getTime());
            statement.setString(10, vdb.getCreatedBy());
            statement.setString(11, DateUtil.getDateAsString( vdb.getCreationDate()) );
//            statement.setLong(10, vdb.getCreationDate().getTime());
            statement.setString(12, vdb.getUpdatedBy());
            statement.setString(13, DateUtil.getDateAsString( vdb.getUpdateDate()) );
//            statement.setLong(12, vdb.getUpdateDate().getTime());
            statement.setString(14, vdb.getFileName());
            if (statement.executeUpdate() != 1) {
                throw new VirtualDatabaseException(ErrorMessageKeys.JDBCC_0023, RuntimeMetadataPlugin.Util.getString(ErrorMessageKeys.JDBCC_0023, vdb.getName() ) );
            }
            LogManager.logTrace(LogRuntimeMetadataConstants.CTX_RUNTIME_METADATA,new Object[]{"Inserted VirtualDatabase with DBID ",new Long(((BasicVirtualDatabaseID)vdb.getID()).getUID())}); //$NON-NLS-1$

        }catch (SQLException se){
            throw new VirtualDatabaseException(se, ErrorMessageKeys.JDBCC_0003, RuntimeMetadataPlugin.Util.getString(ErrorMessageKeys.JDBCC_0003, sql) );
        }finally {
            if ( statement != null ) {
                try {
                    statement.close();
                } catch ( SQLException e ) {
                    I18nLogManager.logError(LogRuntimeMetadataConstants.CTX_RUNTIME_METADATA, ErrorMessageKeys.GEN_0008, e);
                }
            }
        }
    }

    private void insertVDBModels(Collection models, VirtualDatabaseID vdbID) throws VirtualDatabaseException{
        JDBCRuntimeMetadataWriter.insertVDBModels(models, vdbID, jdbcConnection);
    }


    public void deleteVirtualDatabase(VirtualDatabaseID vdbID)throws VirtualDatabaseException{
        //get all model IDs
        this.getModels(vdbID);

        //get models not used by other database (to be deleted)
        Collection modelIDsToBeDeleted = getModelIDsOnlyInVDB(vdbID);

        PreparedStatement statement = null;
        String sql = null;
        long vdbUID = ((BasicVirtualDatabaseID)vdbID).getUID();
        try{
            Iterator iter = modelIDsToBeDeleted.iterator();
            //delete VDB-models
            sql = JDBCTranslator.DELETE_VDB_MODELS;
            statement = jdbcConnection.prepareStatement(sql);
            statement.setLong(1, vdbUID);
            executeStatement(statement);
            statement.close();

            iter = modelIDsToBeDeleted.iterator();
            while(iter.hasNext()){
                long uid = ((BasicModelID)iter.next()).getUID();

                //delete model props
                sql = JDBCTranslator.DELETE_MODEL_PROP_VALS;
	            statement = jdbcConnection.prepareStatement(sql);
                statement.setLong(1, uid);
                executeStatement(statement);
                statement.close();

                sql = JDBCTranslator.DELETE_MODEL_PROP_NMS;
	            statement = jdbcConnection.prepareStatement(sql);
                statement.setLong(1, uid);
                executeStatement(statement);
                statement.close();

                //delete model
                sql = JDBCTranslator.DELETE_MODEL;
	            statement = jdbcConnection.prepareStatement(sql);
                statement.setLong(1, uid);
                executeStatement(statement);
                statement.close();
            }

            //delete VDB
            sql = JDBCTranslator.DELETE_VDB;
            statement = jdbcConnection.prepareStatement(sql);
            statement.setLong(1, vdbUID);
            executeStatement(statement);
            statement.close();


            // each executed is specifically closed therefore
            // must null the statement so that the statement isnt closed again
            // some jdbc drivers throw a NullPointer if close is called more than once
            statement = null;
        }catch (SQLException se){
            throw new VirtualDatabaseException(se, ErrorMessageKeys.JDBCC_0003, RuntimeMetadataPlugin.Util.getString(ErrorMessageKeys.JDBCC_0003, sql) );
        }finally {
            if ( statement != null ) {
                try {
                    statement.close();
                } catch ( SQLException e ) {
                    I18nLogManager.logError(LogRuntimeMetadataConstants.CTX_RUNTIME_METADATA, ErrorMessageKeys.GEN_0008, e);
                }
            }
        }
    }

    public void setConnectorBindingNames(VirtualDatabaseID vdbID, Collection models, Map modelAndCBNames) throws VirtualDatabaseException{
        JDBCRuntimeMetadataWriter.setConnectorBindingNames(vdbID, models, modelAndCBNames, jdbcConnection);
    }

    public void updateVirtualDatabase(VirtualDatabase vdb, String userName) throws VirtualDatabaseException{
        JDBCRuntimeMetadataWriter.updateVirtualDatabase(vdb, userName, jdbcConnection);

    }

    private  VirtualDatabaseID getVirtualDatabaseID(String fullName, String version, boolean isActive) throws VirtualDatabaseDoesNotExistException, VirtualDatabaseException {
        return JDBCRuntimeMetadataReader.getVirtualDatabaseID(fullName, version, isActive, jdbcConnection);
    }

    private void executeStatement(PreparedStatement statement) throws SQLException{
        int lines = statement.executeUpdate();
        try {
            statement.close();
        } catch ( SQLException e ) {
            I18nLogManager.logError(LogRuntimeMetadataConstants.CTX_RUNTIME_METADATA, ErrorMessageKeys.GEN_0008, e);
        }

        LogManager.logTrace(LogRuntimeMetadataConstants.CTX_RUNTIME_METADATA, "" + lines + " rows deleted."); //$NON-NLS-1$ //$NON-NLS-2$
    }

    private String adjustLengthToFit(String oriString){
       	if(oriString != null && oriString.length() > 255){
       		oriString = oriString.substring(0, 255);
       	}
       	return oriString;
    }
}

