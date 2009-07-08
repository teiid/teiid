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

package com.metamatrix.metadata.runtime.model;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Collection;
import java.util.Date;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.teiid.metadata.RuntimeMetadataPlugin;

import com.metamatrix.api.exception.MetaMatrixComponentException;
import com.metamatrix.common.connection.ManagedConnectionException;
import com.metamatrix.common.connection.TransactionMgr;
import com.metamatrix.common.extensionmodule.ExtensionModuleManager;
import com.metamatrix.common.extensionmodule.ExtensionModuleTypes;
import com.metamatrix.common.extensionmodule.exception.DuplicateExtensionModuleException;
import com.metamatrix.common.extensionmodule.exception.ExtensionModuleNotFoundException;
import com.metamatrix.common.extensionmodule.exception.InvalidExtensionModuleTypeException;
import com.metamatrix.common.id.dbid.DBIDGenerator;
import com.metamatrix.common.id.dbid.DBIDGeneratorException;
import com.metamatrix.common.log.I18nLogManager;
import com.metamatrix.common.log.LogManager;
import com.metamatrix.common.vdb.api.ModelInfo;
import com.metamatrix.common.vdb.api.VDBArchive;
import com.metamatrix.common.vdb.api.VDBDefn;
import com.metamatrix.core.util.DateUtil;
import com.metamatrix.core.util.FileUtils;
import com.metamatrix.core.vdb.VDBStatus;
import com.metamatrix.metadata.runtime.RuntimeMetadataCatalog;
import com.metamatrix.metadata.runtime.api.Model;
import com.metamatrix.metadata.runtime.api.ModelID;
import com.metamatrix.metadata.runtime.api.VirtualDatabase;
import com.metamatrix.metadata.runtime.api.VirtualDatabaseException;
import com.metamatrix.metadata.runtime.api.VirtualDatabaseID;
import com.metamatrix.metadata.runtime.exception.InvalidStateException;
import com.metamatrix.metadata.runtime.spi.MetaBaseConnector;
import com.metamatrix.metadata.runtime.util.LogRuntimeMetadataConstants;
import com.metamatrix.metadata.util.ErrorMessageKeys;
import com.metamatrix.metadata.util.LogMessageKeys;

/**
 * The UpdateController is responsible for controlling all updates for the Runtime Metadata.  It manages one connection for all updates to pass through.
 */
public class UpdateController {
    public static final String FIRST_VERSION = "1"; //$NON-NLS-1$

    private TransactionMgr transMgr;

    public UpdateController(TransactionMgr transactionMgr){
            transMgr = transactionMgr;
    }



/**
 * Updates the <code>VirtualDatabase</code> status.
 * @param virtualID representss the VirtualDatabase to be updated
 * @param userName of the person requesting the change
 * @exception RuntimeMetadataException if unable to perform update.
 * @exception InvalidStateException if the status to be set is invalid.
 * @param status is the state the VirtualDatabase should be set to
 */
    public synchronized void setVBDStatus(VirtualDatabaseID virtualID, short status, String userName) throws InvalidStateException, VirtualDatabaseException  {
        setVDBStatus(virtualID, status, userName, false);
    }

    private void setVDBStatus(VirtualDatabaseID virtualID, short status, String userName, boolean checkCompletion) throws InvalidStateException, VirtualDatabaseException  {
        MetaBaseConnector conn = null;
        try{
            conn= getReadTransaction();
            VirtualDatabase vdb = conn.getVirtualDatabase(virtualID);
            if(vdb.getStatus() == VDBStatus.INCOMPLETE
                && status != VDBStatus.DELETED && checkCompletion)
                throw new InvalidStateException(ErrorMessageKeys.UC_0007, RuntimeMetadataPlugin.Util.getString(ErrorMessageKeys.UC_0007) );
        }catch(ManagedConnectionException e){
            throw new VirtualDatabaseException(e, ErrorMessageKeys.UC_0008, RuntimeMetadataPlugin.Util.getString(ErrorMessageKeys.UC_0008) );
        }finally {
            if (conn != null ) {
                try {
                    conn.close();
                } catch (Exception e2) {
                    I18nLogManager.logError(LogRuntimeMetadataConstants.CTX_RUNTIME_METADATA, ErrorMessageKeys.GEN_0001, e2);
                }
            }
        }

        try{
            conn = getWriteTransaction();
            conn.setStatus(virtualID, status, userName);
            conn.commit();
        }catch(ManagedConnectionException e){
            try {
                if ( conn != null ) {
                    conn.rollback();         // rollback the transaction
                }
            }catch (Exception e2 ) {
                I18nLogManager.logError(LogRuntimeMetadataConstants.CTX_RUNTIME_METADATA, ErrorMessageKeys.GEN_0006, e2);
            }
            
            throw new VirtualDatabaseException(e, ErrorMessageKeys.UC_0008, RuntimeMetadataPlugin.Util.getString(ErrorMessageKeys.UC_0008) );
        }catch(VirtualDatabaseException e){
            try {
                if ( conn != null ) {
                    conn.rollback();         // rollback the transaction
                }
            }catch (Exception e2 ) {
                I18nLogManager.logError(LogRuntimeMetadataConstants.CTX_RUNTIME_METADATA, ErrorMessageKeys.GEN_0006, e2);
            }
            throw e;
        }finally {
            if ( conn != null ) {
                try {
                    conn.close();
                } catch ( Exception e3 ) {
                    I18nLogManager.logError(LogRuntimeMetadataConstants.CTX_RUNTIME_METADATA, ErrorMessageKeys.GEN_0001, e3);
                }
            }
        }
    }

    public synchronized void deleteVirtualDatabase(VirtualDatabaseID vdbID)throws InvalidStateException, VirtualDatabaseException  {
        MetaBaseConnector conn = null;
        VirtualDatabase vdb = null;
        try{
            
            
            conn = getWriteTransaction();
            vdb = conn.getVirtualDatabase(vdbID);
            
            Collection vdbs = conn.getDeletedVirtualDatabaseIDs();
            if(!vdbs.contains(vdbID)){
                throw new InvalidStateException(ErrorMessageKeys.UC_0011, RuntimeMetadataPlugin.Util.getString(ErrorMessageKeys.UC_0011, vdbID) );
            }
                     
            conn.deleteVirtualDatabase(vdbID);
            conn.commit();
           
            
        }catch(ManagedConnectionException e){
            try {
                if ( conn != null ) {
                    conn.rollback();         // rollback the transaction
                }
            }catch (Exception e2 ) {
                I18nLogManager.logError(LogRuntimeMetadataConstants.CTX_RUNTIME_METADATA, ErrorMessageKeys.GEN_0006, e2);
            }
            
            throw new VirtualDatabaseException(e, ErrorMessageKeys.UC_0012, RuntimeMetadataPlugin.Util.getString(ErrorMessageKeys.UC_0012) );
        }catch(VirtualDatabaseException e){
            try {
                if ( conn != null ) {
                    conn.rollback();         // rollback the transaction
                }
            }catch (Exception e2 ) {
                I18nLogManager.logError(LogRuntimeMetadataConstants.CTX_RUNTIME_METADATA, ErrorMessageKeys.GEN_0006, e2);
            }
            throw e;
        }finally {
            if ( conn != null ) {
                try {
                    conn.close();
                } catch ( Exception e3 ) {
                    I18nLogManager.logError(LogRuntimeMetadataConstants.CTX_RUNTIME_METADATA, ErrorMessageKeys.GEN_0001, e3);
                }
            }
        }
        
        if (vdb != null) {

            ExtensionModuleManager extension = ExtensionModuleManager.getInstance();
            try {
                extension.removeSource("RuntimeMetadata", vdb.getFileName());
                    
            } catch (MetaMatrixComponentException e1) {
                throw new VirtualDatabaseException(e1);
            } catch (ExtensionModuleNotFoundException e) {
                throw new VirtualDatabaseException(e);
                    
            }  
        }                   
    }

    /**
     * Set connector binding names for models in a virtual database. If the names
     * are set for all the models, the virtual database status is changed to Inactive.
     * @param vdbID is the VirtualDatabaseID
     * @param modelAndCBNames contains ModelID and connector binding name pare.
     * @throws VirtualDatabaseException an error occurs while trying to read the data.
     */
    public void setConnectorBindingNames(VirtualDatabaseID vdbID, Map modelAndCBNames, String userName)throws VirtualDatabaseException{
		LogManager.logTrace(LogRuntimeMetadataConstants.CTX_RUNTIME_METADATA, new Object[]{"Setting connector binding names for", vdbID}); //$NON-NLS-1$

       MetaBaseConnector conn = null;

        boolean isCompleteSet = true;
        Collection models = RuntimeMetadataCatalog.getInstance().getModels(vdbID);
        Collection modelIDs = modelAndCBNames.keySet(); // BasicModelIDs
        Collection cbNames = modelAndCBNames.values();
        
        Iterator namesIter = cbNames.iterator();
        while( namesIter.hasNext()) {
            List mmuuid = (List) namesIter.next();
            if( mmuuid.size() == 0) {
                isCompleteSet = false;
            }
        }

        Iterator iter = models.iterator();
        while(iter.hasNext()){
            Model model = (Model)iter.next();
            if ( model.isPhysical() && (model.requireConnectorBinding()) ) {
                ModelID modelID = (ModelID)model.getID();
                if(! modelIDs.contains(modelID)){
                    isCompleteSet = false;
                    modelAndCBNames.put(modelID, null);
                }
            }
        }

        try{
            conn = getWriteTransaction();
            conn.setConnectorBindingNames(vdbID, models, modelAndCBNames);
            conn.commit();
            if(isCompleteSet){
                setVDBStatus(vdbID, VDBStatus.INACTIVE, userName, false);
            }else{
                setVDBStatus(vdbID, VDBStatus.INCOMPLETE, userName, false);
            }
        }catch(ManagedConnectionException e){
            try {
                if ( conn != null ) {
                    conn.rollback();         // rollback the transaction
                }
            }catch (Exception e2 ) {
                I18nLogManager.logError(LogRuntimeMetadataConstants.CTX_RUNTIME_METADATA, ErrorMessageKeys.GEN_0006, e2);
            }
            
            throw new VirtualDatabaseException(e, ErrorMessageKeys.UC_0015, RuntimeMetadataPlugin.Util.getString(ErrorMessageKeys.UC_0015) );
        }finally {
            if ( conn != null ) {
                try {
                    conn.close();
                } catch ( Exception e2 ) {
                    I18nLogManager.logError(LogRuntimeMetadataConstants.CTX_RUNTIME_METADATA, ErrorMessageKeys.GEN_0001, e2);
                }
            }
        }

        LogManager.logTrace(LogRuntimeMetadataConstants.CTX_RUNTIME_METADATA, new Object[]{"End setting connector binding names for", vdbID}); //$NON-NLS-1$
    }

    /**
     * Update VDB attributes. Only the attributes defined in <code>VirtualDatabase.ModifiableAttributes</code>
     * can be modefied. Call VirtualDatabase.update(String attribute, Object value)
     * to update each attribute of the VDB before calling this method.
     * @param vdb VDB to be updated.
     * @param userName of the person updating the virtual database.
     */
    public void updateVirtualDatabase(VirtualDatabase vdb, String userName)throws VirtualDatabaseException{
        MetaBaseConnector conn= null;

        try{
            conn = getReadTransaction();
            conn.updateVirtualDatabase(vdb, userName);
            conn.commit();
        }catch(ManagedConnectionException e){
            try {
                if ( conn != null ) {
                    conn.rollback();         // rollback the transaction
                }
            }catch (Exception e2 ) {
                I18nLogManager.logError(LogRuntimeMetadataConstants.CTX_RUNTIME_METADATA, ErrorMessageKeys.GEN_0006, e2);
            }
            
            throw new VirtualDatabaseException(e, ErrorMessageKeys.UC_0016, RuntimeMetadataPlugin.Util.getString(ErrorMessageKeys.UC_0016) );
        }finally {
            if ( conn != null ) {
                try {
                    conn.close();
                } catch ( Exception e2 ) {
                    I18nLogManager.logError(LogRuntimeMetadataConstants.CTX_RUNTIME_METADATA, ErrorMessageKeys.GEN_0001, e2);
                }
            }
        }
    }


    protected MetaBaseConnector getReadTransaction() throws ManagedConnectionException {
        return (MetaBaseConnector) this.transMgr.getReadTransaction();
    }

    protected MetaBaseConnector getWriteTransaction() throws ManagedConnectionException {
        return (MetaBaseConnector) this.transMgr.getWriteTransaction();
    }


    /**
     * Create a new instance of a VirtualDatabase object using the supplied information.
     * @param vdbID the MetadataID of the virtual database
     * @param vdbUUID the UUID for the virtual database (typically matches the project's UUID)
     * @param vdbName the name for the new VDB
     * @param userName the name of the user that is creating the VDB
     * @param description the description for the VDB
     * @param inMemory true if the VirtualDatabase is to be used only in memory, or false
     * if VirtualDatabase instances are persisted in a repository.  If set to false,
     * then this method ensures that the version number of the VDB will be incremented
     * over what is already in the persistent store.
     * @return the new VirtualDatabase object
     * @throws VirtualDatabaseException if there is an error
     */
    protected BasicVirtualDatabase buildVirtualDatabaseObject( final VDBArchive vdbArchive,
                                                               final String userName,
                                                               final boolean inMemory )
                                       throws VirtualDatabaseException {
        
        String createdBy = userName;
        Date creationDate = new Date();
        String vdbVersion=FIRST_VERSION;
        
        VDBDefn vdbInfo = vdbArchive.getConfigurationDef();
        
        BasicVirtualDatabaseID vdbID = null;
        try {
			vdbID = (BasicVirtualDatabaseID) this.getReadTransaction().getVirtualDatabaseID(vdbInfo.getName(), null);
		} catch (ManagedConnectionException e) {
			throw new VirtualDatabaseException(e);
		}
        if (vdbID != null) {
            vdbVersion = Integer.toString(Integer.parseInt(vdbID.getVersion()) + 1);
            VirtualDatabase latestVdb = RuntimeMetadataCatalog.getInstance().getVirtualDatabase(vdbID);
            createdBy = latestVdb.getCreatedBy();
            creationDate = latestVdb.getCreationDate();
        }

        
        vdbID = new BasicVirtualDatabaseID(vdbInfo.getName(), vdbVersion); 
       

        String fileName = vdbInfo.getName().trim() + "_" + vdbVersion.trim() + ".vdb";  //$NON-NLS-1$ //$NON-NLS-2$
        
        ExtensionModuleManager extension = ExtensionModuleManager.getInstance();
        try {
            if (extension.isSourceInUse(fileName)) {
                throw new VirtualDatabaseException(RuntimeMetadataPlugin.Util.getString("UpdateController.VDB_File_already_exist_in_extension_modules_25", fileName)); //$NON-NLS-1$
               
            }
            
        } catch (MetaMatrixComponentException e1) {
            throw new VirtualDatabaseException(e1);
        } 
        
        
        final long id = createVirtualDatabaseID();
        
        String vdbUUID = vdbInfo.getUUID();
        String description = vdbInfo.getDescription();
        vdbID.setUID(id);
        BasicVirtualDatabase vdb = new BasicVirtualDatabase(vdbID);
        vdb.setDescription(description);
        vdb.setGUID(vdbUUID);
        vdb.setStatus(VDBStatus.INCOMPLETE);
        vdb.setVersionBy(userName);
        vdb.setVersionDate(new Date());
        vdb.setCreatedBy(createdBy);
        vdb.setCreationDate(creationDate);
        vdb.setUpdatedBy(userName);
        vdb.setUpdateDate(new Date());
        
        
        
        vdb.setFileName(fileName);
        vdb.setHasWSDLDefined(vdbInfo.hasWSDLDefined());        

        return vdb;
    }

    protected long createVirtualDatabaseID() throws VirtualDatabaseException {
        // Create the ID for the Virtual Database ...
        long id = 0;
        try{
            id = DBIDGenerator.getInstance().getID("VirtualDatabases"); //$NON-NLS-1$
        }catch(DBIDGeneratorException e){
            throw new VirtualDatabaseException(e, ErrorMessageKeys.GEN_0004, RuntimeMetadataPlugin.Util.getString(ErrorMessageKeys.GEN_0004) );
        }
        return id;
    }

    /**
     * @param vdbName
     * @param dtcInfo
     * @param userName
     * @param vdbIndexFile
     * @param vdm
     * @return
     */
    public VirtualDatabase createVirtualDatabase(VDBArchive vdbArchive, String userName) throws VirtualDatabaseException {
        // -------------------------------------
        // Create the VirtualDatabase object ...
        // -------------------------------------
         

        BasicVirtualDatabase vdb = buildVirtualDatabaseObject(vdbArchive, userName,false);
        
        BasicVirtualDatabaseID vdbID = (BasicVirtualDatabaseID)  vdb.getVirtualDatabaseID();

        VDBDefn vdbInfo = vdbArchive.getConfigurationDef();
        
        Collection vdbModels = new HashSet();
        Iterator iter = vdbInfo.getModels().iterator();
        while(iter.hasNext()){
            ModelInfo mInfo = (ModelInfo)iter.next();
            BasicModelID modelID = new BasicModelID(mInfo.getName(), mInfo.getVersion(), getNextModelUid());
            Date versionDate = (mInfo.getDateVersioned() != null)?mInfo.getDateVersioned():new Date();
            modelID.setVersionDate(DateUtil.getDateAsString(versionDate));
            modelID.setUuid(mInfo.getUUID());
            BasicModel model = new BasicModel(modelID, vdbID, mInfo);
            vdbModels.add(model);
            vdb.addModelID(modelID);
            
        }

        Collection vdbs = new HashSet(1);
        vdbs.add(vdb);
        //write into database
        MetaBaseConnector conn = null;
        try{
            conn = getWriteTransaction();
            conn.insertVirtualDatabase(vdb);

            conn.insertModels(vdbModels, vdbID);

            conn.commit();

        }catch(ManagedConnectionException e){
            try {
                if ( conn != null ) {
                    conn.rollback();         // rollback the transaction
                }
            }catch (Exception e2 ) {
                I18nLogManager.logError(LogRuntimeMetadataConstants.CTX_RUNTIME_METADATA, ErrorMessageKeys.GEN_0006, e2);
            }
            throw new VirtualDatabaseException(e, ErrorMessageKeys.UC_0005, RuntimeMetadataPlugin.Util.getString(ErrorMessageKeys.UC_0005) );
        }catch(VirtualDatabaseException e){
            try {
                if ( conn != null ) {
                    conn.rollback();         // rollback the transaction
                }
            }catch (Exception e2 ) {
                I18nLogManager.logError(LogRuntimeMetadataConstants.CTX_RUNTIME_METADATA, ErrorMessageKeys.GEN_0006, e2);
            }
            throw e;
        }finally {
            if ( conn != null ) {
                try {
                    conn.close();
                } catch ( Exception e3 ) {
                    I18nLogManager.logError(LogRuntimeMetadataConstants.CTX_RUNTIME_METADATA, ErrorMessageKeys.GEN_0001, e3);
                }
            }
        }

        try {         
                
            ExtensionModuleManager extension = ExtensionModuleManager.getInstance();

            extension.addSource(userName, ExtensionModuleTypes.VDB_FILE_TYPE, vdb.getFileName(), VDBArchive.writeToByteArray(vdbArchive), vdbInfo.getDescription(), true); //$NON-NLS-1$
        } catch (MetaMatrixComponentException e1) {
            throw new VirtualDatabaseException(e1);
        } catch (DuplicateExtensionModuleException e1) {
            throw new VirtualDatabaseException(e1);
        } catch (InvalidExtensionModuleTypeException e1) {
            throw new VirtualDatabaseException(e1);
        }

        I18nLogManager.logInfo(LogRuntimeMetadataConstants.CTX_RUNTIME_METADATA, LogMessageKeys.UC_0002, new Object[]{vdb});
        return vdb;
    }
        
    public byte[] getVDBArchive(String fileName) throws VirtualDatabaseException {
        byte[] archive = null;
        ExtensionModuleManager extension = ExtensionModuleManager.getInstance();
            try {
                if (extension.isSourceInUse(fileName)) {
                	archive = extension.getSource(fileName);                    
                } else {
					try {
						InputStream in = ClassLoader.getSystemResourceAsStream(fileName);
						if (in != null) {
							ByteArrayOutputStream byteStream = new ByteArrayOutputStream(100*1024);
							FileUtils.write(in, byteStream, 10*1024);
							archive = byteStream.toByteArray();
						}
					} catch (IOException e) {
						// it will error below now.
					}
                }
                if (archive == null) {
                	throw new VirtualDatabaseException(RuntimeMetadataPlugin.Util.getString("UpdateController.VDB_File_does_not_exist_in_extension_modules_1",fileName)); //$NON-NLS-1$
                }
            } catch (ExtensionModuleNotFoundException e) {
                throw new VirtualDatabaseException(e);
            } catch (VirtualDatabaseException e) {
                throw e;               
            } catch (MetaMatrixComponentException e) {
                throw new VirtualDatabaseException(e);
                
            }
        return archive;
    }


    private long getNextModelUid() throws VirtualDatabaseException {
        long uid = 0;
        try {
            uid = DBIDGenerator.getInstance().getID("model"); //$NON-NLS-1$
        } catch (DBIDGeneratorException e) {
            throw new VirtualDatabaseException(e, ErrorMessageKeys.GEN_0004, RuntimeMetadataPlugin.Util.getString(ErrorMessageKeys.GEN_0004) );
        }
        return uid;
    }
}

