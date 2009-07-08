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

package com.metamatrix.server.dqp.service;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.teiid.dqp.internal.cache.DQPContextCache;

import com.google.inject.Inject;
import com.metamatrix.api.exception.MetaMatrixComponentException;
import com.metamatrix.common.application.ApplicationEnvironment;
import com.metamatrix.common.application.exception.ApplicationInitializationException;
import com.metamatrix.common.application.exception.ApplicationLifecycleException;
import com.metamatrix.common.config.CurrentConfiguration;
import com.metamatrix.common.config.ResourceNames;
import com.metamatrix.common.config.api.Configuration;
import com.metamatrix.common.config.api.ServiceComponentDefn;
import com.metamatrix.common.log.LogManager;
import com.metamatrix.common.util.LogCommonConstants;
import com.metamatrix.common.vdb.api.ModelInfo;
import com.metamatrix.common.vdb.api.SystemVdbUtility;
import com.metamatrix.common.vdb.api.VDBArchive;
import com.metamatrix.core.event.EventObjectListener;
import com.metamatrix.core.util.StringUtil;
import com.metamatrix.dqp.service.VDBService;
import com.metamatrix.metadata.runtime.RuntimeMetadataCatalog;
import com.metamatrix.metadata.runtime.api.Model;
import com.metamatrix.metadata.runtime.api.VirtualDatabaseDoesNotExistException;
import com.metamatrix.metadata.runtime.api.VirtualDatabaseException;
import com.metamatrix.metadata.runtime.api.VirtualDatabaseID;
import com.metamatrix.metadata.runtime.event.RuntimeMetadataEvent;
import com.metamatrix.metadata.runtime.event.RuntimeMetadataListener;
import com.metamatrix.server.ServerPlugin;
import com.metamatrix.server.util.LogConstants;
import com.metamatrix.vdb.runtime.VDBKey;

/**
 * Implementation of VDBService used in a "normal" server environment.
 */
public class PlatformVDBService implements VDBService, RuntimeMetadataListener {

    private Map vdbIDs = Collections.synchronizedMap(new HashMap());
    private EventObjectListener listener = null;
    private DQPContextCache contextCache;

    @Inject
    public PlatformVDBService(DQPContextCache cache) {
    	this.contextCache = cache;
    }
    /* 
     * @see com.metamatrix.dqp.service.VDBService#isActiveVDB(java.lang.String, java.lang.String)
     */
    public boolean isActiveVDB(final String vdbName, final String vdbVersion)  throws MetaMatrixComponentException{
        try {
            RuntimeMetadataCatalog.getInstance().getActiveVirtualDatabaseID(vdbName, vdbVersion);
        } catch (VirtualDatabaseDoesNotExistException e) {
            return false;
        } catch (VirtualDatabaseException e) {
            throw new MetaMatrixComponentException(e);
        }
        return true;
    }

    /* 
     * @see com.metamatrix.dqp.service.VDBService#getConnectorBinding(java.lang.String, java.lang.String, java.lang.String)
     */
    public List getConnectorBindingNames(final String vdbName, final String vdbVersion, final String modelName)  throws MetaMatrixComponentException{
        //if it is system model, return runtime metadata resource name
        if(modelName.equalsIgnoreCase(SystemVdbUtility.PHYSICAL_MODEL_NAME) ||
            modelName.equalsIgnoreCase(SystemVdbUtility.ADMIN_PHYSICAL_MODEL_NAME) ){
            List bindings = new ArrayList(1);
            bindings.add(ResourceNames.RUNTIME_METADATA_SERVICE);
            return bindings;
        }
        
        try{
            VirtualDatabaseID vdbID = getVirtualDatabaseID(vdbName, vdbVersion);
            Model model = RuntimeMetadataCatalog.getInstance().getModel(modelName, vdbID);
            if (model == null) {
                throw new MetaMatrixComponentException(ServerPlugin.Util.getString("PlatformVDBService.Model_not_found_in_vdb", new Object[] {modelName, vdbName})); //$NON-NLS-1$
            }

            // return all bindings that could be associated with the model
            return model.getConnectorBindingNames(); 
            
        }catch (VirtualDatabaseException e) {
            throw new MetaMatrixComponentException(e);
        }        
    }

    /* 
     * @see com.metamatrix.dqp.service.VDBService#getModelVisibility(java.lang.String, java.lang.String, java.lang.String)
     */
    public int getModelVisibility(final String vdbName, final String vdbVersion, final String modelName)  throws MetaMatrixComponentException{
        //if it is system physical, return private
        if(modelName.equalsIgnoreCase(SystemVdbUtility.PHYSICAL_MODEL_NAME)  ||
                        modelName.equalsIgnoreCase(SystemVdbUtility.ADMIN_PHYSICAL_MODEL_NAME)) {
            return ModelInfo.PRIVATE;
        }
        
        //if it is other system model, return public
        if(SystemVdbUtility.isSystemModelWithSystemTableType(modelName)){
            return ModelInfo.PUBLIC;
        }
        
        try{
            VirtualDatabaseID vdbID = getVirtualDatabaseID(vdbName, vdbVersion);
            Model model = RuntimeMetadataCatalog.getInstance().getModel(modelName, vdbID);
            if(model == null){
                //could be system models that are private such as
                //SimpleDataType. If the model name is wrong, it should
                //fail at other place
                return ModelInfo.PRIVATE;
            }
            return model.getVisibility();
        }catch (VirtualDatabaseException e) {
            throw new MetaMatrixComponentException(e);
        }     
    }

    /** 
     * @see com.metamatrix.dqp.service.VDBConfiguration#getFileVisibility(java.lang.String, java.lang.String, java.lang.String)
     * @since 4.2
     */
    public int getFileVisibility(final String vdbName, final String vdbVersion, final String pathInVDB) throws MetaMatrixComponentException {
        // get the name of model
    	String modelName = StringUtil.getFirstToken(StringUtil.getLastToken(pathInVDB, "/"), "."); //$NON-NLS-1$ //$NON-NLS-2$


        //return configuration.getModelVisibility(vdbName, vdbVersion, modelName);
        //if it is system physical, return private
        if(StringUtil.endsWithIgnoreCase(modelName, SystemVdbUtility.PHYSICAL_MODEL_NAME) ||
                        StringUtil.endsWithIgnoreCase(modelName, SystemVdbUtility.ADMIN_PHYSICAL_MODEL_NAME) ) {
            return ModelInfo.PRIVATE;
        }

        //if it is other system model, return public
        if(SystemVdbUtility.isSystemModelWithSystemTableType(modelName)){
            return ModelInfo.PUBLIC;
        }
        
        try {
            VirtualDatabaseID vdbID = getVirtualDatabaseID(vdbName, vdbVersion);        
            boolean isVisible = RuntimeMetadataCatalog.getInstance().isVisible(pathInVDB, vdbID);
            if(isVisible) {
                return ModelInfo.PUBLIC;            
            }

            return ModelInfo.PRIVATE;
        }catch (VirtualDatabaseException e) {
            throw new MetaMatrixComponentException(e);
        }
    }

    /* 
     * @see com.metamatrix.common.application.ApplicationService#initialize(java.util.Properties)
     */
    public void initialize(final Properties props) throws ApplicationInitializationException {

    }

    /* 
     * @see com.metamatrix.common.application.ApplicationService#start(com.metamatrix.common.application.ApplicationEnvironment)
     */
    public void start(final ApplicationEnvironment environment) throws ApplicationLifecycleException {
        registerVdbListner();

    }

    /* 
     * @see com.metamatrix.common.application.ApplicationService#stop()
     */
    public void stop() throws ApplicationLifecycleException {
        removeVdbListner();

    }
    
    private VirtualDatabaseID getVirtualDatabaseID(String vdbName, String vdbVersion) throws VirtualDatabaseDoesNotExistException, VirtualDatabaseException{
        
        VDBKey vdbKey = new VDBKey(vdbName, vdbVersion);
        VirtualDatabaseID vdbID = null;
        if((vdbID = (VirtualDatabaseID)this.vdbIDs.get(vdbKey)) == null){
            vdbID = RuntimeMetadataCatalog.getInstance().getActiveVirtualDatabaseID(vdbName, vdbVersion);
            this.vdbIDs.put(vdbKey, vdbID);
        }
        return vdbID;
    }
    
    private void removeVirtualDatabaseID(VirtualDatabaseID vdbID) {
        VDBKey vdbKey = new VDBKey(vdbID.getName(), vdbID.getVersion());
        LogManager.logTrace(LogConstants.CTX_QUERY_SERVICE, new Object[] {"PlatformVDBService removing vdb ", vdbKey}); //$NON-NLS-1$
        this.vdbIDs.remove(vdbKey);
    }
    
    /*
     * @see com.metamatrix.dqp.service.VDBService#getConnectorName(java.lang.String)
     */
    public String getConnectorName(String connectorBindingID) throws MetaMatrixComponentException {
        
        Configuration operational = CurrentConfiguration.getInstance().getConfiguration();
        
        ServiceComponentDefn bindingName = operational.getConnectorBindingByRoutingID(connectorBindingID);
        
        if(bindingName != null) {
            return bindingName.toString();
        }
        // bindingName could not be fetched.
        return "UNKNOWN"; //$NON-NLS-1$
    }
    /** 
     * @see com.metamatrix.metadata.runtime.event.RuntimeMetadataListener#processEvent(com.metamatrix.metadata.runtime.event.RuntimeMetadataEvent)
     * @since 4.2
     */
    public void processEvent(RuntimeMetadataEvent event) {
        if(event.deleteVDB() ){
            VirtualDatabaseID vdbID = event.getVirtualDatabaseID();
            // update the cache for this vdb
            removeVirtualDatabaseID(vdbID);

            // remove any cached items
            this.contextCache.removeVDBScopedCache(vdbID.getName(), vdbID.getVersion());
        }
    }    
    
    /**
     * Register a VDBListner created from a IndexMetadataService
     * @param indexService
     * @since 4.2
     */
    private void registerVdbListner() {
        try{
            listener = RuntimeMetadataCatalog.getInstance().registerRuntimeMetadataListener(this);
        }catch(Exception e){
        	LogManager.logError(LogCommonConstants.CTX_SERVICE, e, ServerPlugin.Util.getString("PlatformVDBService.0")); //$NON-NLS-1$
        }        
    }
    
    /**
     * Register a VDBListner created from a IndexMetadataService
     * @param indexService
     * @since 4.2
     */
    private void removeVdbListner() {
        try{
            if (listener != null) {
                RuntimeMetadataCatalog.getInstance().removeRuntimeMetadataListener(listener);
            }
        }catch(Exception e){
        	LogManager.logError(LogCommonConstants.CTX_SERVICE, e, ServerPlugin.Util.getString("PlatformVDBService.1")); //$NON-NLS-1$
        }        
    }    

    /** 
     * @see com.metamatrix.dqp.service.VDBConfiguration#getMultiSourceModels(java.lang.String, java.lang.String)
     * @since 4.2
     */
    public List getMultiSourceModels(String vdbName,
                                     String vdbVersion) throws MetaMatrixComponentException {
        
        // TODO: implement this correctly to find and return multi-source models.  It would be 
        // really, really good if this info could be cached somewhere (probably in RuntimeMetadataCatalog)
        // as this method will be called to determine whether to do a bunch of extra processing. 
        try {
            VirtualDatabaseID vdbID = getVirtualDatabaseID(vdbName, vdbVersion);
            return RuntimeMetadataCatalog.getInstance().getMutiSourcedModels(vdbID);
        } catch (VirtualDatabaseException e) {
            throw new MetaMatrixComponentException(e);
        }
       
    }

    /** 
     * @see com.metamatrix.dqp.service.VDBService#getVDBResource(java.lang.String, java.lang.String)
     * @since 4.3
     */
    public InputStream getVDBResource(String vdbName, String vdbVersion) 
        throws MetaMatrixComponentException {        
        try {
            VirtualDatabaseID vdbID = getVirtualDatabaseID(vdbName, vdbVersion);
            final byte[] bytes = RuntimeMetadataCatalog.getInstance().getVDBArchive(vdbID);
            return new ByteArrayInputStream(bytes);
        } catch (VirtualDatabaseDoesNotExistException e) {
            throw new MetaMatrixComponentException(e);
        } catch (VirtualDatabaseException e) {
            throw new MetaMatrixComponentException(e);
        }
    }

    /** 
     * @see com.metamatrix.dqp.service.VDBService#getVDBStatus(java.lang.String, java.lang.String)
     * @since 4.3
     */
    public int getVDBStatus(String vdbName, String vdbVersion) 
        throws MetaMatrixComponentException {
        throw new UnsupportedOperationException();
    }

    /** 
     * @see com.metamatrix.dqp.service.VDBService#changeVDBStatus(java.lang.String, java.lang.String, int)
     * @since 4.3
     */
    public void changeVDBStatus(String vdbName,String vdbVersion,int status) 
        throws ApplicationLifecycleException, MetaMatrixComponentException {
        throw new UnsupportedOperationException();
    }

    /** 
     * @see com.metamatrix.dqp.service.VDBService#getAvailableVDBs()
     * @since 4.3
     */
    public List getAvailableVDBs() throws MetaMatrixComponentException {
        throw new UnsupportedOperationException();
    }
    
    @Override
    public String getActiveVDBVersion(String vdbName, String vdbVersion)
    		throws MetaMatrixComponentException, VirtualDatabaseException {
    	throw new UnsupportedOperationException();
    }
    
    @Override
    public VDBArchive getVDB(String vdbName, String vdbVersion)
    		throws MetaMatrixComponentException {
    	throw new UnsupportedOperationException();
    }
    
}
