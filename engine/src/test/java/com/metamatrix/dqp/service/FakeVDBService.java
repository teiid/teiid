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

package com.metamatrix.dqp.service;

import java.io.InputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import com.metamatrix.api.exception.MetaMatrixComponentException;
import com.metamatrix.common.application.ApplicationEnvironment;
import com.metamatrix.common.application.exception.ApplicationInitializationException;
import com.metamatrix.common.application.exception.ApplicationLifecycleException;
import com.metamatrix.common.vdb.api.ModelInfo;
import com.metamatrix.common.vdb.api.VDBArchive;

/**
 */
public class FakeVDBService extends FakeAbstractService implements VDBService {
    private Map vdbsMap = new HashMap();    // VdbInfo -> Map<Model name (upper), <ModelInfo>>
    private Map bindingNames = new HashMap();   // binding UUID -> binding name
    
    private static class VdbInfo {
        private final String vdbName;
        private final String vdbVersion;
        
        private final String key;
        
        public VdbInfo(String name, String version) {
            this.vdbName = name;
            this.vdbVersion = version;
            this.key = name.toUpperCase() + ":" + version; //$NON-NLS-1$
        }
        
        public String getName() {
            return this.vdbName;
        }
        
        public String getVersion() {
            return this.vdbVersion;
        }
        
        public boolean equals(Object obj) {
            if(obj == null) {
                return false;
            } else if(obj == this) {
                return true;
            } else {
                return this.key.equals(((VdbInfo)obj).key); 
            }
        }
        
        public int hashCode() {
            return this.key.hashCode();
        }
        
        public String toString() {
            return vdbName + ":" + vdbVersion; //$NON-NLS-1$
        }
    }
    
    private static class FakeModel {
        String modelName;
        boolean multiSource = false;
        int visibility = ModelInfo.PUBLIC;;
        List bindingNames = new ArrayList();    // mapped to UUIDs
        List bindingUUIDs = new ArrayList();    // mapped to names
    }
    
    /**
     * Method for testing - add a model with the specified properties.  The vdb will be created 
     * automatically under the hood. 
     * @param vdbName vdb name
     * @param version vdb version
     * @param modelName model name
     * @param visibility MODEL_PUBLIC or MODEL_PRIVATE
     * @param multiSource true if model is multi-source
     * @since 4.2
     */
    public void addModel(String vdbName, String version, String modelName, int visibility, boolean multiSource) {
        FakeModel model = new FakeModel();
        model.visibility = visibility;
        model.multiSource = multiSource;
        model.modelName = modelName;
        
        VdbInfo vdb = new VdbInfo(vdbName, version);
        Map vdbModels = (Map)this.vdbsMap.get(vdb);
        if(vdbModels == null) {
            vdbModels = new HashMap();
            vdbsMap.put(vdb, vdbModels);
        }
        vdbModels.put(modelName.toUpperCase(), model);
    }   

    /**
     * Method for testing - add a binding to a model with the specified names.  If the model does not
     * exist, it will be created automatically with default properties.
     * 
     * @param vdbName vdb name
     * @param version vdb version
     * @param model
     * @param connectorBindingID
     * @param connectorBindingName
     * @since 4.2
     */
    public void addBinding(String vdbName, String version, String modelName, String connectorBindingID, String connectorBindingName) {
        FakeModel model = null;
        
        // Find existing model
        VdbInfo vdb = new VdbInfo(vdbName, version);
        Map vdbModels = (Map)this.vdbsMap.get(vdb);
        if(vdbModels != null) {
            model = (FakeModel) vdbModels.get(modelName.toUpperCase());
        }

        // If model hasn't been added yet, add it with defaults
        if(model == null) {
            addModel(vdbName, version, modelName, ModelInfo.PUBLIC, false);
            
            // Re-lookup
            vdbModels = (Map)this.vdbsMap.get(vdb);
            model = (FakeModel) vdbModels.get(modelName.toUpperCase());            
        }
        
        // Add binding to model
        model.bindingNames.add(connectorBindingName);
        model.bindingUUIDs.add(connectorBindingID);
        
        // Add uuid to name mapping
        this.bindingNames.put(connectorBindingID, connectorBindingName);               
    }
    
    /* (non-Javadoc)
     * @see com.metamatrix.dqp.service.VDBService#isActiveVDB(java.lang.String, java.lang.String)
     */
    public boolean isActiveVDB(String vdbName, String vdbVersion) {
        return vdbsMap.containsKey(new VdbInfo(vdbName, vdbVersion));
    }

    /* (non-Javadoc)
     * @see com.metamatrix.dqp.service.VDBService#getConnectorBinding(java.lang.String, java.lang.String, java.lang.String)
     */
    public List getConnectorBindingNames(String vdbName, String vdbVersion, String modelName) {
        VdbInfo vdb = new VdbInfo(vdbName, vdbVersion);
        Map vdbModels = (Map)this.vdbsMap.get(vdb);
        if(vdbModels != null) {
            FakeModel model = (FakeModel) vdbModels.get(modelName.toUpperCase());
            return model.bindingUUIDs;
        }
        
        return Collections.EMPTY_LIST;
    }
    
    /*
     *  (non-Javadoc)
     * @see com.metamatrix.dqp.service.VDBService#getConnectorName(java.lang.String)
     */
    public String getConnectorName(String connectorBindingID) {
        return (String) this.bindingNames.get(connectorBindingID);
    }
    
    /* (non-Javadoc)
     * @see com.metamatrix.dqp.service.VDBService#getModelVisibility(java.lang.String, java.lang.String, java.lang.String)
     */
    public int getModelVisibility(String vdbName, String vdbVersion, String modelName) {
        VdbInfo vdb = new VdbInfo(vdbName, vdbVersion);
        Map vdbModels = (Map)this.vdbsMap.get(vdb);
        if(vdbModels != null) {
            FakeModel model = (FakeModel) vdbModels.get(modelName.toUpperCase());
            if(model != null) {
                return model.visibility;
            } 
        }
        
        return ModelInfo.PUBLIC;
    }
    
    /** 
     * @see com.metamatrix.dqp.service.VDBConfiguration#getFileVisibility(java.lang.String, java.lang.String, java.lang.String)
     * @since 4.2
     */
    public int getFileVisibility(String vdbName, String vdbVersion, String pathInVDB) throws MetaMatrixComponentException {
        return ModelInfo.PUBLIC;
    }

    /* (non-Javadoc)
     * @see com.metamatrix.dqp.service.VDBService#getVDBResourceFile(java.lang.String, java.lang.String)
     */
    public String getVDBResourceFile(String vdbName, String vdbVersion) {
        return null;
    }

    /* (non-Javadoc)
     * @see com.metamatrix.common.application.ApplicationService#initialize(java.util.Properties)
     */
    public void initialize(Properties props) throws ApplicationInitializationException {
//        vdbsMap = props;
    }

    /* (non-Javadoc)
     * @see com.metamatrix.common.application.ApplicationService#start(com.metamatrix.common.application.ApplicationEnvironment)
     */
    public void start(ApplicationEnvironment environment) throws ApplicationLifecycleException {
        // TODO Auto-generated method stub

    }

    /* (non-Javadoc)
     * @see com.metamatrix.common.application.ApplicationService#stop()
     */
    public void stop() throws ApplicationLifecycleException {
        // TODO Auto-generated method stub

    }

    public List getMultiSourceModels(String vdbName,
                                     String vdbVersion) throws MetaMatrixComponentException {

        VdbInfo vdb = new VdbInfo(vdbName, vdbVersion);
        Map vdbModels = (Map)this.vdbsMap.get(vdb);
        if(vdbModels != null) {
            List multiModels = new ArrayList();
            
            Iterator modelIter = vdbModels.values().iterator();
            while(modelIter.hasNext()) {
                FakeModel model = (FakeModel) modelIter.next();
                if(model.multiSource) {
                    multiModels.add(model.modelName);
                }
            }
            
            return multiModels;
        }
        
        return Collections.EMPTY_LIST;
    }

    /** 
     * @see com.metamatrix.dqp.service.VDBService#getVDBResource(java.lang.String, java.lang.String)
     * @since 4.3
     */
    public InputStream getVDBResource(String vdbName,
                                 String vdbVersion) throws MetaMatrixComponentException {
        return null;
    }

    /** 
     * @see com.metamatrix.dqp.service.VDBService#getVDBStatus(java.lang.String, java.lang.String)
     * @since 4.3
     */
    public int getVDBStatus(String vdbName,
                            String vdbVersion) throws MetaMatrixComponentException {
        return 0;
    }

    /** 
     * @see com.metamatrix.dqp.service.VDBService#changeVDBStatus(java.lang.String, java.lang.String, int)
     * @since 4.3
     */
    public void changeVDBStatus(String vdbName,String vdbVersion,int status) throws ApplicationLifecycleException,MetaMatrixComponentException {
    }

    /** 
     * @see com.metamatrix.dqp.service.VDBService#getAvailableVDBs()
     * @since 4.3
     */
    public List getAvailableVDBs() throws MetaMatrixComponentException {
        return null;
    }
    
    @Override
    public String getActiveVDBVersion(String vdbName, String vdbVersion) {
    	throw new UnsupportedOperationException();
    }
    
    @Override
    public VDBArchive getVDB(String vdbName, String vdbVersion)
    		throws MetaMatrixComponentException {
    	throw new UnsupportedOperationException();
    }

}
