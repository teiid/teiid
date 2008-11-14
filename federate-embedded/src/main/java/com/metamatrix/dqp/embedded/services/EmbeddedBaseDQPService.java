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

package com.metamatrix.dqp.embedded.services;

import java.util.Collection;
import java.util.Iterator;
import java.util.Properties;

import com.metamatrix.api.exception.MetaMatrixComponentException;
import com.metamatrix.common.application.ApplicationEnvironment;
import com.metamatrix.common.application.ApplicationService;
import com.metamatrix.common.application.exception.ApplicationInitializationException;
import com.metamatrix.common.application.exception.ApplicationLifecycleException;
import com.metamatrix.common.config.api.ConnectorBinding;
import com.metamatrix.common.vdb.api.ModelInfo;
import com.metamatrix.common.vdb.api.VDBArchive;
import com.metamatrix.common.vdb.api.VDBDefn;
import com.metamatrix.core.CoreConstants;
import com.metamatrix.dqp.embedded.DQPEmbeddedPlugin;
import com.metamatrix.dqp.service.ConfigurationService;
import com.metamatrix.dqp.service.DQPServiceNames;
import com.metamatrix.dqp.service.DQPServiceRegistry;
import com.metamatrix.dqp.service.DataService;
import com.metamatrix.dqp.service.VDBService;


/** 
 * This a base service for all the Embedded DQP Services.
 * @since 4.3
 */
public abstract class EmbeddedBaseDQPService implements ApplicationService {
    public static final String SYSTEM_PHYSICAL_MODEL_NAME = CoreConstants.SYSTEM_PHYSICAL_MODEL_NAME;
    
    private DQPServiceRegistry svcRegistry = null;
    private boolean started = false;
    private boolean bound = false;
    private boolean initialized = false;
    
    public EmbeddedBaseDQPService(String serviceName, DQPServiceRegistry svcRegistry) 
        throws MetaMatrixComponentException{
        this.svcRegistry = svcRegistry;
        svcRegistry.registerService(serviceName, this);
    }
    
    
    /** 
     * @see com.metamatrix.common.application.ApplicationService#initialize(java.util.Properties)
     * @since 4.3
     */
    public final void initialize(Properties props) throws ApplicationInitializationException {
        if (!initialized) {
            initialized = true;
            initializeService(props);
        }
    }

    public abstract void initializeService(Properties properties) throws ApplicationInitializationException;
    
    /** 
     * @see com.metamatrix.common.application.ApplicationService#start(com.metamatrix.common.application.ApplicationEnvironment)
     * @since 4.3
     */
    public final void start(ApplicationEnvironment environment) throws ApplicationLifecycleException {
        if (!started) {
            started = true;
            startService(environment);
        }
    }
    
    public abstract void startService(ApplicationEnvironment environment) throws ApplicationLifecycleException;

    /** 
     * @see com.metamatrix.common.application.ApplicationService#bind()
     * @since 4.3
     */
    public final void bind() throws ApplicationLifecycleException {
        if (!bound) {
            bound = true;
            bindService();
        }
    }

    public abstract void bindService() throws ApplicationLifecycleException;
    
    /** 
     * @see com.metamatrix.common.application.ApplicationService#unbind()
     * @since 4.3
     */
    public final void unbind() throws ApplicationLifecycleException {
        if(bound) {
            bound = false;
            unbindService();
        }
    }
    
    public abstract void unbindService() throws ApplicationLifecycleException;

    /** 
     * @see com.metamatrix.common.application.ApplicationService#stop()
     * @since 4.3
     */
    public final void stop() throws ApplicationLifecycleException {        
        if (started) {
            started = false;
            stopService();
        }
    }

    public abstract void stopService()  throws ApplicationLifecycleException;
        
    /**
     * Look up the the service 
     * @param serviceName
     * @return
     * @throws MetaMatrixComponentException
     * @since 4.3
     */
    protected ApplicationService lookupService(String serviceName) 
        throws MetaMatrixComponentException{
        return svcRegistry.lookupService(serviceName);
    }
        
    /**
     * Helper to find the configuration service 
     * @return
     * @throws MetaMatrixComponentException
     * @since 4.3
     */
    protected ConfigurationService getConfigurationService() 
        throws MetaMatrixComponentException{
        return (ConfigurationService)lookupService(DQPServiceNames.CONFIGURATION_SERVICE);
    }
    
    protected DataService getDataService() 
        throws MetaMatrixComponentException{
        return (DataService)lookupService(DQPServiceNames.DATA_SERVICE);
    }

    protected VDBService getVDBService() 
        throws MetaMatrixComponentException{
        return (VDBService)lookupService(DQPServiceNames.VDB_SERVICE);
    }
    
    protected final boolean isStarted() {
        return started;
    }
    protected final boolean isBound() {
        return bound;
    }    
    
    protected String vdbId(VDBArchive vdb) {
        return vdbId(vdb.getName(),vdb.getVersion());
    }
    
    protected String vdbId(String name, String version) {
        return name.toUpperCase()+"_"+version; //$NON-NLS-1$
    }    
       
    protected boolean isFullyConfiguredVDB(VDBArchive vdb) throws MetaMatrixComponentException{
    	VDBDefn def = vdb.getConfigurationDef();
    	Collection models = def.getModels();
    	
        for (Iterator i = models.iterator(); i.hasNext();) {
            ModelInfo model = (ModelInfo)i.next();
            if (model.isPhysical()) {
                if (model.getConnectorBindingNames().isEmpty()) {
                    DQPEmbeddedPlugin.logWarning("VDBService.vdb_missing_bindings", new Object[] {vdb.getName(), vdb.getVersion()}); //$NON-NLS-1$
                    return false;
                }

                // make sure we have connector binding in the 
                // configuration service. 
                String bindingName = (String)model.getConnectorBindingNames().get(0); 
                ConnectorBinding binding = def.getConnectorBindingByName(bindingName);
                if (binding == null || getConfigurationService().getConnectorBinding(binding.getDeployedName()) == null) {
                    DQPEmbeddedPlugin.logWarning("VDBService.vdb_missing_bindings", new Object[] {vdb.getName(), vdb.getVersion()}); //$NON-NLS-1$
                    return false;
                }
            }
        }   
        return true;
    }
   
    /** 
     * checks the validity of the VDB
     * @param vdb
     * @return true if valid; false otherwise.
     */
    protected boolean isValidVDB(VDBArchive vdb) throws MetaMatrixComponentException{

    	// check if vdb has validity errors. If so log it..
        if (vdb.getVDBValidityErrors() != null) {
            String[] errors = vdb.getVDBValidityErrors(); 
            StringBuffer sb = new StringBuffer();
            for (int i = 0; i < errors.length; i++) {
                sb.append("-").append(errors[i]).append(";"); //$NON-NLS-1$ //$NON-NLS-2$
            } // for
            DQPEmbeddedPlugin.logError("VDBService.validityErrors", new Object[] {vdb.getName(), sb}); //$NON-NLS-1$
            return false;
        }
                
        VDBDefn def = vdb.getConfigurationDef();
        Collection models = def.getModels();
        if (models != null && models.isEmpty()) {
            DQPEmbeddedPlugin.logError("VDBService.vdb_missing_models", new Object[] {vdb.getName(), vdb.getVersion()}); //$NON-NLS-1$
            return false;        	
        }
                
        return true;
    }    
}
