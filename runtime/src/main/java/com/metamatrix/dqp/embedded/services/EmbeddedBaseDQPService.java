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

package com.metamatrix.dqp.embedded.services;

import java.util.Collection;
import java.util.Properties;

import com.metamatrix.api.exception.MetaMatrixComponentException;
import com.metamatrix.common.application.ApplicationEnvironment;
import com.metamatrix.common.application.ApplicationService;
import com.metamatrix.common.application.exception.ApplicationInitializationException;
import com.metamatrix.common.application.exception.ApplicationLifecycleException;
import com.metamatrix.common.vdb.api.VDBArchive;
import com.metamatrix.common.vdb.api.VDBDefn;
import com.metamatrix.core.CoreConstants;
import com.metamatrix.dqp.embedded.DQPEmbeddedPlugin;
import com.metamatrix.dqp.service.ConfigurationService;
import com.metamatrix.dqp.service.DQPServiceNames;
import com.metamatrix.vdb.runtime.VDBKey;


/** 
 * This a base service for all the Embedded DQP Services.
 * @since 4.3
 */
public abstract class EmbeddedBaseDQPService implements ApplicationService {
    public static final String SYSTEM_PHYSICAL_MODEL_NAME = CoreConstants.SYSTEM_PHYSICAL_MODEL_NAME;
    
    private boolean started = false;
    private boolean initialized = false;    
    private ApplicationEnvironment environment;
    
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
            this.environment = environment;
            startService(environment);
        }
    }
    
    public abstract void startService(ApplicationEnvironment environment) throws ApplicationLifecycleException;

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
    protected ApplicationService lookupService(String serviceName) {
    	if (this.environment == null) {
    		throw new IllegalStateException("Service "+this.getClass().getName()+" not started"); //$NON-NLS-1$ //$NON-NLS-2$
    	}
        return environment.findService(serviceName);
    }
        
    /**
     * Helper to find the configuration service 
     * @return
     * @throws MetaMatrixComponentException
     * @since 4.3
     */
    protected ConfigurationService getConfigurationService() {
        return (ConfigurationService)lookupService(DQPServiceNames.CONFIGURATION_SERVICE);
    }
    
    protected final boolean isStarted() {
        return started;
    }
    
    protected VDBKey vdbId(VDBArchive vdb) {
        return new VDBKey(vdb.getName(),vdb.getVersion());
    }
    
    protected VDBKey vdbId(String name, String version) {
        return new VDBKey(name, version);
    }    
   
    /** 
     * checks the validity of the VDB
     * @param vdb
     * @return true if valid; false otherwise.
     */
    protected boolean isValidVDB(VDBArchive vdb) {

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
