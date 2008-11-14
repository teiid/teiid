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

package com.metamatrix.dqp.config;

import com.metamatrix.common.application.ApplicationService;
import com.metamatrix.common.application.exception.ApplicationInitializationException;
import com.metamatrix.common.application.exception.ApplicationLifecycleException;
import com.metamatrix.common.log.LogManager;
import com.metamatrix.dqp.DQPPlugin;
import com.metamatrix.dqp.internal.process.DQPCore;
import com.metamatrix.dqp.service.DQPServiceNames;
import com.metamatrix.dqp.util.LogConstants;

/**
 * This launcher class can be used to create an instance of DQP given a
 * {@link DQPConfigSource} that can supply the configuration information.
 */
public class DQPLauncher {

    private DQPConfigSource configSource;

    /**
     * Create the launcher with the configuration source.
     * @param configSource
     */
    public DQPLauncher(final DQPConfigSource configSource) {
        this.configSource = configSource;
    }

    public DQPCore createDqp() throws ApplicationInitializationException {
       	DQPCore dqp = new DQPCore();

        // Initialize DQP
        dqp.initialize(this.configSource.getProperties());

        // Initialize all the services first, because some of services have inter-dependencies
        // as per hanging on to references goes. like data service needs references to
        // vdb,metadata during start-up.
        initServices();
        
        // Load services into DQP
        for(int i=0; i<DQPServiceNames.ALL_SERVICES.length; i++) {
            final String serviceName = DQPServiceNames.ALL_SERVICES[i];
            ApplicationService appService = this.configSource.getService(serviceName);
            dqp.installService(serviceName, appService);
            if(appService == null){
            	//should not come here
                LogManager.logWarning(LogConstants.CTX_DQP, DQPPlugin.Util.getString("DQPLauncher.InstallService_ServiceIsNull", serviceName)); //$NON-NLS-1$
            }else{
                LogManager.logInfo(LogConstants.CTX_DQP, DQPPlugin.Util.getString("DQPLauncher.InstallService_ServiceInstalled", serviceName)); //$NON-NLS-1$
            }
        }

        // Start the DQP
        try {
			dqp.start();
		} catch (ApplicationLifecycleException e) {
			throw new ApplicationInitializationException(e);
		}

        // Ready to go!
        return dqp;
    }
    
    private void initServices() throws ApplicationInitializationException {
        for(int i=0; i<DQPServiceNames.ALL_SERVICES.length; i++) {
            final String serviceName = DQPServiceNames.ALL_SERVICES[i];
            configSource.getService(serviceName);
        }
    }
}
