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

package com.metamatrix.common.application;

import java.util.LinkedHashMap;
import java.util.Map;

import com.metamatrix.cache.CacheFactory;
import com.metamatrix.common.CommonPlugin;
import com.metamatrix.common.application.exception.ApplicationInitializationException;
import com.metamatrix.common.application.exception.ApplicationLifecycleException;

/**
 * The environment is available internally to the application as a means 
 * of finding application services of a particular type or to retrieve
 * other information about the application itself.  
 */
public class ApplicationEnvironment {

    private LinkedHashMap<String, ApplicationService> services = new LinkedHashMap<String, ApplicationService>();
    
    private CacheFactory cache;
    
    public ApplicationEnvironment() {
    	
    }

    /* 
     * @see com.metamatrix.common.application.ApplicationEnvironment#bindService(java.lang.String, com.metamatrix.common.application.ApplicationService)
     */
    public void bindService(String type, ApplicationService service) {
        this.services.put(type, service);
    }

    public ApplicationService findService(String type) {
        return this.services.get(type);
    }
    
    public CacheFactory getCacheFactory() {
		return cache;
	}
    
    public void setCacheFactory(CacheFactory cache) {
		this.cache = cache;
	}
    
    /* 
     * @see com.metamatrix.common.application.Application#installService(com.metamatrix.common.application.ApplicationService)
     */
    public final void installService(String type, ApplicationService service) throws ApplicationInitializationException {
        if(service == null) {
            return;
        }
        
        try {
            service.start(this);
            this.bindService(type, service);
        } catch(ApplicationLifecycleException e) {
            throw new ApplicationInitializationException(e, CommonPlugin.Util.getString("BasicApplication.Failed_while_installing_service_of_type__1") + type); //$NON-NLS-1$
        }
    }

    /**
     * @see com.metamatrix.common.application.Application#stop()
     */
    public void stop() throws ApplicationLifecycleException {
        for (Map.Entry<String, ApplicationService> entry : services.entrySet()) {
            entry.getValue().stop();
		}
        services.clear();
    }
    
}
