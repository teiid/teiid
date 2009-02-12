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

package com.metamatrix.platform.service.proxy;

import java.lang.reflect.InvocationHandler;
import java.util.List;

import com.metamatrix.common.log.LogManager;
import com.metamatrix.common.util.LogContextsUtil;
import com.metamatrix.platform.registry.ServiceRegistryBinding;
import com.metamatrix.platform.service.ServiceMessages;
import com.metamatrix.platform.service.ServicePlugin;
import com.metamatrix.platform.service.api.exception.ServiceNotFoundException;

/**
 * Provides common functionality for all service proxies.<br>
 * Proxies for a given type of service extend this class and provide the required
 * implementation.
 */
public abstract class ServiceProxy implements InvocationHandler {

    private static final int DEFAULT_RETRY_LIMIT = 1;

    private int retryLimit = DEFAULT_RETRY_LIMIT;

    protected static final int MAX_RETRY_DELAY_VAL = 250; // sleep for 250 ms between tries.

    private String serviceType;

    private ServiceSelectionPolicy serviceSelectionPolicy;

    private boolean sticky = false;
    
    public ServiceProxy(String serviceType) {
        this.serviceType = serviceType;
    }
    
    /**
     * Returns the service type that this proxy proxies for.
     * @return The proxy's service type.
     */
    public String getServiceType() {
        return serviceType;
    }
    
    /**
     * Set the <code>ServiceSelectionPolicy</code> that this proxy will use to
     * select service instances for its lifetime.
     * @param policy The <code>ServiceSelectionPolicy</code> to use.
     */
    public void setServiceSelectionPolicy(ServiceSelectionPolicy policy) {
        this.serviceSelectionPolicy = policy;
    }

    /**
     * Get the next service instance from the given service selection policy.
     * @return The <code>ServiceRegistryBinding</code> for the next instance after
     * being operated on by the selection policy.
     */
    protected ServiceRegistryBinding getNextInstance() throws ServiceNotFoundException {
        try {
            return serviceSelectionPolicy.getNextInstance();
        } catch ( ServiceNotFoundException e ) {
        	String msg = ServicePlugin.Util.getString(ServiceMessages.SERVICE_0054, getServiceType());
            throw new ServiceNotFoundException(e, ServiceMessages.SERVICE_0054, msg);
        }
    }
    
    /**
     * Get the list of service instances from the given service selection policy.
     * @return The <code>ServiceRegistryBinding</code> for the next instance after
     * being operated on by the selection policy.
     */
    protected List<ServiceRegistryBinding> getInstances() throws ServiceNotFoundException {
        try {
            return serviceSelectionPolicy.getInstances();
        } catch ( ServiceNotFoundException e ) {
            throw new ServiceNotFoundException(e, ServiceMessages.SERVICE_0054, ServicePlugin.Util.getString(ServiceMessages.SERVICE_0054, getServiceType()));
        }
    }

    public int getRetryLimit() {
        return retryLimit;
    }

    public void setRetryLimit(int retryLimit) {
        this.retryLimit = retryLimit;
    }

    /**
     * Log an exception. Subclassers can override and provide a more service-specific
     * logging mechanism if desired.
     */
    protected final void logException(Throwable e) {
		LogManager.logError(LogContextsUtil.CommonConstants.CTX_PROXY,e, this.getClass().getName());
    }

    /** 
     * @see com.metamatrix.platform.service.proxy.ServiceProxyInterface#getStickyFlag()
     */
    public boolean getStickyFlag() {
        return sticky;
    }
    
    /** 
     * @see com.metamatrix.platform.service.proxy.ServiceProxyInterface#setStickyFlag(boolean)
     */
    public void setStickyFlag(boolean sticky) {
        this.sticky = sticky;
    }
}





