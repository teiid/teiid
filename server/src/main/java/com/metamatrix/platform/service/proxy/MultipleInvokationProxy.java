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

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import com.metamatrix.api.exception.MetaMatrixException;
import com.metamatrix.common.messaging.RemoteMessagingException;
import com.metamatrix.platform.registry.ServiceRegistryBinding;
import com.metamatrix.platform.service.api.ServiceInterface;
import com.metamatrix.platform.service.api.exception.ServiceException;
import com.metamatrix.platform.service.api.exception.ServiceNotInitializedException;

/**
 * Will attempt to invoke all services and collect the results 
 */
public class MultipleInvokationProxy extends ServiceProxy {
    
    MultipleInvokationProxy(String serviceType) {
        super(serviceType);
    }
    
    public Object invoke(Object proxy, Method m, Object[] args) throws Throwable {

    	List<ServiceRegistryBinding> serviceBindings = this.getInstances();
    	
        boolean returnsCollection = Collection.class.isAssignableFrom(m.getReturnType());
        
        Collection result = null;
        
        if (returnsCollection) {
            result = new ArrayList();
        }
        
        int retryLimit = this.getRetryLimit();

        for (ServiceRegistryBinding serviceBinding:serviceBindings) {
        	ServiceInterface service = serviceBinding.getService();

            try {
                boolean called = false;

                for ( int i=0; (i<=retryLimit && !called); i++ ) {

                    try {
                        try {
                            Object singleResult = m.invoke(service, args);
                            if (returnsCollection && singleResult != null) {
                                result.addAll((Collection)singleResult);
                            }
                            called = true;
                            break;
                        } catch (InvocationTargetException e) {
                            throw e.getTargetException();
                        }
                        
                    } catch (RemoteMessagingException e) {
                        logException(e);
                        if (i < retryLimit) {
	                        // Retry retryLimit times...
	                        try {
	                            Thread.sleep(MAX_RETRY_DELAY_VAL);
	                        } catch ( InterruptedException ie ) { /* Try again. */ }
                        }
                    }
                }

                // remove bad service from instance list - logging is done there
                if(! called) {
                    serviceBinding.markServiceAsBad();
                }
                
            } catch (ServiceNotInitializedException snie) {
                // Service is unavailable - try the next one
                logException(snie);
            } catch (ServiceException sce) {
                serviceBinding.markServiceAsBad();
            } catch (MetaMatrixException e) {
                //should be logged by the service, just skip this one
            } 
        }
        
        return result;
    }
}