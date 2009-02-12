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

import java.io.NotSerializableException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.concurrent.locks.ReentrantLock;

import com.metamatrix.common.messaging.RemoteMessagingException;
import com.metamatrix.platform.registry.ServiceRegistryBinding;
import com.metamatrix.platform.service.api.exception.ServiceException;
import com.metamatrix.platform.service.api.exception.ServiceNotInitializedException;

/**
 * Will attempt to invoke services until the first result is obtained 
 */
public class SingleInvokationProxy extends ServiceProxy {
    
    private volatile ServiceRegistryBinding stickyServiceBinding;
    private ReentrantLock lock = new ReentrantLock();
    
    SingleInvokationProxy(String serviceType) {
        super(serviceType);
    }
    
    public Object invoke(Object proxy, Method m,Object[] args) throws Throwable {
    	if (getStickyFlag() && stickyServiceBinding == null) {
            lock.lock();
            if (stickyServiceBinding != null) {
                lock.unlock();
            }
        }
        try {
        	int retryLimit = getRetryLimit();
            while ( true ) {   
                Exception exception = null;
                ServiceRegistryBinding serviceBinding = null;
                if (getStickyFlag() && stickyServiceBinding != null) {
                    serviceBinding = stickyServiceBinding;
                } else {
                    //throws ServiceException when no more instances
                    serviceBinding = getNextInstance();
                }
                try {
                    for ( int i=0; i<=retryLimit; i++ ) {
                        try {
                            try {
                                Object result = m.invoke(serviceBinding.getService(), args);
                                stickyServiceBinding = serviceBinding;
                                return result;
                            } catch (InvocationTargetException err) {
                                throw err.getTargetException();
                            }
                        } catch (RemoteMessagingException e) {
                        	exception = e;
                            if (e.getCause() instanceof NotSerializableException) {
                                //remote calls are not supported for non-serializable arguments
                                throw e;
                            }
                            if (i < retryLimit) {
	                            try {
	                                Thread.sleep(MAX_RETRY_DELAY_VAL);
	                            } catch ( InterruptedException ie ) { /* Try again. */ }
                            }
                        } 
                    }
                    // If we're here, then we've retried the allowed number of times... instance is bad.
                    serviceBinding.markServiceAsBad();
                } catch (ServiceNotInitializedException e) {
                    exception = e;
                    // This does not means that this instance is bad, just not ready... Get next instance.
                } catch (ServiceException e) {
                    exception = e;
                    // This means that this instance is bad...
                    serviceBinding.markServiceAsBad();
                }
                logException(exception);
                //sticky services are not tried indefinitely, just throw an exception
                if (getStickyFlag() && stickyServiceBinding != null) {
                    throw exception;
                }
            }
        } finally {
            if (lock.isHeldByCurrentThread()) {
                lock.unlock();
            }
        }
    }
}