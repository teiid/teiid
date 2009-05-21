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

package com.metamatrix.platform.registry;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.Iterator;

import com.metamatrix.common.config.api.DeployedComponent;
import com.metamatrix.common.messaging.MessageBus;
import com.metamatrix.common.queue.WorkerPoolStats;
import com.metamatrix.platform.service.ServiceMessages;
import com.metamatrix.platform.service.ServicePlugin;
import com.metamatrix.platform.service.api.ServiceID;
import com.metamatrix.platform.service.api.ServiceInterface;
import com.metamatrix.platform.service.api.ServiceState;
import com.metamatrix.platform.service.api.exception.ServiceException;
import com.metamatrix.platform.service.api.exception.ServiceStateException;
import com.metamatrix.platform.service.controller.ServiceData;

public class ServiceRegistryBinding extends RegistryBinding<ServiceInterface> {

    public final static class StateAwareProxy implements InvocationHandler {
		private final ServiceInterface proxiedService;

		public StateAwareProxy(ServiceInterface proxiedService) {
			this.proxiedService = proxiedService;
		}

		public Object invoke(Object proxy, Method m, Object[] args)
				throws Throwable {
			if (!m.getDeclaringClass().equals(ServiceInterface.class)) {
				proxiedService.checkState();
			}

			Object returnObj = null;
			try {
				returnObj = m.invoke(proxiedService, args);
		    } catch (InvocationTargetException err) {
		        throw err.getTargetException();
		    }
		    return returnObj;
		}
	}
    
    private ServiceData serviceData;

    /** indicates if service is an essential service */
    private boolean essential;

    /** defines service in configuration */
    private DeployedComponent deployedComponent;

    /** collection of queue names for service */
    private Collection queueNames;
    
    public ServiceRegistryBinding(ServiceID serviceID, ServiceInterface si, String serviceType, String instanceName,
            String componentType, String deployedName,
            String hostName, DeployedComponent deployedComponent,
            int state, Date time, boolean essential, MessageBus bus) {
    	super(si, hostName, bus);
    	this.serviceData = new ServiceData(ServiceState.STATE_NOT_INITIALIZED);
    	this.serviceData.setId(serviceID);
    	this.serviceData.setRoutingId(serviceType);
    	this.serviceData.setServiceType(componentType);
    	this.serviceData.setInstanceName(instanceName);
        this.deployedComponent = deployedComponent;
        this.essential = essential;
    }
    
    /**
     * Create new ServiceRegistryInstance
     */
    public ServiceRegistryBinding(ServiceInterface si, String hostName,
			DeployedComponent deployedComponent, boolean essential,
			MessageBus bus) {
    	super((ServiceInterface) Proxy.newProxyInstance(Thread
				.currentThread().getContextClassLoader(), si.getClass().getInterfaces(),
				new StateAwareProxy(si)), hostName, bus);
    	this.serviceData = si.getServiceData();
        this.deployedComponent = deployedComponent;
        this.essential = essential;
    }

    /**
     * Return ServiceID for this binding
     * @return ServiceID
     */
    public ServiceID getServiceID() {
        return this.serviceData.getId();
    }

    public String getServiceType() {
        return this.serviceData.getRoutingId();
    }

    public String getInstanceName() {
        return this.serviceData.getInstanceName();
    }

    public String getDeployedName() {
        return this.serviceData.getInstanceName();
    }

    public String getProcessName() {
        return this.getServiceID().getProcessName();
    }

    public int getCurrentState() {
        return this.serviceData.getState();
    }

    public Date getStateChangeTime() {
        return this.serviceData.getStateChangeTime();
    }
    
    public Throwable getInitException() {
        return this.serviceData.getInitException();
    }

    public boolean isEssential() {
        return essential;
    }

    public DeployedComponent getDeployedComponent() {
        return this.deployedComponent;
    }

    public boolean isServiceBad() {
        return (getCurrentState() == ServiceState.STATE_CLOSED ||
                getCurrentState() == ServiceState.STATE_FAILED ||
                getCurrentState() == ServiceState.STATE_INIT_FAILED);
    }

    public Collection getQueueNames() {
    	try {
	    	if (this.queueNames == null) {
	    		this.queueNames = buildQueueNames(getBindObject());
	    	}
    	} catch (ServiceException e) {
    		markServiceAsBad();
    		throw e;
    	}
        return this.queueNames;
    }
    
    public void checkState() {
    	//handle a stale process entry
    	if (!this.isLocal()) {
    		markServiceAsBad();
    		return;
    	}
    	ServiceInterface bindObject = getBindObject();
    	if (bindObject != null) {
    		try {
    			bindObject.checkState();
    		} catch (ServiceException e) {
    			
    		}
    	}
    }
    
    public void updateState(int state) {
    	this.serviceData.updateState(state);
    }
   
    private Collection buildQueueNames(ServiceInterface si) {
        ArrayList queue = null;
        
        if (si != null) {            
	        Collection stats = si.getQueueStatistics();
	        if (stats != null) {
	            queue = new ArrayList();
	            for (Iterator i = stats.iterator(); i.hasNext();) {
	                WorkerPoolStats qs = (WorkerPoolStats) i.next();
	                queue.add(qs.name);
	            }                    
	        }
        }
        return queue;
    }

    public String toString() {
        StringBuffer b = new StringBuffer("ServiceRegistryBinding: "); //$NON-NLS-1$
        b.append("\n\tserviceID: " + getServiceID()); //$NON-NLS-1$
        b.append("\n\tserviceType: " + getServiceType()); //$NON-NLS-1$
        b.append("\n\tinstanceName: " + getInstanceName()); //$NON-NLS-1$
        b.append("\n\thostName: " + getHostName()); //$NON-NLS-1$
        b.append("\n\tDeployedComponent: " + deployedComponent); //$NON-NLS-1$
        b.append("\n\tcurrentState: " + getCurrentState()); //$NON-NLS-1$
        b.append("\n\tessential: " + (essential?"true":"false")); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
        return b.toString();
    }

	public boolean isActive() {
		return (this.getCurrentState() == ServiceState.STATE_OPEN || this.getCurrentState() == ServiceState.STATE_DATA_SOURCE_UNAVAILABLE);		
	}
	
	public void markServiceAsBad() {
		invalidateBindObject();
		updateState(ServiceState.STATE_FAILED);
	}
	
	public ServiceInterface getService() {
		ServiceInterface result = getBindObject();
		if (result == null) {
	        throw new ServiceStateException(ServiceMessages.SERVICE_0012, ServicePlugin.Util.getString(ServiceMessages.SERVICE_0012, this.serviceData.getInstanceName(), this.serviceData.getId()));
		}
		return result;
	}
	
	public void setDirty(boolean dirty) {
		this.serviceData.setDirty(dirty);
	}

	public boolean isDirty() {
		return this.serviceData.isDirty();
	}

}
