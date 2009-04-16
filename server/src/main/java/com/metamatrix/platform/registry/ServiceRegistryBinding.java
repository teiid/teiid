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

import java.io.Serializable;
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
import com.metamatrix.platform.service.api.ServiceID;
import com.metamatrix.platform.service.api.ServiceInterface;
import com.metamatrix.platform.service.api.ServiceState;
import com.metamatrix.server.ResourceFinder;

public class ServiceRegistryBinding implements Serializable {

    private final class StateAwareProxy implements InvocationHandler {
		private final ServiceInterface proxiedService;

		private StateAwareProxy(ServiceInterface proxiedService) {
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
		    
		    ServiceRegistryBinding.this.setInitException(proxiedService.getInitException());
		    ServiceRegistryBinding.this.updateState(proxiedService.getCurrentState());
		    return returnObj;
		}
	}

	/** Identifies service for purpose of looking up in registry */
    private ServiceID serviceID;

    /** local reference to service, made transient so it will not be sent to remote registries */
    private transient ServiceInterface service;
    
    /** remote reference */
    private Object serviceStub;
    
    /** type of service */
    private String serviceType;

    /** component type */
    private String componentType;

    /** Name of deployed service */
    private String deployedName;

    /** Name of host this service is running on */
    private String hostName;

    /** Instance name */
    private String instanceName;

    /**
     * Current state of service, this is updated by the service framework
     * whenever the state changes
     */
    private int currentState;

    /** Time of the last state change */
    private Date stateChangeTime;

    /** indicates if service is an essential service */
    private boolean essential;

    /** defines service in configuration */
    private DeployedComponent deployedComponent;

//    /** identifies psc this service belongs to. */
//    private ProductServiceConfigID pscID;

    /** collection of queue names for service */
    private Collection queueNames;
    
    /** Exception during initialization */
    private Throwable initException;
    
    private transient MessageBus messageBus;
    
    private transient boolean dirty;
    
    /**
     * Create new ServiceRegistryInstance
     *
     * @param serviceID Identifies service
     * @param service ServiceInstance
     * @param serviceType
     * @param instanceName Instance name of service
     * @param componenetType
     * @param deployedName
     * @param hostName
     * @param state
     * @param time
     * @param essential, true indicates service is an essential service and cannot be shutdown if there are no other similiar services running.
     */
    public ServiceRegistryBinding(ServiceID serviceID, ServiceInterface si, String serviceType, String instanceName,
                                  String componentType, String deployedName,
                                  String hostName, DeployedComponent deployedComponent,
                                  int state, Date time, boolean essential, MessageBus bus) {

        this.serviceID = serviceID;
        this.serviceType = serviceType;
        this.instanceName = instanceName;
        this.componentType = componentType;
        this.deployedName = deployedName;
        this.hostName = hostName;
        this.deployedComponent = deployedComponent;
        this.currentState = state;
        this.stateChangeTime = time;
        this.essential = essential;
        this.messageBus = bus;
        this.setService(si);
        
    }

    /**
     * Return ServiceID for this binding
     * @return ServiceID
     */
    public ServiceID getServiceID() {
        return this.serviceID;
    }

    /**
     * Return reference to service
     * If service is local then return local reference
     * Else return stub
     */
    public synchronized ServiceInterface getService() {
    	if (this.service != null) {
    		return service;
    	}
    	if (this.serviceStub == null) {
    		return null;
    	}
    	// when exported to the remote, use remote's message bus instance.
    	MessageBus bus = this.messageBus;
    	if(bus == null) {
    		bus = ResourceFinder.getMessageBus();
    	}
    	this.service = (ServiceInterface)bus.getRPCProxy(this.serviceStub);
    	return this.service;
    }
    
    public String getServiceType() {
        return this.serviceType;
    }

    public String getInstanceName() {
        return this.instanceName;
    }

    public String getComponentType() {
        return this.componentType;
    }

    public String getDeployedName() {
        return this.deployedName;
    }

    public String getHostName() {
        return this.hostName;
    }
    
    public String getProcessName() {
        return this.serviceID.getProcessName();
    }

    public int getCurrentState() {
        return this.currentState;
    }

    public Date getStateChangeTime() {
        return this.stateChangeTime;
    }

    public boolean isEssential() {
        return essential;
    }

    public DeployedComponent getDeployedComponent() {
        return this.deployedComponent;
    }

//    public ProductServiceConfigID getPscID() {
//        return this.pscID;
//    }

    public boolean isServiceBad() {
        return (currentState == ServiceState.STATE_CLOSED ||
                currentState == ServiceState.STATE_FAILED ||
                currentState == ServiceState.STATE_INIT_FAILED);
    }

    public Collection getQueueNames() {
    	if (this.queueNames == null) {
    		this.queueNames = buildQueueNames(this.service);
    	}
        return this.queueNames;
    }

    public Throwable getInitException() {
        return this.initException;
    }
    
    public void setInitException(Throwable t) {
        this.initException = t;
    }
    
    public void updateState(int state) {
    	if (this.currentState != state) {
	        this.currentState = state;
	        this.stateChangeTime = new Date();
	        this.dirty = true;
    	}
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
        b.append("\n\tserviceID: " + serviceID); //$NON-NLS-1$
        b.append("\n\tserviceType: " + serviceType); //$NON-NLS-1$
        b.append("\n\tinstanceName: " + instanceName); //$NON-NLS-1$
        b.append("\n\thostName: " + hostName); //$NON-NLS-1$
//        b.append("\n\tpscName: " + pscID); //$NON-NLS-1$
        b.append("\n\tDeployedComponent: " + deployedComponent); //$NON-NLS-1$
        b.append("\n\tcurrentState: " + currentState); //$NON-NLS-1$
        b.append("\n\tessential: " + (essential?"true":"false")); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$

        try {
            b.append("\n\tserviceStub className = " + serviceStub.getClass().getName()); //$NON-NLS-1$
        } catch (Exception e) {
            b.append("\n\tserviceStub className = null"); //$NON-NLS-1$
        }
        return b.toString();
    }

	private synchronized void setService(ServiceInterface service) {
		this.service = service;
    	if (this.serviceStub != null) {
    		this.messageBus.unExport(serviceStub);
    		serviceStub = null;
    	}
        if (service != null) {
            this.serviceStub = this.messageBus.export(service, service.getClass().getInterfaces());
			this.service = (ServiceInterface) Proxy.newProxyInstance(Thread
					.currentThread().getContextClassLoader(), service.getClass().getInterfaces(),
					new StateAwareProxy(service));
		}
	}
	
	public boolean isActive() {
		return (this.currentState == ServiceState.STATE_OPEN || this.currentState == ServiceState.STATE_DATA_SOURCE_UNAVAILABLE);		
	}
	
	public void markServiceAsBad() {
		setService(null);
		updateState(ServiceState.STATE_FAILED);
	}

	public void setDirty(boolean dirty) {
		this.dirty = dirty;
	}

	public boolean isDirty() {
		return dirty;
	}
}

