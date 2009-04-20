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

import java.lang.reflect.Proxy;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import com.google.inject.Inject;
import com.google.inject.Singleton;
import com.google.inject.name.Named;
import com.metamatrix.common.queue.WorkerPool;
import com.metamatrix.common.queue.WorkerPoolFactory;
import com.metamatrix.platform.registry.ClusteredRegistryState;
import com.metamatrix.platform.registry.RegistryListener;
import com.metamatrix.platform.registry.ServiceRegistryBinding;
import com.metamatrix.platform.service.ServiceMessages;
import com.metamatrix.platform.service.ServicePlugin;
import com.metamatrix.platform.service.api.ServiceInterface;
import com.metamatrix.platform.util.PlatformProxyHelper;
import com.metamatrix.server.Configuration;

/**
 * A traffic cop for all the proxies in this VM.
 */
@Singleton
public class ProxyManager implements RegistryListener{
	
    // Map of all SelectionPolicies.
    private Map<ServiceSelectionPolicyKey, ServiceSelectionPolicy> policyRegistry = new HashMap<ServiceSelectionPolicyKey, ServiceSelectionPolicy>();

    /**
     * Thread that updates the service instances of the service selection policies
     * when notification of a Registry change event has occurred.
     */
    private WorkerPool updatePool = WorkerPoolFactory.newWorkerPool("RegistryUpdate", 1); //$NON-NLS-1$
    
	ClusteredRegistryState registry;
	
	String hostName;
		
	String processName;
	
    @Inject
    public ProxyManager(@Named(Configuration.HOSTNAME)String hostName, @Named(Configuration.PROCESSNAME)String processName, ClusteredRegistryState registry) {
    	this.hostName = hostName;
    	this.processName = processName;
    	this.registry = registry;
    	this.registry.addListener(this);
    }
    
    @Override
    public void registryShutdown() {
    	this.updatePool.shutdownNow();
    }
    
    @Override
	public void registryChanged() {
    	updatePool.execute(new Runnable() {public void run() { doUpdate(); } });
	}
    
    
    /**
	 * Returns a <code>ServiceProxy</code> of the given type.<br>
	 * If a proxy of the given type exists in the proxy registry, its service
	 * instances are updated and it's returned. If no proxy of the given type
	 * exists in the proxy registry, a new proxy is created, registered and
	 * returned.
	 * 
	 * @param serviceTypeName
	 *            The primary method for recognizing a proxy is by the type of
	 *            the service it represents. May not be <code>null</code>.
	 * @param serviceInstances
	 *            The list of updated service instances for the proxy.
	 * @param props
	 *            The proxy properties.
	 * @return The proxy of interest.
	 */
    public synchronized ServiceInterface findOrCreateProxy(String serviceTypeName, Properties props) {
        // Try looking in policy registry first
        ServiceSelectionPolicyKey policyKey = createPolicyKey(serviceTypeName, props);
        ServiceSelectionPolicy policy = policyRegistry.get(policyKey);

        if (policy == null) {
             // Create policy and populate with service instances
             policy = createPolicy(policyKey.getPolicyType());

             // Add service instances to policy
             setServiceInstances(policy, serviceTypeName);

             // Register for next lookup
             policyRegistry.put(policyKey, policy);
        }

        return createProxy(serviceTypeName, props, policy);
    }

    static ServiceInterface createProxy(String serviceTypeName, Properties props, ServiceSelectionPolicy policy) {
        // Create proxy and assign it the selection policy
        String proxyClassName = props.getProperty(ServiceProxyProperties.SERVICE_PROXY_CLASS_NAME);
        if ( proxyClassName == null ) {
            throw new IllegalArgumentException(ServicePlugin.Util.getString(ServiceMessages.SERVICE_0049));
        }

        // Create the proxy
        Class clazz = null;
        try {
            clazz = Class.forName(proxyClassName);
        } catch ( ClassNotFoundException e ) {
            throw new IllegalArgumentException(ServicePlugin.Util.getString(ServiceMessages.SERVICE_0050, proxyClassName, e));
        }

        ServiceProxy serviceProxy = null;
        
        if (Boolean.valueOf(props.getProperty(ServiceProxyProperties.SERVICE_MULTIPLE_DELEGATION)).booleanValue()) {
            serviceProxy = new MultipleInvokationProxy(serviceTypeName);
        } else {
            serviceProxy = new SingleInvokationProxy(serviceTypeName);
            serviceProxy.setStickyFlag(Boolean.valueOf(props.getProperty(ServiceProxyProperties.SERVICE_SELECTION_STICKY)).booleanValue());
        }
        
        serviceProxy.setServiceSelectionPolicy(policy);
        
        return (ServiceInterface)Proxy.newProxyInstance(Thread.currentThread().getContextClassLoader(), new Class[] {clazz}, serviceProxy);
    }

    /**
     * Returns a string representing the current state of the object.
     */
    public synchronized String toString() {
        Iterator itr = policyRegistry.keySet().iterator();
        StringBuffer s = new StringBuffer();
        s.append("("); //$NON-NLS-1$
        s.append(this.getClass().getName() + "(" + policyRegistry.size() + ")"); //$NON-NLS-1$ //$NON-NLS-2$
        s.append("\n     "); //$NON-NLS-1$
        while ( itr.hasNext() ) {
            s.append(itr.next().toString());
            s.append("     "); //$NON-NLS-1$
        }
        s.append(")"); //$NON-NLS-1$
        return s.toString();
    }

    /**
     * method that the <code>updateThread</code> calls to perform the
     * service instance updates in all service selection policies.
     */
    private synchronized void doUpdate() {
        for ( ServiceSelectionPolicyKey key:this.policyRegistry.keySet()) {
            setServiceInstances(policyRegistry.get(key), key.getServiceTypeName());
        }
    }

    /**
     * Create a key for a <code>ServiceSelectionPolicy<code>.<br>
     * The key uses the service type name, which is a combination of the service
     * name and the proxy type {@link ServiceProxy}
     * and whether or not the proxy prefers to talk to local services.
     */
    private ServiceSelectionPolicyKey createPolicyKey(String serviceTypeName, Properties props) {
        String policyTypeName = (String) props.get(ServiceProxyProperties.SERVICE_SELECTION_POLICY_NAME);
        if ( policyTypeName == null ) {
            throw new IllegalArgumentException(ServicePlugin.Util.getString(ServiceMessages.SERVICE_0051));
        }
        ServiceSelectionPolicyKey key = new ServiceSelectionPolicyKey(serviceTypeName, policyTypeName);
        return key;
    }

    /**
     * Create a service selection policy of the proper type.
     * @param policyKey The policy's key object that has information for policy
     * creation.
     * @return The newly created service selection policy.
     */
    private ServiceSelectionPolicy createPolicy(String policy) {
        if (policy.equals(PlatformProxyHelper.RANDOM)) {
        	return new RandomSelectionPolicy(false);
        }
        else if (policy.equals(PlatformProxyHelper.RANDOM_LOCAL)) {
        	return new RandomSelectionPolicy(true);
        }
        else if (policy.equals(PlatformProxyHelper.ROUND_ROBIN)) {
        	return new RoundRobinSelectionPolicy(false);	
        }
        else if (policy.equals(PlatformProxyHelper.ROUND_ROBIN_LOCAL)) {
        	return new RoundRobinSelectionPolicy(true);
        }
        else {
            throw new IllegalArgumentException(ServicePlugin.Util.getString(ServiceMessages.SERVICE_0060, policy));
        }
    }

    /**
     * Set the service instance list(s) on a newly created <code>ServiceSelectionPolicy</code>.
     * @param The <code>ServiceSelectionPolicy</code> on which to set the service
     * instance list(s).
     * @param serviceType The type of the service of interest.
     */
    private void setServiceInstances(ServiceSelectionPolicy policy, String serviceType) {
        List<ServiceRegistryBinding> localServiceBindings = this.registry.getActiveServiceBindings(this.hostName, this.processName, serviceType);
        List<ServiceRegistryBinding> serviceBindings = this.registry.getActiveServiceBindings(null, null, serviceType);
        serviceBindings.removeAll(localServiceBindings);
        policy.updateServices(localServiceBindings, serviceBindings);
    }
    
} // End class ProxyManager





