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

package com.metamatrix.common.config.model;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Properties;

import com.metamatrix.common.config.api.AuthenticationProvider;
import com.metamatrix.common.config.api.AuthenticationProviderID;
import com.metamatrix.common.config.api.ComponentDefn;
import com.metamatrix.common.config.api.ComponentDefnID;
import com.metamatrix.common.config.api.ComponentObject;
import com.metamatrix.common.config.api.ComponentObjectID;
import com.metamatrix.common.config.api.ComponentType;
import com.metamatrix.common.config.api.ComponentTypeID;
import com.metamatrix.common.config.api.Configuration;
import com.metamatrix.common.config.api.ConfigurationInfo;
import com.metamatrix.common.config.api.ConnectorBinding;
import com.metamatrix.common.config.api.ConnectorBindingID;
import com.metamatrix.common.config.api.DeployedComponent;
import com.metamatrix.common.config.api.DeployedComponentID;
import com.metamatrix.common.config.api.Host;
import com.metamatrix.common.config.api.HostID;
import com.metamatrix.common.config.api.ResourceDescriptor;
import com.metamatrix.common.config.api.ServiceComponentDefn;
import com.metamatrix.common.config.api.ServiceComponentDefnID;
import com.metamatrix.common.config.api.VMComponentDefn;
import com.metamatrix.common.config.api.VMComponentDefnID;
import com.metamatrix.common.config.api.exceptions.ConfigurationException;
import com.metamatrix.common.config.api.exceptions.InvalidArgumentException;
import com.metamatrix.common.namedobject.BaseID;
import com.metamatrix.common.util.PropertiesUtils;
import com.metamatrix.core.util.Assertion;

/**
 * Configuration represents a single named set of deployable components (e.g., service and system components),
 *   a deployed set of components, global properties, and additional information.  Once the configuration is
 *   flagged as "Released", it is then available to be deployed.  Once it is flagged as "Deployed",
 *  it cannot be changed.  The configuration must be versioned, and then changes can be made to the
 * new versioned copy.
 */
public class BasicConfiguration extends BasicComponentObject implements Configuration {


    /**
     * @link aggregationByValue
     * @supplierCardinality 1
     * @label info
     */
    private BasicConfigurationInfo info;

    /**
     * Key = ComponentID  Value=Component
     */
    private Map svcComponents = new HashMap();
    private Map connectors = new HashMap();    
    private Map authProviders = new HashMap();    
    private Map conRoutingToIDMap = new HashMap();     
    
    private Map vms = new HashMap();
    private Map pools = new HashMap();
        
    private Map deployedComponents = new HashMap();
    private Map hosts = new HashMap();

    

    /**
     * Construct a new Configuration using the name and component type;
     */
    public BasicConfiguration(ConfigurationInfo configInfo, ComponentTypeID typeID) {
        super(configInfo.getID(), typeID);
        info = (BasicConfigurationInfo) configInfo;
    }

    BasicConfiguration(BasicConfiguration config) {
        super(config);
    }

    public BaseID getID() {
	    return info.getID();
    }

   /**
   * Override this method to obtain the name from the ConfigurationInfo.
   */
    public String getName() {
	    return info.getID().getName();
    }

    /**
     * Does this config represent the Next Startup config?
     * @return true if it does, false otherwise.
     */
    public boolean isNextStartUp() {
        return info.getID().equals(NEXT_STARTUP_ID);
    }

    public ConfigurationInfo getInfo() {
	    return info;
    }

    /**
    *   return a clone of the Map of ComponentDefns so that no one can add
    *   directly to this objects Map of component definitions
    */
   public Map getComponentDefns() {
   		int s = 1;
   		    	
    	if (svcComponents != null) {
    		s += svcComponents.size();
    	}
    	    	
    	if (vms != null) {
    		s += vms.size();
    	}
        
        if (hosts != null) {
            s += hosts.size();
        }        
   	
    	if (pools != null) {
    		s += pools.size();
    	}
    	
    	if (authProviders != null) {
    		s += authProviders.size();
    	}

    	if (connectors != null) {
    		s += connectors.size();
    	}

        Map comps = new HashMap(s);

		comps.putAll(pools);
		comps.putAll(svcComponents);
		comps.putAll(vms);
		comps.putAll(authProviders);
		comps.putAll(connectors);
        comps.putAll(hosts);
		
        return comps;

    }
    
    public boolean doesExist(ComponentDefnID componentID) {
    	
            switch(BasicUtil.getComponentType(componentID)){
                case ComponentType.VM_COMPONENT_TYPE_CODE:
                    if (getVMDefn(componentID) != null) {
                    	return true;
                    } 
                    return false;
 
                case ComponentType.SERVICE_COMPONENT_TYPE_CODE: 
                	if (getServiceComponentDefn(componentID) != null) {
                		return true;
                	}
                	return false;
                	                 
                 case ComponentType.RESOURCE_COMPONENT_TYPE_CODE:
                    if (getConnectionPool(componentID) != null) {
                    	return true;
                    }
                    return false;
                    
                 case ComponentType.CONNECTOR_COMPONENT_TYPE_CODE:
                    if (getConnectorBinding(componentID) != null) {
                    	return true;
                    }
                    return false;
                 
                 case ComponentType.AUTHPROVIDER_COMPONENT_TYPE_CODE:
                     if (getAuthenticationProvider(componentID) != null) {
                     	return true;
                     }
                     return false;                    
                    
                 case ComponentType.HOST_COMPONENT_TYPE_CODE:
                     if (getHost(componentID) != null) {
                        return true;
                     }
                     return false;
                    
    
                default:
                    Assertion.assertTrue(false, "Process Error: component defn of type " + componentID.getClass().getName() + " not accounted for."); //$NON-NLS-1$ //$NON-NLS-2$
                    return false;
            }
    	
    	
    }
    

    public ComponentDefn getComponentDefn(ComponentDefnID componentID) {
    	
            switch(BasicUtil.getComponentType(componentID)){
                case ComponentType.VM_COMPONENT_TYPE_CODE:
                    return getVMDefn(componentID);
                 case ComponentType.SERVICE_COMPONENT_TYPE_CODE:
                	return getServiceComponentDefn(componentID);                   
                case ComponentType.RESOURCE_COMPONENT_TYPE_CODE:
                    return getConnectionPool(componentID);
                case ComponentType.CONNECTOR_COMPONENT_TYPE_CODE:
                    return getConnectorBinding(componentID);                    
                 case ComponentType.HOST_COMPONENT_TYPE_CODE:
                    return getHost(componentID);    
                 case ComponentType.AUTHPROVIDER_COMPONENT_TYPE_CODE:
                     return getAuthenticationProvider(componentID);                     
                 default:
                     Assertion.assertTrue(false, "Process Error: component defn of type " + componentID.getClass().getName() + " not accounted for."); //$NON-NLS-1$ //$NON-NLS-2$
                     return null;
                 
            }
    	

    }
    
    
    public VMComponentDefn getVMComponentDefn(ComponentDefnID componentID) {
    	return getVMDefn(componentID);
    }
    public VMComponentDefn getVMDefn(ComponentDefnID componentID) {
    	
        if (vms.containsKey(componentID)) {
            return ((VMComponentDefn) vms.get(componentID));
        }
        return null;    		   	
    }
        
    
    /**
     * Returns a <code>AuthenticationProvider</code> based on the specified name.
     *  @return AuthenticationProvider for that name
     */
    public AuthenticationProvider getAuthenticationProvider(String name) {
    
    
    	Iterator it = authProviders.keySet().iterator();	
    	while (it.hasNext()) {
    		ComponentDefnID id = (ComponentDefnID) it.next();
    		if (id.getFullName().equalsIgnoreCase(name)) {
    			return ((AuthenticationProvider) authProviders.get(id)); 
    		}	
    		
    	}
    	return null;
    	
    }

    public AuthenticationProvider getAuthenticationProvider(ComponentDefnID componentID) {
    		
            if (authProviders.containsKey(componentID)) {
                return ((AuthenticationProvider) authProviders.get(componentID));
            }
            return null;     		
    	
    }
    
    public ResourceDescriptor getConnectionPool(ComponentDefnID componentID) {
    	
        if (pools.containsKey(componentID)) {
            return ((ResourceDescriptor) pools.get(componentID));
        }
        return null;    		   	
    }
    
    public ConnectorBinding getConnectorBinding(ComponentDefnID componentID) {
    	
        if (connectors.containsKey(componentID)) {
            return ((ConnectorBinding) connectors.get(componentID));
        }
        return null;    		   	
    }
    
   /**
     * Returns a <code>ConnectorBinding</code> based on the specified name.
     *  @return ConnectorBinding for that name
     */
    public ConnectorBinding getConnectorBinding(String name) {
    
    
    	Iterator it = connectors.keySet().iterator();	
    	while (it.hasNext()) {
    		ComponentDefnID id = (ComponentDefnID) it.next();
    		if (id.getFullName().equalsIgnoreCase(name)) {
    			return ((ConnectorBinding) connectors.get(id)); 
    		}	
    		
    	}
    	return null;
    	
    }
    
    /**
     * Returns a <code>ConnectorBinding</code> based on the specified routing ID..
     *  @return ConnectorBinding for that name
     */
    public ConnectorBinding getConnectorBindingByRoutingID(String routingID) {
   
        ConnectorBinding conn = (ConnectorBinding) conRoutingToIDMap.get(routingID);
        if (conn != null) {
            return conn;
        }
        return null;
        
    }
    
     
    
    public ServiceComponentDefn getServiceComponentDefn(ComponentDefnID componentID) {
    	
        if (svcComponents.containsKey(componentID)) {
            return ((ServiceComponentDefn) svcComponents.get(componentID));
        }
        return null;    		   	
    }
    
    /**
      * Returns a <code>ConnectorBinding</code> based on the specified name.
      *  @return ConnectorBinding for that name
      */
     public ServiceComponentDefn getServiceComponentDefn(String name) {
    
    
         Iterator it = svcComponents.keySet().iterator();  
         while (it.hasNext()) {
             ComponentDefnID id = (ComponentDefnID) it.next();
             if (id.getName().equalsIgnoreCase(name)) {
                 return ((ServiceComponentDefn) svcComponents.get(id)); 
             }  
            
         }
         return null;
        
     }
         

    /**
     * Returns a Collection of ComponentDefnID objects that are of the
     * indicated type.  Note that the type must be an exact match, no
     * component defns of a sub-type of the indicated type will be
     * returned.
     * @param componentTypeID ComponentTypeID of desired ComponentDefns
     * contained in this Configuration
     * @return Collection of ComponentDefnID objects - use
     * {@link #getComponentDefn} method to retrieve the full
     * ComponentDefn object
     */
    public Collection getComponentDefnIDs(ComponentTypeID componentTypeID){
        Collection ids = new ArrayList();
        
        Iterator allDefnsIDs = null;
        
        // find the first one that matches and return those that match
		allDefnsIDs = svcComponents.values().iterator();  
		ids = getIDs(allDefnsIDs, componentTypeID);
		
		if (ids.size() > 0) {
			return ids;
		}
		
		allDefnsIDs = connectors.values().iterator();  
		ids = getIDs(allDefnsIDs, componentTypeID);
		
		if (ids.size() > 0) {
			return ids;
		}
		
		allDefnsIDs = vms.values().iterator(); 
		ids = getIDs(allDefnsIDs, componentTypeID);
		
		if (ids.size() > 0) {
			return ids;
		}
		
	
		allDefnsIDs = pools.values().iterator();
		ids = getIDs(allDefnsIDs, componentTypeID);
		
		if (ids.size() > 0) {
			return ids;
		}
        
        allDefnsIDs = hosts.values().iterator();
        ids = getIDs(allDefnsIDs, componentTypeID);
        
        if (ids.size() > 0) {
            return ids;
        } 
        
        allDefnsIDs = authProviders.values().iterator();
        ids = getIDs(allDefnsIDs, componentTypeID);
        
        if (ids.size() > 0) {
            return ids;
        }        
		
				              	
        return Collections.EMPTY_LIST;
	
    }
    
    private Collection getIDs(Iterator allDefnsIDs, ComponentTypeID componentTypeID) {
        ArrayList ids = new ArrayList();

        ComponentDefn aDefn = null;
        while (allDefnsIDs.hasNext()){
        	aDefn = (ComponentDefn) allDefnsIDs.next();
            if (componentTypeID.equals(aDefn.getComponentTypeID())){
                ids.add(aDefn.getID());
            }
        }
        
        return ids;
    	
    }

    public Properties getDependentPropsForComponent(BaseID componentObjectID) {
        if (componentObjectID == null) {
            return new Properties();
        }
        if (componentObjectID instanceof DeployedComponentID) {
            return getDependentPropsForDeployedComp((DeployedComponentID) componentObjectID);

        } else if (componentObjectID instanceof ComponentDefnID) {
			
			if (! doesExist((ComponentDefnID) componentObjectID)) {
				return new Properties();				
            }
            // only dependent props for a defn are the Configurations
            return PropertiesUtils.clone(this.getEditableProperties());

        } else {
            return new Properties();

        }
    }

    private Properties getDependentPropsForDeployedComp(DeployedComponentID componentID)  {
        if (componentID == null) {
            return new Properties();
        }
        
        if (!deployedComponents.containsKey(componentID)) {
        	return new Properties();
        }

        BasicDeployedComponent bsc = (BasicDeployedComponent) deployedComponents.get(componentID);

        // start with the dependent properties for this deployed component
        Properties newProps = getDependentPropsForComponent(bsc.getDeployedComponentDefnID());

        // for the component that is deployed, get its definition properties
        BasicComponentDefn bcd = (BasicComponentDefn) bsc.getDeployedComponentDefn(this);
        newProps.putAll( PropertiesUtils.clone( bcd.getEditableProperties() ) );

        return newProps;
    }

    public Properties getAllPropertiesForComponent(BaseID componentObjectID)  {
        if (componentObjectID == null) {
            return new Properties();
        }
        // get the dependent properties
        Properties newProps = getDependentPropsForComponent(componentObjectID);
        // add the properties for the specified component object id
        if (componentObjectID instanceof DeployedComponentID) {
            BasicDeployedComponent bsc = (BasicDeployedComponent) deployedComponents.get(componentObjectID);
            newProps.putAll( PropertiesUtils.clone (bsc.getEditableProperties() ) );

        } else if (componentObjectID instanceof ComponentDefnID) {
        	ComponentDefn defn = getComponentDefn( (ComponentDefnID) componentObjectID);
        	BasicComponentDefn bcd = (BasicComponentDefn) defn;
            newProps.putAll( PropertiesUtils.clone (bcd.getEditableProperties() ) );
        }

        return newProps;
    }

    public Collection getDeployedComponents() {
        if (deployedComponents == null) {
            deployedComponents = new HashMap();
            return new ArrayList(1);
        }
        
        
        Collection comps = new ArrayList(deployedComponents.size());
        comps.addAll(deployedComponents.values());
//        for (Iterator it=deployedComponents.values().iterator(); it.hasNext(); ){
//            comps.add(it.next());
//        }

        return comps;
    }
    
    public DeployedComponent getDeployedComponent(DeployedComponentID deployedComponentID) {
    	
       if (!deployedComponents.containsKey(deployedComponentID)) {
 			return null;
 	   }

        BasicDeployedComponent bsc = (BasicDeployedComponent) deployedComponents.get(deployedComponentID);

	    return  bsc;   	
    }


    public Collection getDeployedComponents(ComponentDefnID componentID) {
        Collection comps = new ArrayList(deployedComponents.size());

        // verify the component is a componentDefn in this configuration
        if (doesExist(componentID)) {
            DeployedComponent dc;
            for (Iterator it=getDeployedComponents().iterator(); it.hasNext(); ){
                dc = (DeployedComponent) it.next();
                if (dc.getDeployedComponentDefnID().equals(componentID)) {
                    comps.add(dc);
                }
            }
        }

        return comps;
    }
   

    public VMComponentDefn getVMForHost(VMComponentDefnID vmID, HostID hostID) {
 
        Collection dcs = getVMsForHost(hostID);

        VMComponentDefn dc = null;

        for (Iterator it = dcs.iterator(); it.hasNext(); ) {
            dc = (VMComponentDefn) it.next();
            // 	is the component from the same host
                if (dc.getID().equals(vmID)) {
                	return dc;
                }
        }
      

        return null;

    }

    public DeployedComponent getDeployedServiceForVM(ServiceComponentDefnID serviceID, VMComponentDefn vmComponent) {
        DeployedComponent dc;
        for (Iterator it=getDeployedComponents().iterator(); it.hasNext(); ) {
            dc = (DeployedComponent) it.next();
            if (dc.getDeployedComponentDefnID().equals(serviceID) &&
                dc.getVMComponentDefnID().equals(vmComponent.getID())) {
                return dc;
            }
        }

        return null;

    }

    public DeployedComponent getDeployedServiceForVM(ServiceComponentDefnID serviceID, VMComponentDefnID vmID, HostID hostID) {
        DeployedComponent dc;
        for (Iterator it=getDeployedComponents().iterator(); it.hasNext(); ) {
            dc = (DeployedComponent) it.next();
            if (dc.getDeployedComponentDefnID().equals(serviceID) &&
                dc.getVMComponentDefnID().equals(vmID) &&
                dc.getHostID().equals(hostID)) {
                return dc;
            }
        }

        return null;

    }

    public Collection getVMsForHost(String name)  {
        HostID id = new HostID(name);
	    return getVMsForHost(id);
    }
    
    public VMComponentDefn getVMForHost(String hostname, String processName)  {

        Collection dcs = getVMsForHost(hostname);

        VMComponentDefn dc = null;

        for (Iterator it = dcs.iterator(); it.hasNext(); ) {
            dc = (VMComponentDefn) it.next();
            // is the component from the same host
                if (dc.getID().getName().equalsIgnoreCase(processName)) {
                        return dc;
                }
        }

        return null;
    }    

    /**
     *  Returns a <code>Collection</code> of type <code>DeployeComponent</code> that
     *  represent VMs.
     */
    public Collection getVMsForHost(HostID id) {
        if (id == null) {
        	return Collections.EMPTY_LIST;
        }
		Collection dcs = getVMComponentDefns();

        HashSet vms = new HashSet(dcs.size());
        VMComponentDefn dc = null;

        for (Iterator it = dcs.iterator(); it.hasNext(); ) {
            dc = (VMComponentDefn) it.next();
            if (dc.getHostID().equals(id)) {               
                    vms.add(dc);
            }
        }

        return vms;
    }
    
    /**
     * Returns a <code>Collection</code> of type <code>HostID</code> that were deployed for this configuration.
     */
    public Collection getHostIDs() {
        Collection hostIDs = new ArrayList(hosts.size());
        
        // reiterate thru the deployed compoments to obtain all the possible hosts
        for (Iterator it = hosts.keySet().iterator(); it.hasNext(); ) {
                Object o = it.next();
                hostIDs.add(o);
        }
        return hostIDs;  	 
    }
    
    public Collection getConnectorBindings() {    	
    	
        Collection results = new ArrayList(connectors.size());
        Iterator it = connectors.keySet().iterator();
        while (it.hasNext()) {
            ConnectorBindingID cID = (ConnectorBindingID) it.next();
            results.add(connectors.get(cID));
        }
        
        return results;                    
    	
    }
    
    public Collection getAuthenticationProviders() {    	
    	
        Collection results = new ArrayList(authProviders.size());
        Iterator it = authProviders.keySet().iterator();
        while (it.hasNext()) {
        	AuthenticationProviderID aID = (AuthenticationProviderID) it.next();
            results.add(authProviders.get(aID));
        }
        
        return results;                    
    	
    }

    
    public Collection getHosts() {
        Collection results = new ArrayList(hosts.size());
        Iterator it = hosts.keySet().iterator();
        while (it.hasNext()) {
            HostID hID = (HostID) it.next();
            results.add(hosts.get(hID));
        }
        
        return results;                    
    }
    
    public Host getHost(ComponentDefnID hostID)  {
        return getHost(hostID.getFullName());
        
    }
    
    /**
     * Returns a <code>Host</code> for the specified host name
     *  @return Host
     */
    public Host getHost(String hostName)  {
        if (hostName == null) {
        	return null;
        }
    
        Iterator it = hosts.keySet().iterator();
        while (it.hasNext()) {
            HostID hID = (HostID) it.next();
            if (hID.getFullName().equalsIgnoreCase(hostName)) {
                return (Host) hosts.get(hID);
            }
        }
        
        return null;                    
            
        
    }
    


    /**
     * This method is technically incorrect, because deployed services should
     * be returned for the <i>deployed</i> vm that they are deployed to,
     * <i>not</i> the vm component definition which may itself be deployed
     * many times.
      */
    public Collection getDeployedServicesForVM(VMComponentDefnID vmComponentID)  {
        if (vmComponentID == null) {
        	return Collections.EMPTY_LIST;
        }
        VMComponentDefn vm = this.getVMComponentDefn(vmComponentID);
        return getDeployedServicesForVM(vm);
        
    }

    public Collection getDeployedServicesForVM(VMComponentDefn vm)  {
        if (vm == null) {
        	return Collections.EMPTY_LIST;
        }
        VMComponentDefnID vmComponentID = (VMComponentDefnID) vm.getID();
        HostID hostID = vm.getHostID();
		Collection dcs = getDeployedComponents();

        Collection comps = new ArrayList(dcs.size());
        DeployedComponent dc;
        for (Iterator it = dcs.iterator(); it.hasNext(); ) {
            dc = (DeployedComponent) it.next();
            // if deployed component is a ServiceComponent and the VM id matches
            if (dc.getServiceComponentDefnID() != null) {
                if (dc.getVMComponentDefnID().equals(vmComponentID) &&
                    dc.getHostID().equals(hostID)) {
                    comps.add(dc);
                }
           }
        }
        return comps;
    }

    /**
     * Returns a <code>Collection</code> of DeployedComponents that
     * represent ServiceComponentDefns deployed on the indicated deployed VM,
     * and originating from the indicated ProductServiceConfig
     * @param deployedVM represents the deployed vm for which the services
     * have been deployed.
     * @param psc ProductServiceConfig which the desired deployed
     * ServiceComponentDefns belong to
     * @return Collection of DeployedComponent objects representing deployed
     * services
     * @throws InvalidArgumentException if either parameter is null
     */
//    public Collection getDeployedServices(VMComponentDefn vm, ProductServiceConfig psc) {
//        if (vm == null || psc == null) {
//        	return Collections.EMPTY_LIST;
//        }
//        VMComponentDefnID vmComponentID = (VMComponentDefnID) vm.getID();
//        HostID hostID = vm.getHostID();
//        ProductServiceConfigID pscID = (ProductServiceConfigID)psc.getID();
//        
//		Collection dcs = getDeployedComponents();
//
//        Collection comps = new ArrayList(dcs.size());
//        DeployedComponent dc;
////        ComponentDefn comp;
//        for (Iterator it = dcs.iterator(); it.hasNext(); ) {
//            dc = (DeployedComponent) it.next();
//            // if component is a ServiceComponent and the VM id matches
////            comp = dc.getDeployedComponentDefn(this);
////            if (comp instanceof ServiceComponentDefn &&
//            if (dc.getVMComponentDefnID().equals(vmComponentID) &&
//                dc.getHostID().equals(hostID) && dc.getProductServiceConfigID() != null &&
//                dc.getProductServiceConfigID().equals(pscID)) {
//                    comps.add(dc);
//            }
//        }
//        return comps;
//    }


    /**
     * Returns a Collection of ProductServiceConfig objects which have
     * ServiceComponentDefns which are deployed to the indicated VM.
     * Note that, physically speaking, only ServiceComponentDefns and
     * VMComponentDefns are "deployed", so this is just a convenience
     * method for organizing deployed services by the PSC that originated
     * them, and to then organize PSCs by the vm in which those services
     * are deployed.
     * @param deployedVM DeployedComponent representing a deployed
     * VMComponentDefn
     * @return Collection of ProductServiceConfig objects
     * @throws InvalidArgumentException if the parameter is null
     */
//    public Collection getPSCsForVM(VMComponentDefn vm){
//        Iterator deployedServices = this.getDeployedServicesForVM(vm).iterator();
//        HashSet result = new HashSet();
//        DeployedComponent dc = null;
//        while (deployedServices.hasNext()){
//            dc = (DeployedComponent)deployedServices.next();
//            result.add(this.getPSC((dc.getProductServiceConfigID())));
//        }
//        return result;
//    }

//    public Collection getPSCsForServiceDefn(ServiceComponentDefnID serviceDefnID)  {
//    	Collection result = new ArrayList(pscs.size());
//        if (serviceDefnID == null) {
//        	return Collections.EMPTY_LIST;
//        }
//        Object obj;
//        ProductServiceConfig psc = null;
//        for (Iterator iter = pscs.values().iterator(); iter.hasNext(); ){
//            obj = iter.next();
//                psc = (ProductServiceConfig)obj;
//                if (psc.getServiceComponentDefnIDs().contains(serviceDefnID)){
//                    result.add(psc);
//                }
//        }
//        return result;
//    	
//    }
    

    public Collection getComponentObjectDependencies(BaseID componentObjectID)  {
        if (componentObjectID == null) {
        	return Collections.EMPTY_LIST;
        }

        Collection deps = new ArrayList();
        
        ComponentObject co;
        
        Collection dcs = this.getDeployedComponents();
        for (Iterator it=dcs.iterator(); it.hasNext(); ) {
            co = (ComponentObject) it.next();
            if (co.isDependentUpon(componentObjectID)) {
                deps.add(co);
            }
        }
        
        /*
        * we must do this here because the ServiceComponentDefns do not contain
        * any reference to their psc parents. this means that the isDependentUpon() method
        * of the BasicServiceComponentDefn cannot check to see if it is
        * dependent upon its PSC.
        */
//        if (componentObjectID instanceof ProductServiceConfigID) {
//            ProductServiceConfigID pscID = (ProductServiceConfigID)componentObjectID;
//            ProductServiceConfig psc = (ProductServiceConfig)this.pscs.get(pscID);
//            Collection serviceComponentDefns = psc.getServiceComponentDefnIDs();
//            Iterator iterator = serviceComponentDefns.iterator();
//            while (iterator.hasNext()) {
//            	ComponentDefnID id = (ComponentDefnID) iterator.next();
//            	deps.add(getComponentDefn(id));
//            	
////                deps.add(this.getComponentDefns().get(iterator.next()));
//            }
//        }else {
        
            Map cd = this.getComponentDefns();
            for (Iterator it=cd.values().iterator(); it.hasNext(); ) {
                co = (ComponentObject) it.next();
                if (co.isDependentUpon(componentObjectID)) {
                    deps.add(co);
                }
            }
 //       }
        


        return deps;
    }
    
    /**
     * Returns a <code>Collection</code> of type <code>ComponentType</code> that represent
     * all the ComponentTypes defined.
     * @return Collection of type <code>ComponentType</code>
     * @throws ConfigurationException if an error occurred within or during communication with the Configuration Service.
     * @see #ComponentType
     */
    public Collection getResourcePools() {
        if (pools == null) {
        	return Collections.EMPTY_LIST;
        }
           
        Collection rd = new ArrayList(pools.size());
                                          
        Iterator compDefns = pools.values().iterator();
        ComponentDefn aDefn = null;

        while (compDefns.hasNext()){
            aDefn = (ComponentDefn)compDefns.next();
 			rd.add(aDefn);            
        }
                   
        return rd;
         
    }   
    
   
     /**
     * Returns a <code>Collection</code> of type <code>ServiceComponentDefn</code> that represent
     * all the ServiceComponentDefn defined.
     * @return Collection of type <code>ServiceComponentDefn</code>
     * @throws ConfigurationException if an error occurred within or during communication with the Configuration Service.
     * @see #ProductServiceConfig
     */
    public Collection getServiceComponentDefns() {
        if (svcComponents == null) {
        	return Collections.EMPTY_LIST;
        }
           
        Collection rd = new ArrayList(svcComponents.size());
                                          
        Iterator compDefns = svcComponents.values().iterator();
        ComponentDefn aDefn = null;

        while (compDefns.hasNext()){
            aDefn = (ComponentDefn)compDefns.next();
 			rd.add(aDefn);            
        }
                   
        return rd;
         
    } 
    
     /**
     * Returns a <code>Collection</code> of type <code>VMComponentDefn</code> that represent
     * all the VMComponentDefn defined.
     * @return Collection of type <code>VMComponentDefn</code>
     * @throws ConfigurationException if an error occurred within or during communication with the Configuration Service.
     * @see #ProductServiceConfig
     */
    public Collection getVMComponentDefns() {
        if (vms == null) {
        	return Collections.EMPTY_LIST;
        }
        Collection rd = new ArrayList(vms.size());
                                          
        Iterator compDefns = vms.values().iterator();
        ComponentDefn aDefn = null;

        while (compDefns.hasNext()){
            aDefn = (ComponentDefn)compDefns.next();
 			rd.add(aDefn);            
        }
                   
        return rd;
         
    }     
    /**
     * Returns a <code>ResourceDescriptor</code> for the specified <code>descriptorName</code>
     * @param descriptorName is the name of the resource descriptor to return
     * @return ResourceDescriptor is the descriptor requested
     * @throws ConfigurationException if an error occurred within or during communication with the Configuration Service.
     */
    public ResourceDescriptor getResourcePool(String poolName) {
        if (poolName == null) {
        	return null;
        }
                                               
        Iterator compDefns = pools.values().iterator();
        ResourceDescriptor aDefn = null;
        while (compDefns.hasNext()){
            aDefn = (ResourceDescriptor)compDefns.next();
            if (aDefn.getName().equalsIgnoreCase(poolName)) {
                // must break so the readlock is released                    
                return aDefn;
            }
          
        }
         
                
        return null;
        
    }
     
    
    public boolean isDeployed() {
	      return info.isDeployed();
    }
    public boolean isLocked() {
	      return info.isLocked();
    }
    public boolean isReleased() {
	      return info.isReleased();
    }
    
       
    void setInfo(ConfigurationInfo configInfo) {
        info = (BasicConfigurationInfo) configInfo;
    }

    void setIsRelease(boolean release) {
        info.setIsReleased(release);
    }

    void setIsDeployed(boolean deploy) {
        info.setIsDeployed(deploy);
    }

    void setIsLocked(boolean lock) {
        info.setIsLocked(lock);
    }

    /**
     * Add a component to list of available components that can be deployed for this configuration.
     */
    public void addComponentDefn(ComponentDefn component) {
    	
           switch(BasicUtil.getComponentType(component)){
                case ComponentType.VM_COMPONENT_TYPE_CODE:
                    vms.put(component.getID(), component);
                    return;
                case ComponentType.CONNECTOR_COMPONENT_TYPE_CODE:
                    addConnectorBinding(component);
                    return;                    
                case ComponentType.RESOURCE_COMPONENT_TYPE_CODE:
                    pools.put(component.getID(), component);
                    return;
                    
                case ComponentType.SERVICE_COMPONENT_TYPE_CODE:
                    svcComponents.put(component.getID(), component);
                    return;    
                    
                case ComponentType.AUTHPROVIDER_COMPONENT_TYPE_CODE:
                    authProviders.put(component.getID(), component);
                    return;    
                    
                    
                case ComponentType.HOST_COMPONENT_TYPE_CODE:
                    addHost((Host) component);
                    return;                     
                    
           }
           
          
				
    	// default
        
    }

    /**
     * Add a <code>Component</code> to the list of deployed components.
     */
    public void addDeployedComponent(DeployedComponent component) {
        deployedComponents.put(component.getID(), component);
    }
    
    public void addHost(Host component) {
        hosts.put(component.getID(), component);
    }
    
    private void addConnectorBinding(ComponentDefn defn) {
        ConnectorBinding cb = (ConnectorBinding) defn;
        
        connectors.put(defn.getID(), defn);
        conRoutingToIDMap.put(cb.getRoutingUUID(), defn);
        
    }
    
    /**
     * Remove the component from this list of available components that can be deployed.
     */
    public void removeComponentObject(ComponentObjectID componentID) {

        if (componentID instanceof ComponentDefnID) {
        	ComponentDefnID defnID = (ComponentDefnID) componentID;        	        	
        	
            Collection comps = getDeployedComponents( (ComponentDefnID) componentID);

            DeployedComponent dc = null;
            for (Iterator it=comps.iterator(); it.hasNext(); ) {
                dc = (DeployedComponent) it.next();
                deployedComponents.remove(dc.getID());
            }
            
            switch(BasicUtil.getComponentType(defnID)){
                case ComponentType.VM_COMPONENT_TYPE_CODE:
                    VMComponentDefn vm = getVMComponentDefn((VMComponentDefnID) componentID);                    
                    removeDeployedVM( vm);
 //                   vms.remove(defnID);
                    break;

                case ComponentType.CONNECTOR_COMPONENT_TYPE_CODE:
                     removeConnectorBinding(defnID);
                    break;
                    
                case ComponentType.SERVICE_COMPONENT_TYPE_CODE:                
                     svcComponents.remove(defnID);
                    break;
                case ComponentType.RESOURCE_COMPONENT_TYPE_CODE:
                    pools.remove(defnID);
                    break;
                case ComponentType.AUTHPROVIDER_COMPONENT_TYPE_CODE:
                    authProviders.remove(defnID);
                    break;
                case ComponentType.HOST_COMPONENT_TYPE_CODE:
                    removeHost((HostID) componentID);
                	break;
                default:
                    Assertion.assertTrue(false, "Process Error: component defn of type " + componentID.getClass().getName() + " not accounted for."); //$NON-NLS-1$ //$NON-NLS-2$

                    
            }
            

        } else if (componentID instanceof DeployedComponentID) {
        	
//        	DeployedComponent dcd = getDeployedComponent((DeployedComponentID) componentID);
//        	if (dcd != null) {
//        		// if this is a deployed VM, then the VMDefn and the deployed services 
//        		// must be removed
//        		if (!dcd.isDeployedService()) {
//        			removeDeployedVMAndDeployedServices(dcd);	
//        		}
//        	}
        	
            deployedComponents.remove(componentID);
            
        } 


    }
    
//    private void removeServiceFromPSCs(ServiceComponentDefnID defnID) {
//        if (defnID==null) {
//            return;
//        }
//    	Collection pscs = null;
//         	pscs = getPSCsForServiceDefn(defnID);
//			
//			
//	   for (Iterator it=pscs.iterator(); it.hasNext(); ) {
//     		ProductServiceConfig psc =(ProductServiceConfig) it.next();
//     		if (psc.containsService(defnID)) {
//     			BasicProductServiceConfig basicPSC = (BasicProductServiceConfig) psc;
//    			basicPSC.removeServiceComponentDefnID(defnID);
//     		}
//     	}
//    	
//    	
//    }
    
//    private void removeDeployedServicesFromVM(ProductServiceConfigID pscID) {
//    	
//    	ProductServiceConfig psc = getPSC(pscID);
//    	if (psc == null) {
//    		return;
//    	}
//        Iterator serviceDefnIDs = psc.getServiceComponentDefnIDs().iterator();
//        
//        ServiceComponentDefnID serviceID = null;
//        //delete each service definition of this PSC, one by one
//        while (serviceDefnIDs.hasNext()){
//            serviceID = (ServiceComponentDefnID)serviceDefnIDs.next();
//            
//            Collection dcs = getDeployedComponents(serviceID, (ProductServiceConfigID) psc.getID());
//            
//            if (dcs != null) {
//	            Iterator dcIter = dcs.iterator();
//	            while (dcIter.hasNext()) {
//	            	DeployedComponent comp = (DeployedComponent)dcIter.next();
//	            	removeComponentObject((ComponentObjectID)comp.getID());	
//	            }
//            }
//        }
//     	
//    }
    
    /**
     * used whend removing the complete host
     */
    private void removeVMsFromHost(HostID hostID) {
            Collection vms = getVMComponentDefns();
 
        	if (vms != null && vms.size() > 0 ) {
        		for (Iterator it=vms.iterator(); it.hasNext(); ) {
        			VMComponentDefn vmd = (VMComponentDefn) it.next();
                    if (vmd.getHostID().equals(hostID)) {
                        removeDeployedVM(vmd);
                    }
 						 						            			
          		}
            }
 	
    	
    }
    
    /**
     * used whend removing the complete host
     */
    private void removeHost(HostID hostID) {        
        removeVMsFromHost( hostID);
        hosts.remove(hostID);
                
    }    
    
    
    private void removeConnectorBinding(ComponentDefnID id) {
        ConnectorBinding cb = getConnectorBinding(id);
        if (cb != null) {
            connectors.remove(id);
            conRoutingToIDMap.remove(cb.getRoutingUUID());
        }

    }
    
    /**
     * To remove a VMComponentDefn, you must also remove
     * - the deployed VMs
     * - the deployed Services for each VM
     */
	private void removeDeployedVM(VMComponentDefn vm) {
		if (vm == null) {
			return;
		}
		
        
        removeDeployedServicesForVM(vm);
        	
        vms.remove(vm.getID());
          

		
	}
    
    /**
     * removes this deployed VM, its VMCompDefn and any
     * deployed services related to the deployed VM
     */
    private void removeDeployedServicesForVM(VMComponentDefn vm) {
    	
       	Collection dsvcs = getDeployedServicesForVM(vm);
 
	  // remove the deployed services that are deployed
		if (dsvcs != null && dsvcs.size() > 0) {
			for (Iterator sit=dsvcs.iterator(); sit.hasNext(); ) {
				DeployedComponent smd = (DeployedComponent) sit.next();
				
				deployedComponents.remove(smd.getID());
						
			}
		}
			

    }
      

    public void setDeployedComponents(Map components) {
       
    	this.deployedComponents = new HashMap(components.size());
        for (Iterator it = components.values().iterator(); it.hasNext(); ) {
            DeployedComponent dc = (DeployedComponent) it.next();
            deployedComponents.put(dc.getID(), dc);
        }
        
    }
    
    public void setConnectors(Map components) {
       
    	this.connectors = new HashMap(components.size());
        for (Iterator it = components.values().iterator(); it.hasNext(); ) {
            ComponentDefn dc = (ComponentDefn) it.next();
            addConnectorBinding(dc);
        }
        
    }
    

    public void setComponentDefns(Map components) {
    	
     	this.svcComponents = new HashMap();
    	this.vms = new HashMap();
   		this.pools = new HashMap();
   		this.connectors = new HashMap();
    	
    	
        for (Iterator it = components.values().iterator(); it.hasNext(); ) {
            ComponentDefn cd = (ComponentDefn) it.next();
            addComponentDefn( cd);
        }
    	
    }
    
    public void setHosts(Map newHosts) {
    	this.hosts = new HashMap(newHosts.size());
        for (Iterator it = newHosts.values().iterator(); it.hasNext(); ) {
            Host h = (Host) it.next();
            this.hosts.put(h.getID(), h);
        }
    	
     }

    public synchronized Object clone() {
        BasicConfiguration result = null;
            
            /**
             * NOTE: cannot use the ConfiguationObjectEditor here because
             * it in turns calls clone on the configuration, therefore
             * a recursive loop would occur.
             */
           
    	    result = new BasicConfiguration(this);
            
            BasicConfigurationInfo info = (BasicConfigurationInfo) this.getInfo().clone();
            result.setInfo(info);
            
                 
             // the cloning is done here because the set methods don't handle the cloning       
            Collection dcs =  this.getDeployedComponents();
            for (Iterator it = dcs.iterator(); it.hasNext(); ) {
                DeployedComponent dc = (DeployedComponent) it.next();
                result.addDeployedComponent( (DeployedComponent) dc.clone());
            }

            Map cds = this.getComponentDefns();
            for (Iterator it = cds.values().iterator(); it.hasNext(); ) {
                ComponentDefn cd = (ComponentDefn) it.next();
                result.addComponentDefn((ComponentDefn) cd.clone());
            }
            
//            Collection h1 = this.getHosts();
//            if (h1 != null) {
//                for (Iterator it = h1.iterator(); it.hasNext(); ) {
//                    Host h= (Host) it.next();
//                    result.addHost( (Host) h.clone());
//                }
//            }
            

        return result;
    }
 

}


