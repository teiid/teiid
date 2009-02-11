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

package com.metamatrix.common.config.model;

import java.util.*;

import com.metamatrix.common.CommonPlugin;
import com.metamatrix.common.config.api.*;
import com.metamatrix.common.config.api.exceptions.ConfigurationException;
import com.metamatrix.common.util.ErrorMessageKeys;
import com.metamatrix.core.util.ArgCheck;
import com.metamatrix.core.util.Assertion;


/**
 * The ConfigurationObjectEditorHelper provides methods that do not deal with actions.
 *
 * This is new for the implementation of using configuration models
 */

public class ConfigurationObjectEditorHelper {

     /**
     * The command to signify setting of an attribute.
     */
    public static final int SET = 0;

    /**
     * The command to signify addition of an attribute.
     */
    public static final int ADD = 1;

    /**
     * The command to signify removal of an attribute.
     */
    public static final int REMOVE = 2;

    // ----------------------------------------------------------------------------------
    //                  C R E A T E    M E T H O D S
    // ----------------------------------------------------------------------------------
   public static Host createHost(Configuration config, String hostName ) throws ConfigurationException {
		ArgCheck.isNotNull(hostName);
		ArgCheck.isNotNull(config);

        BasicHost bh = (BasicHost) BasicUtil.createComponentDefn(ComponentDefn.HOST_COMPONENT_CODE, (ConfigurationID) config.getID(), Host.HOST_COMPONENT_TYPE_ID, hostName);
//        HostID id = new HostID(hostName);
//        ComponentTypeID ctID = new ComponentTypeID(Host.COMPONENT_TYPE_NAME);

//        BasicHost bh = new BasicHost((ConfigurationID) config.getID(), id, Host.HOST_COMPONENT_TYPE_ID);


        BasicConfiguration bc = (BasicConfiguration) verifyTargetClass(config,BasicConfiguration.class);
		bc.addHost(bh);


        return bh;

    }


    // ----------------------------------------------------------------------------------
    //                  M O D I F I C A T I O N    M E T H O D S
    // ----------------------------------------------------------------------------------

    public static ComponentType setLastChangedHistory(ComponentType type, String lastChangedBy, String lastChangedDate) {
    	Assertion.isNotNull(type);

        BasicComponentType target = (BasicComponentType) verifyTargetClass(type,BasicComponentType.class);

	     	target.setLastChangedBy(lastChangedBy);
	     	target.setLastChangedDate(lastChangedDate);

     	return target;

    }
    public static ComponentType setCreationChangedHistory(ComponentType type, String createdBy, String creationDate) {
    	Assertion.isNotNull(type);

        BasicComponentType target = (BasicComponentType) verifyTargetClass(type,BasicComponentType.class);

	     	target.setCreatedBy(createdBy);
	     	target.setCreatedDate(creationDate);

     	return target;
    }


    public static ComponentObject setLastChangedHistory(ComponentObject defn, String lastChangedBy, String lastChangedDate) {
    	Assertion.isNotNull(defn);

        BasicComponentObject target = (BasicComponentObject) verifyTargetClass(defn,BasicComponentObject.class);

	    	target.setLastChangedBy(lastChangedBy);

	     	target.setLastChangedDate(lastChangedDate);

     	return target;

    }
    public static boolean set = false;
    public static ComponentObject setCreationChangedHistory(ComponentObject defn, String createdBy, String creationDate) {
    	Assertion.isNotNull(defn);

	    BasicComponentObject target = (BasicComponentObject) verifyTargetClass(defn,BasicComponentObject.class);

	     	target.setCreatedBy(createdBy);
	     	target.setCreatedDate(creationDate);


    	return target;

    }



    /**
     * Sets this ServiceComponentDefn's String routing UUID.  This method
     * will modify the ServiceComponentDefn parameter, and also create the
     * action to set the routing UUID at the remote server.
     * @param serviceComponentDefn ServiceComponentDefn to have it's routing
     * UUID modified - this instance will be locally modified, and an action
     * will also be created for execution as a transaction later on
     * @param newRoutingUUID new String routing UUID for the indicated
     * ServiceComponentDefn
     */
    public static void setRoutingUUID(ServiceComponentDefn serviceComponentDefn, String newRoutingUUID){
    	Assertion.isNotNull(serviceComponentDefn);

        BasicServiceComponentDefn basicService = (BasicServiceComponentDefn) verifyTargetClass(serviceComponentDefn,BasicServiceComponentDefn.class);
        basicService.setRoutingUUID(newRoutingUUID);

    }



    /**
     * This is a lighterweight version of the other
     * {@link #setEnabled(Configuration, ServiceComponentDefn, boolean, boolean) setEnabled}
     * method.  It simply modifies the ServiceComponentDefn parameter and
     * creates the necessary change object.  It cannot update the
     * Configuration of the ServiceComponentDefn, nor can it automatically
     * delete any DeployedComponents of the ServiceComponentDefn parameter,
     * if the ServiceComponentDefn is being disabled.  This method is only
     * needed by the JDBC spi and maybe the import/export tool.
     */

 public static void setEnabled(ServiceComponentDefnID serviceComponentDefnID, ProductServiceConfig psc, boolean enabled) {
    	Assertion.isNotNull(serviceComponentDefnID);
    	Assertion.isNotNull(psc);


        if (!psc.containsService(serviceComponentDefnID)) {
        	return;
        }

        boolean oldEnabled = psc.isServiceEnabled(serviceComponentDefnID);
        //if a change is not being made to the enabled value, this whole method
        //will be essentially bypassed
        if (enabled != oldEnabled){
            BasicProductServiceConfig basicPSC = (BasicProductServiceConfig) verifyTargetClass(psc,BasicProductServiceConfig.class);
			basicPSC.setServiceEnabled(serviceComponentDefnID, enabled);

        } //end if enabled!= oldEnabled
 }

    public static Configuration addHostComponent(Configuration t, Host host) {
		if (host == null) {
			return t;
	    }

        BasicConfiguration target = (BasicConfiguration) verifyTargetClass(t,BasicConfiguration.class);

        target.addHost(host);

        return target;
    }



    /**
     * Adds an existing ServiceComponentDefn to indicated PSC.
     * The ServiceComponentDefn will be removed from
     * any PSC it previously belonged to.

     * @param psc ProductServiceConfig to have service comp defn added to
     * @param serviceComponentDefnID will be added to the indicated
     * ProductServiceConfiguration (and removed from any PSC it previously
     * belonged to).
     */
    public static ProductServiceConfig addServiceComponentDefn(ProductServiceConfig psc, ServiceComponentDefnID serviceComponentDefnID){
    	Assertion.isNotNull(psc);
    	Assertion.isNotNull(serviceComponentDefnID);

        BasicProductServiceConfig basicPSC = (BasicProductServiceConfig) verifyTargetClass(psc,BasicProductServiceConfig.class);
        basicPSC.addServiceComponentDefnID(serviceComponentDefnID);

        return basicPSC;
    }


    public static Configuration addComponentDefn( Configuration t, ComponentDefn defn) {
        BasicConfiguration target = (BasicConfiguration) verifyTargetClass(t,BasicConfiguration.class);
        target.addComponentDefn(defn);
        return target;
    }


   public static Configuration addDeployedComponent( Configuration t, DeployedComponent deployComponent) {
        BasicConfiguration target = (BasicConfiguration) verifyTargetClass(t,BasicConfiguration.class);
        target.addDeployedComponent(deployComponent);
        return target;
    }


    public static ComponentObject addProperty( ComponentObject t, String name, String value ) {
    	Assertion.isNotNull(t);
    	Assertion.isNotNull(name);
    	Assertion.isNotNull(value);


        BasicComponentObject target = (BasicComponentObject) verifyTargetClass(t,BasicComponentObject.class);

        target.addProperty(name, value);

        return target;
    }

    public static ComponentObject setProperty( ComponentObject t, String name, String value ) {
    	Assertion.isNotNull(t);
    	Assertion.isNotNull(name);
    	Assertion.isNotNull(value);

        BasicComponentObject target = (BasicComponentObject) verifyTargetClass(t,BasicComponentObject.class);

        target.removeProperty(name);

        target.addProperty(name, value);

        return target;
    }
    
    public static ProductServiceConfig resetServices(ProductServiceConfig psc) {
        Assertion.isNotNull(psc);
        
        BasicProductServiceConfig basicPSC = (BasicProductServiceConfig) verifyTargetClass(psc,BasicProductServiceConfig.class);
        basicPSC.resetServices();

        return basicPSC;
    }

    


    public static ComponentObject removeProperty( ComponentObject t, String name) {
    	Assertion.isNotNull(t);
    	Assertion.isNotNull(name);

        BasicComponentObject target = (BasicComponentObject) verifyTargetClass(t,BasicComponentObject.class);

        target.removeProperty(name);

        return target;

    }

    public static ComponentObject modifyProperties( ComponentObject t, Properties props, int command ) {
    	Assertion.isNotNull(t);

    	if (props == null) {
    		return t;
    	}


        BasicComponentObject target = (BasicComponentObject) verifyTargetClass(t,BasicComponentObject.class);
        Properties newProps = null;

        switch ( command ) {
            case ADD:
                newProps = new Properties();
                newProps.putAll( target.getEditableProperties() );
                newProps.putAll( props );

                target.addProperties(newProps);

                break;
            case REMOVE:
                newProps = new Properties();
                newProps.putAll( target.getEditableProperties() );
                Iterator iter = props.keySet().iterator();
                while ( iter.hasNext() ) {
                    newProps.remove( iter.next() );
                }

                target.setProperties(newProps);
                break;
            case SET:
                target.setProperties(props);
                break;
        }

        return target;


    }



	public static Configuration delete( ComponentObjectID targetID, Configuration configuration ) throws ConfigurationException {
        //System.out.println("<!><!><!><!>deleting " + target + ", delete dependencies: " + deleteDependencies);

        BasicConfiguration basicConfig = (BasicConfiguration) verifyTargetClass(configuration,BasicConfiguration.class);

        if (targetID instanceof ProductServiceConfigID) {

            if (ProductServiceConfigID.ALL_STANDARD_PSC_NAMES.contains(targetID.getName())){
                throw new IllegalArgumentException(CommonPlugin.Util.getString(ErrorMessageKeys.CONFIG_ERR_0070));
            }

            basicConfig.removeComponentObject( targetID);
        } else {
        	basicConfig.removeComponentObject( targetID);
        }

        return basicConfig;
    }

    /**
     * Adds the service type represented by the indicated ComponentType to
     * the list of legal service types of the indicated ProductType.
     * @param productType ProductType to have a new service type added to
     * @param serviceComponentType ComponentType to be added to the
     * ProductType
     */
    public static ProductType addServiceComponentType(ProductType productType, ComponentType serviceComponentType){
        BasicProductType basicProdType = (BasicProductType)productType;

        ComponentTypeID productTypeID = (ComponentTypeID)basicProdType.getID();
        setParentComponentTypeID(serviceComponentType, productTypeID);
        //add the service ComponentTypeID to the BasicProductType
        basicProdType.addServiceTypeID((ComponentTypeID)serviceComponentType.getID());

        return basicProdType;
    }

    protected static ComponentType setParentComponentTypeID(ComponentType t, ComponentTypeID parentID) {

        BasicComponentType target = (BasicComponentType) verifyTargetClass(t,BasicComponentType.class);
        ComponentTypeID oldValue = target.getParentComponentTypeID();

        if ( (parentID == null && oldValue != null) ||
             ( oldValue==null ||!oldValue.equals(parentID)) ) {
            target.setParentComponentTypeID(parentID);
        }

        return target;

    }




   /**
     * Removes the service type represented by the indicated ComponentType from
     * the list of legal service types of the indicated ProductType.
     * @param productType ProductType to have the service type taken from
     * @param serviceComponentType ComponentType to be taken from the
     * ProductType
     */
    public static ProductType removeServiceComponentType(ProductType productType, ComponentType serviceComponentType){
        BasicProductType basicProdType = (BasicProductType)productType;

        //add the service ComponentTypeID to the BasicProductType
        basicProdType.removeServiceTypeID((ComponentTypeID)serviceComponentType.getID());

        return basicProdType;
    }


//    public static void renameHostAndDeployedComponents(ConfigurationModelContainer cmc, String oldHostName, String newHostName, String newPortNumber) throws ConfigurationException {
//
//        /**
//        * We must add the host controller port property to this Host to
//        * allow it to be contacted over this port by the metamatrix platform.
//        */
//        Host oldHost = cmc.getHost(oldHostName);
//        if (oldHost == null) {
//        	throw new ConfigurationException(ErrorMessageKeys.CONFIG_ERR_0070, CommonPlugin.Util.getString(ErrorMessageKeys.CONFIG_ERR_0070, new Object[] {oldHostName, cmc.getConfiguration().getName()}));
//        }
//
//
//        /**
//        * Create a new host based on that URL
//        */
//
//        Host host = createHost(cmc.getConfiguration(), newHostName);
//
//        HostID hostID = (HostID) host.getID();
//        setProperty(host, HostPropertyNames.PORT_NUMBER, newPortNumber);
//
//
//
//        Collection configObjects = cmc.getConfiguration().getDeployedComponents();
//
//        Collection deployedObjects = new ArrayList();
//        Map vmObjects = new HashMap(5);
//        Iterator iterator = configObjects.iterator();
//        while (iterator.hasNext()) {
//            Object obj = iterator.next();
//                DeployedComponent origComponent = (DeployedComponent)obj;
//				HostID hID = origComponent.getHostID();
//				if (hID.getFullName().equalsIgnoreCase(oldHostName)) {
////                	VMComponentDefnID vmID = origComponent.getVMComponentDefnID();
////                	if (origComponent.isDeployedService()) {
//                		deployedObjects.add(origComponent);
////                	} else if (!vmObjects.containsKey(vmID)) {
////						VMComponentDefn vmDefn = cmc.getConfiguration().getVMComponentDefn(vmID);
////						vmObjects.put(vmID, vmDefn);
////						deployedObjects.add(origComponent);
////                	}
//				}
//        }
//
//       delete((ComponentObjectID) oldHost.getID(), cmc.getConfiguration());
//
//       Iterator iterator2 = cmc.getConfiguration().getVMComponentDefns().iterator(); 
//           //vmObjects.keySet().iterator();
//       while (iterator2.hasNext()) {
//       		Object obj = iterator2.next();
//       		VMComponentDefnID vmDefnID = (VMComponentDefnID)obj;
//       		VMComponentDefn vmDefn = (VMComponentDefn) vmObjects.get(vmDefnID);
//       		addConfigurationComponentDefn(cmc.getConfiguration(), vmDefn);
//
//       }
//
//        Iterator iterator3 = deployedObjects.iterator();
//        while (iterator3.hasNext()) {
//            Object obj = iterator3.next();
//                DeployedComponent origComponent = (DeployedComponent)obj;
//                String depComponentName = origComponent.getName();
//                ConfigurationID configID = origComponent.getConfigurationID();
//                VMComponentDefnID vmID = origComponent.getVMComponentDefnID();
//                ServiceComponentDefnID defnID = origComponent.getServiceComponentDefnID();
//                ProductServiceConfigID pscID = origComponent.getProductServiceConfigID();
//                ComponentTypeID typeID = origComponent.getComponentTypeID();
//
//                /**
//                * We need to create a new Deployed ComponentID as the Host
//                * name is incorporated into the ID name of every Deployed
//                * Component.
//                */
//                DeployedComponentID newID = null;
//                if (origComponent.getServiceComponentDefnID() == null) {
//               
//                    newID= new DeployedComponentID(depComponentName,
//                                                                    configID,
//                                                                    hostID,
//                                                                    vmID);
//                } else {
//                    
//                    newID= new DeployedComponentID(depComponentName,
//                                                                    configID,
//                                                                    hostID,
//                                                                    vmID,
//                                                                    pscID,
//                                                                    defnID);                                                                               
//                }
//                    /**
//                * Here we create a new DeployedComponent that is a clone of
//                * the original DeployedComponent in the list except for we
//                * switch out the HostID to reflect the new Host in the new
//                * server install.
//                */
//                BasicDeployedComponent newComponent = new BasicDeployedComponent(
//                                        newID,
//                                        configID,
//                                        hostID,
//                                        vmID,
//                                        defnID,
//                                        pscID,
//                                        typeID
//                                        );
//                /**
//                * Now we remove the original deployed component from the list of
//                * objects to be commited to the database and add the modified
//                * deployed component to the list of objects to be commited
//                * to the configuraiton database.
//                */
//
//				addDeployedComponent(cmc.getConfiguration(), newComponent);
//
//
//        }
//
//
//
//    }

    public static Configuration addConfigurationComponentDefn(Configuration t, ComponentDefn defn ) {
		Assertion.isNotNull(defn);

        BasicConfiguration target = (BasicConfiguration) verifyTargetClass(t,BasicConfiguration.class);
		target.addComponentDefn(defn);

        return target;
   }

    public static Configuration addConfigurationDeployedComponent(Configuration t, DeployedComponent deployedComponent) {
		Assertion.isNotNull(deployedComponent);

        BasicConfiguration target = (BasicConfiguration) verifyTargetClass(t,BasicConfiguration.class);

		target.addDeployedComponent(deployedComponent);
        return target;
    }
    public static Configuration addConfigurationHostComponent(Configuration t, Host host) {
		Assertion.isNotNull(host);

        BasicConfiguration target = (BasicConfiguration) verifyTargetClass(t,BasicConfiguration.class);
		target.addHost(host);
        return target;
    }
    
    public static Configuration addAuthenticationProviderComponent(Configuration t, AuthenticationProvider provider) {
		Assertion.isNotNull(provider);

        BasicConfiguration target = (BasicConfiguration) verifyTargetClass(t,BasicConfiguration.class);
		target.addComponentDefn(provider);
        return target;
    }
    


    public static Properties getEditableProperties(ComponentObject t) {
           BasicComponentObject bco = (BasicComponentObject) verifyTargetClass(t,BasicComponentObject.class);
           return bco.getEditableProperties();
    }




    // ----------------------------------------------------------------------------------
    // P R I V A T E
    // ----------------------------------------------------------------------------------

    /**
     * Subclass helper method that simply verifies that the specified target is either an instance of
     * the specified class (or interface).
     * @param target the target or target identifier.
     * @param requiredClass the class/interface that the target must be an instance of.
     * @return the target object (for convenience)
     * @throws IllegalArgumentException if either the target is not an instance of the specified class.
     */
    protected static Object verifyTargetClass( Object target, Class requiredClass ) throws IllegalArgumentException {
        if ( ! requiredClass.isInstance(target) ) {
            throw new IllegalArgumentException(CommonPlugin.Util.getString(ErrorMessageKeys.CONFIG_ERR_0072, requiredClass.getName()) );
        }
        return target;
    }


}



