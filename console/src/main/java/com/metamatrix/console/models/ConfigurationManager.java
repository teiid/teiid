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

//#############################################################################
package com.metamatrix.console.models;

import java.io.FileInputStream;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import com.metamatrix.admin.AdminMessages;
import com.metamatrix.admin.AdminPlugin;
import com.metamatrix.admin.api.exception.security.InvalidSessionException;
import com.metamatrix.api.exception.MetaMatrixComponentException;
import com.metamatrix.api.exception.security.AuthorizationException;
import com.metamatrix.common.actions.ActionDefinition;
import com.metamatrix.common.actions.ModificationActionQueue;
import com.metamatrix.common.config.api.ComponentObject;
import com.metamatrix.common.config.api.ComponentTypeDefn;
import com.metamatrix.common.config.api.ComponentTypeID;
import com.metamatrix.common.config.api.Configuration;
import com.metamatrix.common.config.api.ConfigurationID;
import com.metamatrix.common.config.api.ConfigurationModelContainer;
import com.metamatrix.common.config.api.ConfigurationObjectEditor;
import com.metamatrix.common.config.api.ConnectorBinding;
import com.metamatrix.common.config.api.DeployedComponent;
import com.metamatrix.common.config.api.Host;
import com.metamatrix.common.config.api.HostID;
import com.metamatrix.common.config.api.ResourceDescriptor;
import com.metamatrix.common.config.api.ServiceComponentDefn;
import com.metamatrix.common.config.api.ServiceComponentDefnID;
import com.metamatrix.common.config.api.VMComponentDefn;
import com.metamatrix.common.config.api.VMComponentDefnID;
import com.metamatrix.common.config.api.VMComponentDefnType;
import com.metamatrix.common.config.api.exceptions.ConfigurationException;
import com.metamatrix.common.config.model.BasicConfigurationObjectEditor;
import com.metamatrix.common.config.xml.XMLConfigurationImportExportUtility;
import com.metamatrix.common.log.LogManager;
import com.metamatrix.common.namedobject.BaseID;
import com.metamatrix.common.object.PropertiedObject;
import com.metamatrix.common.object.PropertyDefinition;
import com.metamatrix.console.connections.ConnectionInfo;
import com.metamatrix.console.ui.views.deploy.event.ConfigurationChangeEvent;
import com.metamatrix.console.ui.views.deploy.event.ConfigurationChangeListener;
import com.metamatrix.console.util.ExternalException;
import com.metamatrix.console.util.LogContexts;
import com.metamatrix.console.util.StaticUtilities;
import com.metamatrix.core.MetaMatrixRuntimeException;
import com.metamatrix.core.util.Assertion;
import com.metamatrix.platform.admin.api.ConfigurationAdminAPI;

/**
 * The <code>ConfigurationManager</code> is used as an abstraction layer between
 * the GUI and the {@link ConfigurationAdminAPI}.
 * @since Golden Gate
 * @version 1.0
 * @author Dan Florian
 */
public final class ConfigurationManager
    extends Manager implements ManagerListener {

/************ TO DO & ISSUES **********

- in theory the startup config won't change so shouldn't have to refresh it

***************************************/

    ///////////////////////////////////////////////////////////////////////////
    // CONSTANTS
    ///////////////////////////////////////////////////////////////////////////

    ///////////////////////////////////////////////////////////////////////////
    // FIELDS
    ///////////////////////////////////////////////////////////////////////////

    private ConfigurationObjectEditor configEditor;

    private ConfigurationModelContainer config = null;

    //key=HostID
    //value=HashMap (key=ConfigID, value=Collection of VMComponentDefnIDs)
 //   private HashMap hostDeployments = new HashMap();
    
    // Key=VMComponentDefn id
    // value=Collection of ProductServiceConfigIDs
//    private HashMap deployedPscs = new HashMap();
    
    // Key=ProductServiceConfig id
    // value=(key=VMComponentDefnID value=Collection of DeployedComponentID-services)
    // ProductServiceConfigID -> {VMComponentDefnID -> ArrayList[DeployedComponentID]}
 //   private HashMap deployedServices = new HashMap();
    
    //Key=id, value=ProductType/
//    private HashMap products = new HashMap();

    //key=ProductType ID, value=HashMap (key=ConfigID, value=Collection of psc def IDs)
//    private HashMap prodPscDefs = new HashMap();

    //key=ProductServiceConfigID id, value=value=Collection of service def IDs
//    private HashMap serviceDefnMap = new HashMap();

    private ConfigurationID nextStartUpId = Configuration.NEXT_STARTUP_ID;

    private ArrayList listeners = new ArrayList();

    /**
     * Set to indicate to the DeployMainPanel that it should refresh.
     */
    private boolean refreshNeeded = false;
    
    
    /**
     * <p>
     * "ALL" TYPE DEFINITIONS CACHE
     * </p>
     * <p>
     * Only used internally in this class by methods used by {@link ConfigurationPropertiedObjectEditor}. The two package-level
     * methods are {@link #getAllCachedComponentTypeDefinitions} and {@link #getComponentTypeDefn}.
     * </p>
     * <p>
     * This cache is updated by the {@link #getAllComponentTypeDefinitions} method, and it is cleared whenever either
     * executeTransaction method is called: {@link #executeTransaction(List) executeTransaction} or
     * {@link #executeTransaction(ActionDefinition) executeTransaction).</p>
     */
    private Map typeIDtoAllTypeDefns = new HashMap();

    
    

    ///////////////////////////////////////////////////////////////////////////
    // CONSTRUCTORS
    ///////////////////////////////////////////////////////////////////////////

    /**
     * Constructs a <code>ConfigurationManager</code>.
     * @throws ExternalException if a problem occurs during construction.
     */
    public ConfigurationManager(ConnectionInfo connection)
        	throws ExternalException {
		super(connection);
        init();
   		refreshImpl();

    // listen for SystemProperty changes
    }



//////////////////////////////////////////////////////////////////////////    /
//     METHODS
//////////////////////////////////////////////////////////////////////////    /

    /** 
     * @see com.metamatrix.console.models.ManagerListener#modelChanged(com.metamatrix.console.models.ModelChangedEvent)
     * @since 4.3
     */
    public void modelChanged(ModelChangedEvent e) {
        if (e.getMessage().equals(Manager.MODEL_CHANGED)){
            try {
                this.refreshConfigs();
            } catch (ExternalException ee) {
                LogManager.logCritical(
                    LogContexts.CONFIG,
                    "ConfigurationManager.refreshConfigs:" + //$NON-NLS-1$
                        "Error refreshing configuration."); //$NON-NLS-1$
                
            }
            
        }        
    }
    

    ///////////////////////////////////////////////////////////////////////////
    // METHODS
    ///////////////////////////////////////////////////////////////////////////

    /**
     * Adds the given listener to those being notified.
     * @param theListener the listener who wants to be notified
     */
    public void addConfigurationChangeListener(
        ConfigurationChangeListener theListener) {

        listeners.add(theListener);
    }
    
    private void notifyHostChangeToConfigs(Host theHost, int eventType) {
             fireConfigurationChange(
                new ConfigurationChangeEvent(
                    eventType,
                    theHost,
                    config.getConfiguration(),
                    new Object[] { config.getConfiguration()}));

    }

    /**
     * Changes the deployed PSC for a given process.
     * @param theOldPsc the deployed PSC being deleted
     * @param theNewPsc the PSC being deployed
     * @param theProcess the process the PSCs belong to
     * @param theConfigId the ID of the configuration
     */
//    public void changeDeployedPsc(
//        ProductServiceConfig theOldPsc,
//        ProductServiceConfig theNewPsc,
//        VMComponentDefn theProcess,
//        Host theHost,
//        ConfigurationID theConfigId)
//        throws ExternalException {
//
////??// this should really be wrapped in a transaction!!!!
//        deleteDeployedPsc(theOldPsc, theProcess, theHost, theConfigId);
//        deployPsc(theNewPsc, theProcess, theHost, theConfigId);
//    }

    public void commitImportedObjects(Collection theImportedObjects)
        throws ExternalException {

        try {
            
            
            ConfigurationObjectEditor importEditor = getAPI().createEditor();
            // must first delete the current next startup configuration
            // to avoid an error condition because of constraints on the
            // tables in the database
			
			importEditor.delete(nextStartUpId);
			importEditor.createConfiguration(nextStartUpId, theImportedObjects);			
			getAPI().executeTransaction(importEditor.getDestination().getActions());
        } catch (Exception theException) {
            throw new ExternalException(
                formatErrorMsg("commitImportedObjects", theException), //$NON-NLS-1$
                theException);
        }
    }

    /**
     * Creates a host.
     * @param theHostName the name of the new host
     * @return the new host or <code>null</code> if host already exists
     * @throws ExternalException if problem occurs creating the host
     */
    public Host createHost(String theHostName)
        throws ExternalException {

        Host host = null;

        // check to see if host exists in cache
        // if it does return null to signify
        Host testHost = getHost(theHostName, Configuration.NEXT_STARTUP_ID); 
        if (testHost != null) {
            return testHost;
        }
        ConfigurationObjectEditor editor = null;
        try {
            Configuration config = getConfig(Configuration.NEXT_STARTUP_ID);
            editor = getEditor();
            host = editor.createHost(config, theHostName);
            // set port to default
            Properties defaultProps = this.getConfigModel(Configuration.NEXT_STARTUP_ID).getDefaultPropertyValues(host.getComponentTypeID());
            
//            Properties props = new Properties();
//            props.setProperty(PropertyConstants.PORT_PROP, ""+15001); //$NON-NLS-1$
            host = (Host)
                editor.modifyProperties(host,
                                        defaultProps,
                                        ConfigurationObjectEditor.SET);
            getAPI().executeTransaction(editor.getDestination().popActions());

            notifyHostChangeToConfigs(host, ConfigurationChangeEvent.NEW);
        } catch (Exception theException) {
            // rollback
            if (editor != null) {
                editor.getDestination().popActions();
            }
            throw new ExternalException(
                formatErrorMsg("createHost", "host=" + theHostName, theException), //$NON-NLS-1$ //$NON-NLS-2$
                theException);
        }
        return host;
    }

    /**
     * Creates a process.
     * @param theProcessName the name of the new process
     * @param theHost the host to add the process to
     * @param theConfigId the ID of the {@link Configuration} to add the
     * process to.
     * @throws ExternalException if problem occurs creating the process
     */
    public VMComponentDefn createProcess(
        String theProcessName,
        String portNumber,
        Host theHost,
        ConfigurationID theConfigId)
        throws ExternalException {

        ConfigurationObjectEditor editor = null;
        try {
            Configuration config = getConfig(theConfigId);
            editor = getEditor();

             // create defn first
            VMComponentDefn processDefn =
                editor.createVMComponentDefn(
                    config, (HostID) theHost.getID(), VMComponentDefn.VM_COMPONENT_TYPE_ID, theProcessName);

            ConfigurationModelContainer cmc = getConfigModel(theConfigId);
            Properties defaultProps = cmc.getDefaultPropertyValues(processDefn.getComponentTypeID());
            
            defaultProps.putAll(processDefn.getProperties());
            
            // set the defaults for min and max heap size to the system global property setting
            String min = cmc.getConfiguration().getProperty(VMComponentDefnType.VM_MINIMUM_HEAP_SIZE_PROPERTY_NAME);
            String max = cmc.getConfiguration().getProperty(VMComponentDefnType.VM_MAXIMUM_HEAP_SIZE_PROPERTY_NAME);
            if (min != null && min.length() > 0)
            {
                defaultProps.setProperty(VMComponentDefnType.VM_MINIMUM_HEAP_SIZE_PROPERTY_NAME, min);
            }
            if (max != null && max.length() > 0)
            {
                defaultProps.setProperty(VMComponentDefnType.VM_MAXIMUM_HEAP_SIZE_PROPERTY_NAME, max);
            }
            if (portNumber != null && portNumber.length() > 0) {
                defaultProps.setProperty(VMComponentDefnType.SERVER_PORT, portNumber);
            }
              
            processDefn = (VMComponentDefn) editor.modifyProperties(processDefn, defaultProps, ConfigurationObjectEditor.SET);
            
            // create deployed component next
//            editor.createDeployedVMComponent(processDefn.getName(),
//                                             config,
//                                             (HostID)theHost.getID(),
//                                             processDefn);
            getAPI().executeTransaction(editor.getDestination().popActions());
            
            // update local cache
//            HashMap map = (HashMap)hostDeployments.get(theHost.getID());
//            if (map == null) {
//                // this is first deployed process
//                map = new HashMap();
//                hostDeployments.put(theHost.getID(), map);
//            }
//            Collection procs = (Collection)map.get(theConfigId);
//            if (procs == null) {
//                // first deployed process
//                procs = new ArrayList(1);
//                map.put(theConfigId, procs);
//            }
 //           procs.add(processDefn.getID());            

            // notify listeners
            fireConfigurationChange(
                new ConfigurationChangeEvent(
                    ConfigurationChangeEvent.NEW,
                    processDefn,
                    config,
                    new Object[] {theHost, config}));
            return processDefn;
        } catch (Exception theException) {
            // rollback
            if (editor != null) {
                editor.getDestination().popActions();
            }
            throw new ExternalException(
                formatErrorMsg("createProcess", //$NON-NLS-1$
                               "process=" + theProcessName + //$NON-NLS-1$
                                   ", host=" + theHost + //$NON-NLS-1$
                                   ", config=" + theConfigId, //$NON-NLS-1$
                               theException),
                theException);
        }
    }

    /**
     * Creates a PSC definition. New PSC definitions are created by copying
     * an existing definition.
     * @param thePscDefName the name of the new PSC definition
     * @param thePscBeingCopied the PSC being copied
     * @param theConfigId the ID of the {@link Configuration} the PSC
     * definition is contained in.
     * @throws ExternalException if problem occurs creating the PSC definition
     */
//    public ProductServiceConfig copyPscDef(
//        String thePscDefName,
//        ProductServiceConfig thePscBeingCopied,
//        ConfigurationID theConfigId)
//        throws ExternalException {
//
//        ConfigurationObjectEditor editor = null;
//        String attemptedAction = ""; //$NON-NLS-1$
//        try {
//            Configuration config = getConfig(theConfigId);
//            editor = getEditor();
//            attemptedAction = "createProductServiceConfig()"; //$NON-NLS-1$
//            ProductServiceConfig pscDef =
//                editor.createProductServiceConfig(
//                    config, thePscBeingCopied, thePscDefName);
//            attemptedAction = "executeTransaction()"; //$NON-NLS-1$
//            getAPI().executeTransaction(editor.getDestination().popActions());
//            
//            // update cache
//            ProductType product = BasicProductType.PRODUCT_TYPE;
//            Map map = (Map)prodPscDefs.get(product.getID());
//            attemptedAction = "map.get()"; //$NON-NLS-1$
//            Collection pscDefs = (Collection)map.get(theConfigId);
//            if (pscDefs == null) {
//                pscDefs = new ArrayList();
//            }
//            attemptedAction = "pscDefs.add()"; //$NON-NLS-1$
//            pscDefs.add(pscDef.getID());            
//            
//            // notify listeners
//            attemptedAction = "fireConfigurationChange()"; //$NON-NLS-1$
//            fireConfigurationChange(
//                new ConfigurationChangeEvent(
//                    ConfigurationChangeEvent.NEW,
//                    pscDef,
//                    config,
//                    new Object[] {BasicProductType.PRODUCT_TYPE, config}));
//
//            // add Service defns
//            attemptedAction = "getServiceDefinitions()"; //$NON-NLS-1$
//            getServiceDefinitions(pscDef, config);
//            return pscDef;
//        } catch (Exception theException) {
//            // rollback
//            if (editor != null) {
//                editor.getDestination().popActions();
//            }
//            throw new ExternalException(
//                formatErrorMsg("createPscDef", //$NON-NLS-1$
//                               "psc name=" + thePscDefName + //$NON-NLS-1$
//                                   ", source PSC=" + thePscBeingCopied + //$NON-NLS-1$
//                                   ", config=" + theConfigId + //$NON-NLS-1$
//                                   ", attempted action = " + attemptedAction, //$NON-NLS-1$
//                               theException),
//                theException);
//        }
//    }
    
    /**
     * Updates a PSC definition. The the service IDs will replace the current
     * services in the psc.
     * @param thePscDef the PSC definition to be updated
     * @param theServiceIds the service IDs to replace current service IDs
     * @throws ExternalException if problem occurs updating the PSC definition
     */
//    public ProductServiceConfig updatePscDef(
//        ProductServiceConfig thePscDef,
//        Collection theServiceIds)
//        throws ExternalException {  
//            
//            ConfigurationObjectEditor editor = null;
//            String attemptedAction = ""; //$NON-NLS-1$
//            try {
//                Configuration config = getConfig(Configuration.NEXT_STARTUP_ID);
//                editor = getEditor();
//                attemptedAction = "updateProductServiceConfig()"; //$NON-NLS-1$
//                
//                thePscDef = editor.updateProductServiceConfig(config, thePscDef, theServiceIds);
//                
//                attemptedAction = "executeTransaction()"; //$NON-NLS-1$
//                getAPI().executeTransaction(editor.getDestination().popActions());
//                
//                ProductType product = BasicProductType.PRODUCT_TYPE;
//                	//getProduct(thePscDef);
//                Map map = (Map)prodPscDefs.get(product.getID());
//                attemptedAction = "map.get()"; //$NON-NLS-1$
//                Collection pscDefs = (Collection)map.get(config.getID());
//                if (pscDefs == null) {
//                    pscDefs = new ArrayList();
//                }
//                attemptedAction = "pscDefs.updated()"; //$NON-NLS-1$
//                pscDefs.add(thePscDef.getID());
//                
//                // notify listeners
//                attemptedAction = "fireConfigurationChange()"; //$NON-NLS-1$
//                fireConfigurationChange(
//                    new ConfigurationChangeEvent(
//                        ConfigurationChangeEvent.MODIFIED,
//                        thePscDef,
//                        config,
//                        new Object[] {BasicProductType.PRODUCT_TYPE, config}));
//
//                // add Service defns
//                attemptedAction = "getServiceDefinitions()"; //$NON-NLS-1$
//                getServiceDefinitions(thePscDef, config);
//                return thePscDef;
//           } catch (Exception theException) {
//                // rollback
//                if (editor != null) {
//                    editor.getDestination().popActions();
//                }
//                throw new ExternalException(
//                    formatErrorMsg("updatePscDef", //$NON-NLS-1$
//                                   "psc name=" + thePscDef.getName() + //$NON-NLS-1$
//                                       ", prodType=" + thePscDef.getComponentTypeID() + //$NON-NLS-1$
//                                       ", config=" + Configuration.NEXT_STARTUP_ID + //$NON-NLS-1$
//                                       ", attempted action = " + attemptedAction, //$NON-NLS-1$
//                                   theException),
//                    theException);
//            }
//            
//        }  
    
    /**
     * Creates a PSC definition. New PSC definitions are created by copying
     * an existing definition.
     * @param thePscDefName the name of the new PSC definition
     * @param thePscBeingCopied the PSC being copied
     * @param theConfigId the ID of the {@link Configuration} the PSC
     * definition is contained in.
     * @throws ExternalException if problem occurs creating the PSC definition
     */
//    public ProductServiceConfig createPscDef(
//        String thePscDefName,
//        ProductTypeID thePscProdTypeID,
//        Collection theServiceIds,
//        ConfigurationID theConfigId)
//        throws ExternalException {
//
//        ConfigurationObjectEditor editor = null;
//        String attemptedAction = ""; //$NON-NLS-1$
//        try {
//            Configuration config = getConfig(theConfigId);
//            editor = getEditor();
//            attemptedAction = "createProductServiceConfig()"; //$NON-NLS-1$
//            ProductServiceConfig pscDef = 
//            	editor.createProductServiceConfig(config, thePscProdTypeID, thePscDefName);
//            	
//            	
//           	for (Iterator sidIt=theServiceIds.iterator(); sidIt.hasNext(); ) {
//           		ServiceComponentDefnID id = (ServiceComponentDefnID) sidIt.next();
//           		
//            	editor.addServiceComponentDefn(pscDef, id);
//           	}
//            	
//            attemptedAction = "executeTransaction()"; //$NON-NLS-1$
//            getAPI().executeTransaction(editor.getDestination().popActions());
//            
//            ProductType product = BasicProductType.PRODUCT_TYPE;
//            Map map = (Map)prodPscDefs.get(product.getID());
//            attemptedAction = "map.get()"; //$NON-NLS-1$
//            Collection pscDefs = (Collection)map.get(theConfigId);
//            if (pscDefs == null) {
//                pscDefs = new ArrayList();
//            }
//            attemptedAction = "pscDefs.add()"; //$NON-NLS-1$
//            pscDefs.add(pscDef.getID());
//                        
//            // notify listeners
//            attemptedAction = "fireConfigurationChange()"; //$NON-NLS-1$
//            fireConfigurationChange(
//                new ConfigurationChangeEvent(
//                    ConfigurationChangeEvent.NEW,
//                    pscDef,
//                    config,
//                    new Object[] {BasicProductType.PRODUCT_TYPE, config}));
//
//            // add Service defns
//            attemptedAction = "getServiceDefinitions()"; //$NON-NLS-1$
//            getServiceDefinitions(pscDef, config);
//            return pscDef;
//        } catch (Exception theException) {
//            // rollback
//            if (editor != null) {
//                editor.getDestination().popActions();
//            }
//            throw new ExternalException(
//                formatErrorMsg("createPscDef", //$NON-NLS-1$
//                               "psc name=" + thePscDefName + //$NON-NLS-1$
//                               	   ", prodType=" + thePscProdTypeID + //$NON-NLS-1$
//                                   ", config=" + theConfigId + //$NON-NLS-1$
//                                   ", attempted action = " + attemptedAction, //$NON-NLS-1$
//                               theException),
//                theException);
//        }
//    }
    

    /**
     * Deletes a component object.
     * @param theObject the object being deleted
     * @param theConfigId the ID of the {@link Configuration} the object
     * exists in.
     * @throws ExternalException if problem occurs deleting the object
     */
    private void delete(
        ComponentObject theObject,
        ConfigurationID theConfigId,
        boolean theDeleteDependenciesFlag)
        throws Exception {

        ConfigurationObjectEditor editor = null;
        try {
            editor = getEditor();
                    
                editor.delete(theObject,
                              getConfig(theConfigId),
                              theDeleteDependenciesFlag);
            // the editor won't have any actions if the ComponentObject
            // being deleted does not have any DeployedComponents under it,
            // a NPE is thrown if executeTransaction() is called
            if (editor.getDestination().getActionCount() != 0) {
            	getAPI().executeTransaction(editor.getDestination().popActions());
            }
        }
        catch (Exception theException) {
            // rollback
            if (editor != null) {
                editor.getDestination().popActions();
            }
            throw theException;
        }
    }

//    public void deleteDeployedPsc(
//        ProductServiceConfig thePsc,
//        VMComponentDefn theProcess,
//        Host theHost,
//        ConfigurationID theConfigId)
//        throws ExternalException {
//
//        ConfigurationObjectEditor editor = null;
//        try {
//            // since PSCs are not really deployed, must get all their deployed
//            // services and delete them
//            Configuration config = getConfig(theConfigId);
//            // each process has only one deployed component
////            Collection deployedComps =
////                config.getDeployedComponents((ComponentDefnID)theProcess.getID());
////            if ((deployedComps == null) || (deployedComps.isEmpty())) {
////                throw new IllegalStateException(
////                    "ConfigurationManager.deleteDeployedPsc:" + //$NON-NLS-1$
////                        " VM does not have a deployed component. PSC=" + thePsc + //$NON-NLS-1$
////                        ", process=" + theProcess + ", config=" + theConfigId); //$NON-NLS-1$ //$NON-NLS-2$
////            }
////            DeployedComponent deployedVm =
////                (DeployedComponent)deployedComps.iterator().next();
//            Collection services = config.getDeployedServices(theProcess, thePsc);
//            if (services != null) {
//                editor = getEditor();
//                Iterator servItr = services.iterator();
//                while (servItr.hasNext()) {
//                    ComponentObject service = (ComponentObject)servItr.next();
//                    editor.delete(service, config, true);
//
//                }
//                // persist delete
//                getAPI().executeTransaction(editor.getDestination().popActions());
//                
//                Collection pscs = (Collection)deployedPscs.get(theProcess.getID());
//                pscs.remove(thePsc.getID());
//                HashMap map = (HashMap)deployedServices.get(thePsc.getID());
//                map.remove(theProcess.getID());
//                
//
//                // notify listeners
//                fireConfigurationChange(
//                    new ConfigurationChangeEvent(
//                        ConfigurationChangeEvent.DELETED,
//                        thePsc,
//                        config,
//                        new Object[] {theProcess, theHost, config}));
//            }
//        } catch (Exception theException) {
//            // rollback
//            if (editor != null) {
//                editor.getDestination().popActions();
//            }
//            throw new ExternalException(
//                formatErrorMsg("deleteDeployedPsc", //$NON-NLS-1$
//                               "PSC=" + thePsc + ", process=" + theProcess + //$NON-NLS-1$ //$NON-NLS-2$
//                                   ", host=" + theHost + //$NON-NLS-1$
//                                   ", config=" + theConfigId, //$NON-NLS-1$
//                               theException),
//                theException);
//        }
//    }

    public void deleteHost(
        Host theHost,
        ConfigurationID theConfigId)
        throws ExternalException {

        try {
                ConfigurationObjectEditor editor = getEditor();
 				// delete only the host, the process logic will remove all the
 				// dependencies
                editor.delete(theHost);
                getAPI().executeTransaction(editor.getDestination().popActions());

                // notify listeners that host has been deleted
                // need to do this for each config
                deleteHostFromConfigs(theHost);

                LogManager.logDetail(
                    LogContexts.PSCDEPLOY,
                    "ConfigurationManager.deleteHost:" + theHost); //$NON-NLS-1$
        }
        catch (Exception theException) {
            throw new ExternalException(
                formatErrorMsg("deleteHost", //$NON-NLS-1$
                               "host=" + theHost +", config=" + theConfigId, //$NON-NLS-1$ //$NON-NLS-2$
                               theException),
                theException);
        }
    }

    private void deleteHostFromConfigs(Host theHost) {
             fireConfigurationChange(
                new ConfigurationChangeEvent(
                    ConfigurationChangeEvent.DELETED,
                    theHost,
                    config.getConfiguration(),
                    new Object[] {config.getConfiguration()}));

    }

    public void deleteProcess(
        VMComponentDefn theProcess,
        ConfigurationID theConfigId)
        throws ExternalException {

        try {
            Configuration config = getConfig(theConfigId);
            // must delete deployed processes first
//            Collection deployedComps =
//                config.getDeployedComponents((ComponentDefnID)theProcess.getID());
//            // should only be one deployed process
//            DeployedComponent deployedComp = 
//                (DeployedComponent)deployedComps.iterator().next();
            HostID hostId = theProcess.getHostID();
            
            // delete the deployed VM, the process logic will handle
            // removing all dependencies, regardless of the boolean argument
            delete(theProcess, theConfigId, false);

            // update caches
            BaseID processID = theProcess.getID();
            
//            Iterator pscItr = deployedServices.values().iterator();
//            while (pscItr.hasNext()) {
//                Map pscToSvcMap = (Map) pscItr.next();
//                pscToSvcMap.remove(processID);
//            }

//            HashMap map = (HashMap)hostDeployments.get(hostId);
//            Collection procs = (Collection)map.get(theConfigId);
//            if (procs != null) {
//                procs.remove(processID);
//            }
            
            // notify listeners
            Host host = config.getHost(hostId.getFullName());
            
            fireConfigurationChange(
                new ConfigurationChangeEvent(
                    ConfigurationChangeEvent.DELETED,
                    theProcess,
                    config,
                    new Object[] {host, config}));
                    
                   
            //fireModelChangedEvent(MODEL_CHANGED);
        } catch (Exception theException) {
            throw new ExternalException(
                formatErrorMsg("deleteProcess", //$NON-NLS-1$
                               "process=" + theProcess + ", config=" + theConfigId, //$NON-NLS-1$ //$NON-NLS-2$
                               theException),
                theException);
        }
    }

//    public void deletePscDefinition(
//        ProductServiceConfig thePsc,
//        ProductType theProduct,
//        ConfigurationID theConfigId)
//        throws ExternalException {
//
//        try {
//            delete(thePsc, theConfigId, false);
//            
//            // update cache
//            Map map = (Map)prodPscDefs.get(theProduct.getID());
//            Collection pscDefs = (Collection)map.get(theConfigId);
//            if (pscDefs != null) {
//                pscDefs.remove(thePsc.getID());            
//            }
//
//            // notify listeners
//            fireConfigurationChange(
//                new ConfigurationChangeEvent(
//                    ConfigurationChangeEvent.DELETED,
//                    thePsc,
//                    getConfig(theConfigId),
//                    new Object[] {theProduct,
//                                  getConfig(theConfigId)}));
//        } catch (Exception theException) {
//            throw new ExternalException(
//                formatErrorMsg("deletePscDefinition", //$NON-NLS-1$
//                               "PSC=" + thePsc + //$NON-NLS-1$
//                                   ", product=" + theProduct + //$NON-NLS-1$
//                                   ", config=" + theConfigId, //$NON-NLS-1$
//                               theException),
//                theException);
//        }
//    }

    // creates a new PSC under a process
//    public void deployPsc(
//        ProductServiceConfig thePsc,
//        VMComponentDefn theProcess,
//        Host theHost,
//        ConfigurationID theConfigId)
//        throws ExternalException {
//
//        ConfigurationObjectEditor editor = null;
//        try {
//            editor = getEditor();
//            Configuration config = getConfig(theConfigId);
//            Collection result =
//                editor.deployProductServiceConfig(
//                    config,
//                    thePsc,
//                    (HostID)theHost.getID(),
//                    (VMComponentDefnID)theProcess.getID());
//            getAPI().executeTransaction(editor.getDestination().popActions());
//            
//            // update cache
//            Collection pscs = (Collection)deployedPscs.get(theProcess.getID());
//            if (pscs == null) {
//                // process first deployed psc
//                pscs = new ArrayList();
//                deployedPscs.put(theProcess.getID(), pscs);
//            }
//            pscs.add(thePsc.getID());
//            HashMap map = new HashMap();
//            Collection ids = new ArrayList(result.size());
//            for (Iterator it=result.iterator(); it.hasNext(); ) {
//                DeployedComponent dc=(DeployedComponent)it.next();
//                ids.add(dc.getID());
//            }
//            map.put(theProcess.getID(), ids);
//            deployedServices.put(thePsc.getID(), map);            
//
//            // notify listeners
//            fireConfigurationChange(
//                new ConfigurationChangeEvent(
//                    ConfigurationChangeEvent.NEW,
//                    thePsc,
//                    config,
//                    new Object[] {theProcess,
//                                  theHost,
//                                  config}));
//            if (result != null) {
//                Iterator itr = result.iterator();
//                while (itr.hasNext()) {
//                    DeployedComponent service = (DeployedComponent)itr.next();
//                    fireConfigurationChange(
//                        new ConfigurationChangeEvent(
//                            ConfigurationChangeEvent.NEW,
//                            service,
//                            config,
//                            new Object[] {thePsc, theProcess, theHost, config}));
//                }
//            }
//
//            // update cache
//           } catch (Exception theException) {
//            // rollback
//            if (editor != null) {
//                editor.getDestination().popActions();
//            }
//            throw new ExternalException(
//                formatErrorMsg("deployPsc", //$NON-NLS-1$
//                               "PSC=" + thePsc + //$NON-NLS-1$
//                                   ", process=" + theProcess + //$NON-NLS-1$
//                                   ", host=" + theHost + //$NON-NLS-1$
//                                   ", config=" + theConfigId, //$NON-NLS-1$
//                               theException),
//                theException);
//        }
//    }

    private void fireConfigurationChange(ConfigurationChangeEvent theEvent) {
        LogManager.logDetail(LogContexts.PSCDEPLOY, "ConfigurationChangeEvent=" + theEvent.paramString()); //$NON-NLS-1$
        for (int size=listeners.size(), i=0; i<size; i++) {
            ConfigurationChangeListener l = (ConfigurationChangeListener)listeners.get(i);
            l.configurationChanged(theEvent);
        }
    }

    private String formatErrorMsg(
        String theMethodName,
        Exception theException) {

        return formatErrorMsg(theMethodName, null, theException);
    }

    private String formatErrorMsg(
        String theMethodName,
        String theDetails,
        Exception theException) {

        return theException.getMessage() +
               " < ConfigurationManager." + theMethodName + //$NON-NLS-1$
               ((theDetails == null) ? "" : ":" + theDetails) + //$NON-NLS-1$ //$NON-NLS-2$
               " >"; //$NON-NLS-1$
    }

    public Configuration getConfig(ConfigurationID theId)  { 
        ConfigurationModelContainer model = getConfigModel(theId);    	
        return model.getConfiguration();
    }
    
    public ConfigurationModelContainer getConfigModel(ConfigurationID theId) {
        return  config; 
    }

    public Collection getConfigObjects(ConfigurationID theConfigId)
        throws ExternalException {

        try {
            
 //           return getConfigModel(theConfigId).getAllObjects();
             return getAPI().getConfigurationAndDependents(theConfigId);
        } catch (Exception theException) {
            throw new ExternalException(
                formatErrorMsg("getConfigObjects", //$NON-NLS-1$
                               "config=" + theConfigId, //$NON-NLS-1$
                               theException),
                theException);
        }
    }

//    public Collection getDeployedPscs(VMComponentDefn theProcess)
//        throws ExternalException {
//
//        ComponentDefnID procId = (ComponentDefnID)theProcess.getID();
//        Configuration config = getConfig(theProcess.getConfigurationID());
//
//        Collection result = (Collection)deployedPscs.get(procId);
//        if (result == null) {
//            // deployed pscs have not been requested   
// 
//            if (config == null) {
//                LogManager.logCritical(
//                    LogContexts.PSCDEPLOY,
//                    "ConfigurationManager.getDeployedPscs:" + //$NON-NLS-1$
//                        "Configuration for process " + theProcess + //$NON-NLS-1$
//                        " not found."); //$NON-NLS-1$
//            } else {
//    
////                    Collection deployedComps =
////                        config.getDeployedComponents(procId);
////                    if ((deployedComps != null) && (!deployedComps.isEmpty())) {
//                        try {
////                            Iterator itr = deployedComps.iterator();
////                            DeployedComponent dp = (DeployedComponent)itr.next();
//                            // per Scott, process will only have one
//                            // deployed component
//                            result = config.getPSCsForVM(theProcess);
//                            
//                            
//                            Collection ids = new ArrayList(result.size());
//                            for (Iterator it=result.iterator(); it.hasNext(); ) {
//                                ProductServiceConfig psc=(ProductServiceConfig)it.next();
//                                ids.add(psc.getID());
//                            }
//                            deployedPscs.put(theProcess.getID(), ids);
//                                                        
//                            // cache the pscs
//                            // fire the events
//                            if (result != null) {
//                                Host host = getHost(theProcess);
//                                Iterator pscItr = result.iterator();
//                                while (pscItr.hasNext()) {
//                                    ProductServiceConfig psc =
//                                        (ProductServiceConfig)pscItr.next();
//                                                                  
//                                        fireConfigurationChange(
//                                            new ConfigurationChangeEvent(
//                                                ConfigurationChangeEvent.NEW,
//                                                 psc,
//                                                 config,
//                                                 new Object[] {theProcess,
//                                                               host,
//                                                               config}));
//        
//                                        // cache the deployed services
//                                        getDeployedServices(psc, theProcess);
//
//                                }
//                               
//                            }
//                        } catch (Exception theException) {
//                            throw new ExternalException(
//                                formatErrorMsg("getDeployedPscs", //$NON-NLS-1$
//                                               "process=" + theProcess, //$NON-NLS-1$
//                                               theException),
//                                theException);
//                        }
//                //    }
//            }
//        } else {
//            Collection r = new ArrayList(result.size());
//            for (Iterator it=result.iterator(); it.hasNext();) {
//                ProductServiceConfigID pscID = (ProductServiceConfigID) it.next();
//                r.add( config.getPSC(pscID) );
//                
//            }
//            result = r;
//        }
// 
//        return result;
//    }

    public Collection getDeployedServices(
        VMComponentDefn theProcess)
        throws ExternalException {

         Configuration config = getConfig(theProcess.getConfigurationID());
        
//        HashMap map = (HashMap)deployedServices.get(pscId);
//        if (map == null) {
//            map = new HashMap();
//        }
        Collection ids = null;
        Collection result = null;
//        if ((result == null) && !map.containsKey(theProcess.getID())) {

            // get the process deployed component
            // will only have one
            
            // get the VM Deployed Components
//            Collection deployedComps =
//                config.getDeployedComponents((ComponentDefnID)theProcess.getID());
//            if ((deployedComps != null) && (!deployedComps.isEmpty())) {
                try {
//                    Iterator itr = deployedComps.iterator();
//                    DeployedComponent dp = (DeployedComponent)itr.next();
                    
                    // get the services that are deployed to the VM and PSC
                    Collection serviceComps = config.getDeployedServicesForVM(theProcess);
                    if (serviceComps != null) {
                        result = new ArrayList(serviceComps.size());
                        ids = new ArrayList(serviceComps.size());
                        Iterator servCompItr = serviceComps.iterator();
                        while (servCompItr.hasNext()) {
                            DeployedComponent service =
                                (DeployedComponent)servCompItr.next();
                            result.add(service);
                            ids.add(service.getID());
                                fireConfigurationChange(
                                    new ConfigurationChangeEvent(
                                        ConfigurationChangeEvent.NEW,
                                        service,
                                        config,
                                        new Object[] {theProcess,
                                                      getHost(theProcess),
                                                      config}));
                        }

                    }
                    
//                    if (map == null) {
//                        map = new HashMap();
//                    }
//                    map.put(theProcess.getID(), ids);
//                    deployedServices.put(pscId, map);                    
                  } catch (Exception theException) {
                    throw new ExternalException(
                        formatErrorMsg("getDeployedServices", //$NON-NLS-1$
                                       "Process=" + theProcess + //$NON-NLS-1$
                                            ", config=" + config, //$NON-NLS-1$
                                       theException),
                        theException);
                }
//            } else {
//               map.put(theProcess.getID(), null);
//               deployedServices.put(pscId, map);
//           }
//        } else {
//            Collection r = new ArrayList(result.size());
//            for (Iterator it=result.iterator(); it.hasNext();) {
//                DeployedComponentID dcID = (DeployedComponentID) it.next();
//                r.add( config.getDeployedComponent(dcID) );
//                
//            }
//            result = r;
//            
//        }
        return result;
    }

    public ConfigurationObjectEditor getEditor()
        throws ExternalException {

        try {
            if (configEditor == null) {
                configEditor = getAPI().createEditor();
            }
            // make sure all actions have been cleared
            // not sure this needs to be done
            configEditor.getDestination().clear();
            return configEditor;
        } catch (Exception theException) {
            throw new ExternalException(
                formatErrorMsg("getEditor", theException), //$NON-NLS-1$
                theException);
        }
    }
    
    public Collection getHosts(ConfigurationID configID) {
        ConfigurationModelContainer config = getConfigModel(configID);
        return config.getHosts();
        
    }
    
    public Host getHost(String hostFullName, ConfigurationID configID) {
        ConfigurationModelContainer config = getConfigModel(configID);
        return config.getHost(hostFullName);
    }

    public Host getHost(VMComponentDefn theProcess) {
        Configuration config = getConfig(theProcess.getConfigurationID());
        Host host = null;
//        Collection deployedComps =
//            config.getDeployedComponents((ComponentDefnID)theProcess.getID());
//        if ((deployedComps == null) || (deployedComps.isEmpty())) {
//            LogManager.logError(
//                LogContexts.PSCDEPLOY,
//                "No deployed component for VM " + theProcess.getName()); //$NON-NLS-1$
//            throw new IllegalStateException(
//                "No deployed component for VM " + theProcess.getName()); //$NON-NLS-1$
//        }
        // per Scott, 080601, only one object will be in collection
//        Iterator itr = deployedComps.iterator();
//        DeployedComponent depComp = (DeployedComponent)itr.next();
        HostID hostId = theProcess.getHostID();
            host = config.getHost(hostId.getFullName());
        if (host == null) {
            throw new IllegalStateException(
                "Host <" + hostId + "> not found containing process " + theProcess + " in configuration " + config.getFullName()); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
            }
        return host;
    }

    public Collection getHostProcesses(Host theHost,ConfigurationID theConfigId) throws ExternalException {

        try {
            Collection result = null; // VMComponentDefn
            Collection ids = null;
            Configuration config = getConfig(theConfigId);
            if (config == null) {
                LogManager.logCritical(LogContexts.PSCDEPLOY,"ConfigurationManager.getHostProcesses: Configuration " + config + " not found."); //$NON-NLS-1$ //$NON-NLS-2$
                result = Collections.EMPTY_LIST;
            } 
                
//
//                HashMap map = (HashMap)hostDeployments.get(theHost.getID());
//                if (map == null) {
//                    // host deployed processes has not be requested
//                    map = new HashMap();
//                    hostDeployments.put(theHost.getID(), map);
//                }
//                if (map.containsKey(theConfigId)) {
//                    // is cached, but collection can be null if
//                    // no deployed processes
//                    ids = (Collection)map.get(theConfigId);
//
//                    result = new ArrayList(ids.size());
//                    for (Iterator it=ids.iterator(); it.hasNext();) {
//                        VMComponentDefnID vmID = (VMComponentDefnID) it.next();
//                        result.add( config.getVMComponentDefn(vmID));               
//                    }
//                    
//                } else {
                    // not been cached                
                
                // see if it already cached
                    Collection hostProcesses = null;
                    hostProcesses = config.getVMsForHost(theHost.getName());
                    if (hostProcesses == null) {
                        // host has no deployed processes
                        // cache it
 //                       map.put(theConfigId, null);
                        result = Collections.EMPTY_LIST;
                    } else {
                        result = new ArrayList(hostProcesses.size());
                        ids = new ArrayList(hostProcesses.size());
                        Iterator itr = hostProcesses.iterator();
                        
                        if (itr.hasNext()) {
                            while (itr.hasNext()) {
//                                DeployedComponent deployedComp =
//                                    (DeployedComponent)itr.next();
//                                VMComponentDefnID processId =
//                                    deployedComp.getVMComponentDefnID();
                                VMComponentDefn process = (VMComponentDefn) itr.next();
//                              config.getComponentDefn(processId);
                                result.add(process);
                                ids.add(process.getID());
                                
                                fireConfigurationChange(new ConfigurationChangeEvent(ConfigurationChangeEvent.NEW,process,config,new Object[] {theHost,config}));
    
                                    // call this to cache the pscs
 //                                   getDeployedPscs(process);
                            } // end of while
 //                           map.put(theConfigId, ids);
                        } // end if itr of host processes
                    } // end of hostprocesses = null
 //               } // end of map not containing configid
 //           }  // end of where config = null
            return result;
        } catch (Exception theException) {
            throw new ExternalException(
                formatErrorMsg("getHostProcesses", //$NON-NLS-1$
                               "host=" + theHost + ", config=" + theConfigId, //$NON-NLS-1$ //$NON-NLS-2$
                               theException),
                theException);
        }
    }

 //   private ProductType getProduct(ProductTypeID theId) {
 ////   	return BasicProductType.PRODUCT_TYPE;
//        return this.getConfigModel(Configuration.NEXT_STARTUP_ID).getProductType(theId.getFullName());
//        return (ProductType)products.get(theId);
 //   }

//    public ProductType getProduct(ProductServiceConfig thePsc) {
//        ProductTypeID prodId = (ProductTypeID) thePsc.getComponentTypeID();
//        return getProduct(prodId);
//    }

//    public Map getAllProductPscs(ConfigurationID theConfigId)
//        throws ExternalException {
//        HashMap map = new HashMap();
//
//            Configuration config = getConfig(theConfigId);
//
//            ArrayList pscs = new ArrayList();
//            ProductType prod = BasicProductType.PRODUCT_TYPE;
//            Collection temp = getPscDefinitions(prod, config);
//            if (temp != null) {
//                pscs.addAll(temp);
//            }
//            map.put(prod, pscs);
//
//
//        return map;
//    }

//    public Collection getProducts() {
//    	Collection products = new ArrayList(1);
//    	products.add(BasicProductType.PRODUCT_TYPE);
//        return products;
//    }

    public ConfigurationPropertiedObjectEditor getPropertiedObjectEditor()
        throws ExternalException {

        try {
            // always get the editor that does not do auto commit
            // this editor only commits when api.executeXXX is done.
            // create a new editor each time so that each detail
            // panel will have their own POP editor to cache
           
            ConfigurationObjectEditor editor = getAPI().createEditor();
            ModificationActionQueue queue = editor.getDestination();
            ConfigurationPropertiedObjectEditor propEditor = new ConfigurationPropertiedObjectEditor(getConnection(), queue);
            return propEditor;
        } catch (Exception theException) {
            throw new ExternalException(
                formatErrorMsg("getPropertiedObjectEditor", theException), //$NON-NLS-1$
                theException);
        }
    }
    
    

    
    
    /**
     * Returns a PropertiedObject representation of a ComponentObject
     * 
     * @param componentObject
     *            ComponentObject for which PropertiedObject representation is sought
     * @returns PropertiedObject representation of parameter
     * @throws MetaMatrixRuntimeException
     *             if parameter can not be expressed as a PropertiedObject
     */
    public PropertiedObject getPropertiedObjectForComponentObject(ComponentObject componentObject) {
        
        if (componentObject == null) {
            Assertion.isNotNull(componentObject, AdminPlugin.Util.getString(AdminMessages.ADMIN_0018, "ComponentObject")); //$NON-NLS-1$
        }
        if (!(componentObject instanceof PropertiedObject)) {
            Assertion.assertTrue(componentObject instanceof PropertiedObject,
                                 AdminPlugin.Util.getString(AdminMessages.ADMIN_0019));
        }
        return (PropertiedObject)componentObject;
    }
    
    /**
     * Updates the resourceDescriptor with the new propertyValue and returns
     * the updated resourceDescriptor.
     * @param resourceDescriptor is the resource to be updated
     * @param propertyKey is the name of the property to change
     * @param propertyValue is the new value for the propertyKey
     * @throws ConfigurationException if the propertyKey does not exist as a property on the descriptor.
     * @throws AuthorizationException if caller is not authorized to perform this method.
     * @throws InvalidSessionException if the <code>callerSessionID</code> is not valid or is expired.
     * @throws MetaMatrixComponentException if an error occurred in communicating with a component.
     */
    public ResourceDescriptor updateResourcePropertyValue(ResourceDescriptor resourceDescriptor,
                                                          String propertyKey,
                                                          String propertyValue) throws ConfigurationException,
                                                                               InvalidSessionException,
                                                                               AuthorizationException,
                                                                               MetaMatrixComponentException {
        
        ConfigurationObjectEditor editor = new BasicConfigurationObjectEditor(false);
        ResourceDescriptor updatedRD = (ResourceDescriptor)editor.setProperty(resourceDescriptor, propertyKey, propertyValue);

        return updatedRD;
    }
    
    
//
//    private Collection getPscs(
//        ServiceComponentDefn theService,
//        Configuration theConfig) {
//
//          Iterator itr = serviceDefnMap.keySet().iterator();
//          ProductServiceConfigID pscId = null;
//        
//          Collection result = new ArrayList(20);
//          while (itr.hasNext()) {
//              Object key = itr.next();
//              Collection serviceIDs = (Collection)serviceDefnMap.get(key);
//              if (serviceIDs.contains(theService.getID())) {
//                  pscId = (ProductServiceConfigID)key;
//                  ProductServiceConfig psConfig = theConfig.getPSC(pscId);
//                  if(psConfig!=null) {
//                	  result.add(psConfig);
//                  }
//              }
//        
//          }
//          return result;
//        
//     }

//        public Collection getPscDefinitions(
//            ProductType theProduct,
//            Configuration theConfiguration)
//            throws ExternalException {
//
//            Collection result = null;
//            Collection ids = null;
//            ConfigurationID configId = (ConfigurationID)theConfiguration.getID();
//            ProductTypeID prodId = (ProductTypeID)theProduct.getID();
//            HashMap map = (HashMap)prodPscDefs.get(prodId);
//            if ((map == null) && !prodPscDefs.containsKey(prodId)) {
//                // product pscs has not be requested
//                map = new HashMap();
//                prodPscDefs.put(prodId, map);
//            }
//            if (map.containsKey(configId)) {
//                // is cached, but collection can be null if no psc defs
//                ids = (Collection)map.get(configId);
//                result = new ArrayList(ids.size());
//                for (Iterator it=ids.iterator(); it.hasNext();) {
//                    ProductServiceConfigID pscID = (ProductServiceConfigID) it.next();
//                    result.add( theConfiguration.getPSC(pscID) );
//                }
//                    
//                 
//                
//            } else {
//                // not been cached
//                Collection pscIds = theConfiguration.getComponentDefnIDs(prodId);
//                if (pscIds != null) {
//                    result = new ArrayList(pscIds.size());
//                    ids = new ArrayList(pscIds.size());
//                    Iterator pscIdItr = pscIds.iterator();
//                    while (pscIdItr.hasNext()) {
//                        ProductServiceConfigID pscId =
//                            (ProductServiceConfigID)pscIdItr.next();
//                        ProductServiceConfig psc =
//                            theConfiguration.getPSC(pscId);
//                            //getComponentDefn(pscId);
//                        result.add(psc);
//                        ids.add(pscId);
//
//                        fireConfigurationChange(
//                            new ConfigurationChangeEvent(
//                                ConfigurationChangeEvent.NEW,
//                                psc,
//                                theConfiguration,
//                                new Object[] {theProduct, theConfiguration}));
//                        //fireModelChangedEvent(MODEL_CHANGED);
//                        getServiceDefinitions(psc, theConfiguration);
//
//                    }
//                    map.put(configId, ids);
//                }
//
//            }
//            return result;
//        }
//        
   public Collection getServiceDefinitions(
        VMComponentDefn theVM,
        Configuration theConfiguration)
        throws ExternalException {


        Collection services = null;
        VMComponentDefnID vmId = (VMComponentDefnID)theVM.getID();
        
        Collection svcIDs = null;
        
 //       Collection svcdefns = theConfiguration.getDeployedServicesForVM(theVM);
        
        
//        if (serviceDefnMap.containsKey(theConfiguration.getID())) {
//            // is cached, but collection can be null if no psc defs
//            svcIDs = (Collection)serviceDefnMap.get(theConfiguration.getID());
//            services = new ArrayList(svcIDs.size());
//            for (Iterator it=svcIDs.iterator(); it.hasNext();) {
//                ServiceComponentDefnID id =
//                    (ServiceComponentDefnID)it.next();
//                ServiceComponentDefn service =
//                    (ServiceComponentDefn)theConfiguration.getComponentDefn(id);
//                
//                services.add( service );
//            }
//        } else {
        
//            svcIDs = (Collection)serviceDefs.get(pscId);
              
            // service defs have not been cached
        	Host host = theConfiguration.getHost(theVM.getHostID().getName());
            Collection svcdfns = theConfiguration.getDeployedServicesForVM(theVM);
            if (svcdfns != null) {

                services = new ArrayList(svcdfns.size());
                svcIDs = new ArrayList(svcdfns.size());
                Iterator servIdItr = svcdfns.iterator();
                while (servIdItr.hasNext()) {
                    
                    ServiceComponentDefn service =
                        (ServiceComponentDefn)servIdItr.next();
                    ServiceComponentDefnID id = (ServiceComponentDefnID) service.getID();
 
                    services.add(service);
                    svcIDs.add(id);
                    
                        fireConfigurationChange(
                            new ConfigurationChangeEvent(
                                ConfigurationChangeEvent.NEW,
                                service,
                                theConfiguration,
                                new Object[] {theVM,
                                              host,
                                              theConfiguration}));

                }

            }
            
//            serviceDefs.put(pscId, svcIDs);
 //           serviceDefnMap.put(pscId, svcIDs);
            
 //       } 
        
//        else {
//           
//            services = new ArrayList(svcIDs.size());
//            for (Iterator it=svcIDs.iterator(); it.hasNext();) {
//                ServiceComponentDefnID svcID = (ServiceComponentDefnID) it.next();
//                services.add(theConfiguration.getComponentDefn(svcID) );
//              
//            }
//           
//        }
            
        return services;
    }

    public Collection importObjects(String theFileName)
        throws ExternalException {

        try {
            FileInputStream xmlStream = new FileInputStream(theFileName);
                      
            XMLConfigurationImportExportUtility xmlUtil =
                new XMLConfigurationImportExportUtility();
            Collection configObjs =
                xmlUtil.importConfigurationObjects(xmlStream,
                		getAPI().createEditor(),
                                                  Configuration.NEXT_STARTUP);                            
            return configObjs;
        } catch (Exception theException) {
            throw new ExternalException(
                formatErrorMsg("importObjects", //$NON-NLS-1$
                               "file name=" + theFileName, //$NON-NLS-1$
                               theException),
                theException);
        }
    }

    private boolean isDeployed(
        Host theHost,
        ConfigurationID theConfigId) {

    	if (this.getHost(theHost.getFullName(), theConfigId) != null) {
    		return true;
    	}

        return false;
        
    }


    public boolean isRefreshNeeded() {
        return refreshNeeded;
    }

    private Object modify(
        ComponentObject theObject,
        Properties theProperties)
        throws Exception {

        ConfigurationObjectEditor editor = null;
        try {
            editor = getEditor();
            Object obj =
                editor.modifyProperties(
                    theObject, theProperties, ConfigurationObjectEditor.SET);
            getAPI().executeTransaction(editor.getDestination().popActions());
            return obj;
        } catch (Exception theException) {
            // rollback
            if (editor != null) {
                editor.getDestination().popActions();
            }
            throw theException;
        }
    }

    public Host modifyHost(
        Host theHost,
        Properties theProperties)
        throws ExternalException {

        try {
            Host host = (Host)modify(theHost, theProperties);
            // modify host in all configurations
            
                 if (isDeployed(theHost, config.getConfigurationID())) {
                    fireConfigurationChange(
                        new ConfigurationChangeEvent(
                            ConfigurationChangeEvent.MODIFIED,
                            host,
                            config.getConfiguration(),
                            new Object[] {config.getConfiguration()}));
                }


                return host;
        } catch (Exception theException) {
            throw new ExternalException(
                formatErrorMsg("modifyHost", "host=" + theHost, theException), //$NON-NLS-1$ //$NON-NLS-2$
                theException);
        }
    }

    public VMComponentDefn modifyProcess(
        VMComponentDefn theProcess,
        Properties theProperties)
        throws ExternalException {

        try {
            VMComponentDefn process =
                (VMComponentDefn)modify(theProcess, theProperties);

            // modify cache
            Configuration config = getConfig(theProcess.getConfigurationID());
            Host host = getHost(process);

            fireConfigurationChange(
                new ConfigurationChangeEvent(
                    ConfigurationChangeEvent.MODIFIED,
                    process,
                    config,
                    new Object[] {host, config}));
            return process;
        } catch (Exception theException) {
            throw new ExternalException(
                formatErrorMsg("modifyProcess", //$NON-NLS-1$
                               "process=" + theProcess, //$NON-NLS-1$
                               theException),
                theException);
        }
    }

    public void modifyPropertiedObject(ConfigurationPropertiedObjectEditor editor)
        throws ExternalException {

        try {
            ModificationActionQueue queue = editor.getQueue();
            getAPI().executeTransaction(queue.popActions());
        } catch (Exception theException) {
            throw new ExternalException(
                formatErrorMsg("modifyPropertiedObject", theException), //$NON-NLS-1$
                theException);
        }
    }
    
    
    

//    public ProductServiceConfig modifyPsc(
//        ProductServiceConfig thePscDef,
//        Properties theProperties)
//        throws ExternalException {
//
//        try {
//            ProductServiceConfig pscDef =
//                (ProductServiceConfig)modify(thePscDef, theProperties);
//            Configuration config = getConfig(pscDef.getConfigurationID());
//            ProductType product = BasicProductType.PRODUCT_TYPE;
//
//            // update local cache
//            fireConfigurationChange(
//                new ConfigurationChangeEvent(
//                    ConfigurationChangeEvent.MODIFIED,
//                    pscDef,
//                    config,
//                    new Object[] {product, config}));
//            return pscDef;
//        } catch (Exception theException) {
//            throw new ExternalException(
//                formatErrorMsg("modifyPsc", "PSC=" + thePscDef, theException), //$NON-NLS-1$ //$NON-NLS-2$
//                theException);
//        }
//    }

    public ServiceComponentDefn modifyService(
        ServiceComponentDefn theService,
        Properties theProperties)
        throws ExternalException {

        try {
            ServiceComponentDefn service =
                (ServiceComponentDefn)modify(theService, theProperties);
            Configuration config = getConfig(service.getConfigurationID());
            
             
                 
                fireConfigurationChange(
                    new ConfigurationChangeEvent(
                        ConfigurationChangeEvent.MODIFIED,
                        service,
                        config,
                        new Object[] {config}));

            return service;
        } catch (Exception theException) {
            throw new ExternalException(
                formatErrorMsg("modifyService", //$NON-NLS-1$
                               "service=" + theService, //$NON-NLS-1$
                               theException),
                theException);
        }
    }
    
    private void addConfig(ConfigurationModelContainer theConfig) {
        config = theConfig;
        fireConfigurationChange(new ConfigurationChangeEvent(ConfigurationChangeEvent.NEW,theConfig.getConfiguration(),theConfig.getConfiguration(), null));
    }
    
    public void refresh(){
        super.refresh();
        this.setRefreshNeeded();
        try {
            refreshImpl();
            refreshConfigs();
        } catch (ExternalException e) {
            LogManager.logCritical(LogContexts.CONFIG, "ConfigurationManager.refreshConfigs: Error refreshing configuration."); //$NON-NLS-1$
        }
    }    

    private void refreshConfigs() throws ExternalException {
        	
		fireConfigurationChange(new ConfigurationChangeEvent(ConfigurationChangeEvent.REFRESH_START,this));         	
 
        try {
            ConfigurationModelContainer nextStartUp = getAPI().getConfigurationModel(Configuration.NEXT_STARTUP);
            if (nextStartUp == null) {
                LogManager.logCritical(LogContexts.CONFIG,"ConfigurationManager.refreshConfigs:Next Startup Configuration is null."); //$NON-NLS-1$
            } else {
                addConfig(nextStartUp);
            }

			fireConfigurationChange(new ConfigurationChangeEvent(ConfigurationChangeEvent.REFRESH_END,this));   
            
            refreshNeeded = false;            
        } catch (Exception theException) {
            throw new ExternalException(formatErrorMsg("refreshConfigs", theException), theException); //$NON-NLS-1$
        }
    }
    
    private void refreshDeployedHosts() throws ExternalException {
		Iterator itr = config.getHosts().iterator();
		while (itr.hasNext()) {
			Host host = (Host) itr.next();
			getHostProcesses(host, config.getConfigurationID());
		}
	}

	private void refreshHosts()
	    throws ExternalException {
	
	    try {
	        Collection hostCollection = getConfigModel(Configuration.NEXT_STARTUP_ID).getHosts();
	        if ((hostCollection == null) || (hostCollection.isEmpty()))  {
	            LogManager.logCritical(LogContexts.CONFIG,"ConfigurationManager.refreshHosts: No hosts found or is null."); //$NON-NLS-1$
	        } else {
	            Iterator itr = hostCollection.iterator();
	            while (itr.hasNext()) {
	                Host host = (Host)itr.next();
	                LogManager.logDetail(LogContexts.CONFIG,"ConfigurationManager.refreshHosts: Adding Host:" + host); //$NON-NLS-1$
	                notifyHostChangeToConfigs(host, ConfigurationChangeEvent.NEW);
	            }
	        }
	    } catch (Exception theException) {
	        throw new ExternalException(
	            formatErrorMsg("refreshHosts", theException), //$NON-NLS-1$
	            theException);
	    }
	}    

    public void refreshImpl()
        throws ExternalException {

        fireConfigurationChange(new ConfigurationChangeEvent(ConfigurationChangeEvent.REFRESH_START,this));
        
        refreshConfigs();
        refreshHosts();
        refreshDeployedHosts();
        
        fireConfigurationChange(new ConfigurationChangeEvent(ConfigurationChangeEvent.REFRESH_END,this));
        refreshNeeded = false;
    }


    /**
     * Removes the given listener from those being notified.
     * @param theListener the listener being removed
     */
    public void removeConfigurationChangeListener(
        ConfigurationChangeListener theListener) {

        listeners.remove(theListener);
    }

    public void setEnabled(
        DeployedComponent thedeployed,
        boolean theEnableFlag,
        Configuration theConfig)
        throws ExternalException {

        ConfigurationObjectEditor editor = null;
        try {
            editor = getEditor();
           thedeployed = editor.setEnabled(thedeployed, theEnableFlag);
                
           getAPI().executeTransaction(editor.getDestination().popActions());
             
        } catch (Exception theException) {
            // rollback
            if (editor != null) {
                editor.getDestination().popActions();
            }
            throw new ExternalException(
                formatErrorMsg("setEnabled", //$NON-NLS-1$
                               "service=" + thedeployed.getName() + //$NON-NLS-1$
                                   ", enable=" + theEnableFlag + //$NON-NLS-1$
                                   ", config=" + theConfig, //$NON-NLS-1$
                               theException),
                theException);
        }
    }
    private ConfigurationAdminAPI getAPI() {
    	return ModelManager.getConfigurationAPI(getConnection());
    }
    /**
     * To indicate a refresh is needed.
     */
    public void setRefreshNeeded() {
        refreshNeeded = true;
    }
    
    
    /** 
     * @return Returns the listeners.
     * @since 4.2.1
     */
    public ArrayList getListeners() {
        return this.listeners;
    }
    
    
    /**
     * Check whether the encrypted properties for the specified ConnectorBindings can be decrypted.
     * If not, displays a warning dialog with a list of the failed bindings.  
     * @param bindings List<ConnectorBinding>
     * @throws ConfigurationException
     * @throws AuthorizationException
     * @throws InvalidSessionException
     * @throws MetaMatrixComponentException
	 * @return true if the bindings' properties can all be decrypted
     * @since 4.3
     */
    public boolean checkDecryptable(List bindings) throws ConfigurationException, AuthorizationException, InvalidSessionException, 
        MetaMatrixComponentException {
        
        List results = getAPI().checkPropertiesDecryptable(bindings);
        
        if (! results.contains(new Boolean(false))) {
            return true;
        }
        
        
        
        String header = "Warning: Unable to decrypt connector binding passwords"; //$NON-NLS-1$
        
        StringBuffer messageBuffer = new StringBuffer();
        messageBuffer.append("Warning: The following connector bindings were added, but the passwords could not be decrypted: \n"); //$NON-NLS-1$
        
        Iterator iter = bindings.iterator();
        Iterator iter2 = results.iterator(); while (iter.hasNext() && iter2.hasNext()) {
            ConnectorBinding binding = (ConnectorBinding) iter.next();
            boolean decryptable = ((Boolean) iter2.next()).booleanValue();
            
            if (! decryptable) {
                messageBuffer.append("  "); //$NON-NLS-1$
                messageBuffer.append(binding.getName());
                messageBuffer.append("\n"); //$NON-NLS-1$
            }
        }
        
        messageBuffer.append("\n\nSynchronization was not performed.");        //$NON-NLS-1$
        messageBuffer.append("\nThese bindings may have been exported from a system with a different keystore.");        //$NON-NLS-1$
        messageBuffer.append("\nYou must manually re-enter the passwords via the Properties tab, or convert the file with the 'convertpasswords' utility and re-import.");        //$NON-NLS-1$
        
                                                       
        StaticUtilities.displayModalDialogWithOK(header, messageBuffer.toString());
        return false;
    }
    
    
    
    // --------------------------------------------------------------
    // package-level utility methods for caching ComponentTypes,
    // these methods are used by ConfigurationPropertiedObjectEditor
    // --------------------------------------------------------------

    public Collection getAllCachedComponentTypeDefinitions(ComponentTypeID componentTypeID) throws ConfigurationException,
                                                                                    InvalidSessionException,
                                                                                    AuthorizationException,
                                                                                    MetaMatrixComponentException {
        Collection result = null;
        result = (Collection)typeIDtoAllTypeDefns.get(componentTypeID);
        if (result == null) {
            result = getAPI().getAllComponentTypeDefinitions(componentTypeID);
        }
        return result;
    }

    
    public ComponentTypeDefn getComponentTypeDefn(PropertyDefinition propertyDefinition,
                                                  ComponentObject componentObject) throws ConfigurationException,
                                                                                  InvalidSessionException,
                                                                                  AuthorizationException,
                                                                                  MetaMatrixComponentException {
        // TODO caching
        Collection typedefns = this.getAllCachedComponentTypeDefinitions(componentObject.getComponentTypeID());
        Iterator iter = typedefns.iterator();
        ComponentTypeDefn result = null;
        ComponentTypeDefn typeDefn = null;
        while (iter.hasNext()) {
            typeDefn = (ComponentTypeDefn)iter.next();
            if (typeDefn.getPropertyDefinition().getName().equals(propertyDefinition.getName())) {
                result = typeDefn;
                break;
            }
        }
        return result;
    }
    
}