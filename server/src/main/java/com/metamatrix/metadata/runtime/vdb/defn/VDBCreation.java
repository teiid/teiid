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

package com.metamatrix.metadata.runtime.vdb.defn;



import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import org.teiid.metadata.RuntimeMetadataPlugin;

import com.metamatrix.api.exception.MetaMatrixException;
import com.metamatrix.common.actions.ObjectEditor;
import com.metamatrix.common.config.CurrentConfiguration;
import com.metamatrix.common.config.api.ComponentDefnID;
import com.metamatrix.common.config.api.ComponentType;
import com.metamatrix.common.config.api.ComponentTypeDefn;
import com.metamatrix.common.config.api.ComponentTypeID;
import com.metamatrix.common.config.api.ConfigurationModelContainer;
import com.metamatrix.common.config.api.ConfigurationObjectEditor;
import com.metamatrix.common.config.api.ConnectorBinding;
import com.metamatrix.common.config.api.ConnectorBindingID;
import com.metamatrix.common.config.api.ConnectorBindingType;
import com.metamatrix.common.config.api.DeployedComponent;
import com.metamatrix.common.config.api.ServiceComponentDefnID;
import com.metamatrix.common.config.api.VMComponentDefn;
import com.metamatrix.common.config.api.VMComponentDefnID;
import com.metamatrix.common.config.model.BasicConfigurationObjectEditor;
import com.metamatrix.common.log.LogManager;
import com.metamatrix.common.util.LogCommonConstants;
import com.metamatrix.common.vdb.api.ModelInfo;
import com.metamatrix.common.vdb.api.VDBArchive;
import com.metamatrix.common.vdb.api.VDBDefn;
import com.metamatrix.core.util.ArgCheck;
import com.metamatrix.metadata.runtime.RuntimeMetadataCatalog;
import com.metamatrix.metadata.runtime.api.RuntimeMetadataPropertyNames;
import com.metamatrix.metadata.runtime.api.VirtualDatabase;
import com.metamatrix.metadata.runtime.api.VirtualDatabaseID;
import com.metamatrix.platform.config.spi.xml.XMLConfigurationConnector;
import com.metamatrix.platform.config.spi.xml.XMLConfigurationMgr;


/**
* <p>The VDBCreation is used to install a VDB and its models
*     into the runtimemetadata and add Connector Types and Connector Bindings to
*     the configuration so that the VDB can be immediately available for access (user
*     needs to start the connector bindings on the console);

*     The process uses an xml file that defines the VDB, the models,
*     Connectors and Connector bindings.
*
* </p>
*/

public class VDBCreation  {

    private static final String UNDEFINED_PRINCIPAL = "VDBCreation_UndefinedPrincipal"; //$NON-NLS-1$

    
    private Properties runtimeProps;
    private boolean updateBindingProperties = false;
    private List vmsToDeployTo = null;    

    private String thePrincipal;


    public void setUpdateBindingProperties(boolean updateProperties) {
        updateBindingProperties = updateProperties;
    }
    
    public void setVMsToDeployBindings(List vms) {
        vmsToDeployTo = vms;

    }       

    public VirtualDatabase loadVDBDefn(VDBArchive vdbArchive, String principal) throws Exception {

        if (principal == null || principal.length() == 0) {
            this.thePrincipal = UNDEFINED_PRINCIPAL;
        } else {
            this.thePrincipal = principal;
        }

        ArgCheck.isNotNull(vdbArchive.getName(), "VDBDefn name must not be null"); //$NON-NLS-1$

        VDBDefn def = vdbArchive.getConfigurationDef();
        Map connectorBindings =  def.getConnectorBindings();
        Map connectorTypes = def.getConnectorTypes();
        
        Set addedTypes = new HashSet(connectorTypes.size());

        Map reMapBinding = new HashMap(connectorBindings.size());
        XMLConfigurationConnector writer = getWriter();
        BasicConfigurationObjectEditor editor = new BasicConfigurationObjectEditor(true);

        ConfigurationModelContainer cmc = CurrentConfiguration.getInstance().getConfigurationModel(); 
        
        for (Iterator it= connectorBindings.values().iterator(); it.hasNext(); ) {
            ConnectorBinding cb = (ConnectorBinding) it.next();

            // add the components to configuration
            ComponentType ct = (ComponentType) connectorTypes.get(cb.getComponentTypeID().getName());
             
            ConnectorBinding existingBinding = addConfigurationObjects(cmc, cb, ct,  writer, editor, addedTypes);
            // if the binding is returned, it indicates the binding already existed and
            // therefore will need to be remapped in its related model so that the 
            // model-to-binding mapping is correct
            if (existingBinding != null) {
                reMapBinding.put(cb.getRoutingUUID(), existingBinding);
            }

        }
        
        writer.executeActions(editor.getDestination().popActions());
        writer.commit();
        
        VirtualDatabase vdb = RuntimeMetadataCatalog.getInstance().createVirtualDatabase(vdbArchive, principal);
        VirtualDatabaseID vdbID = (VirtualDatabaseID)vdb.getID();
        // is this call necessary??
        RuntimeMetadataCatalog.getInstance().setVDBStatus(vdbID, vdbArchive.getStatus(), principal);
        return vdb;
    }

    protected void setVDBInfo(VDBDefn vdbDefn, String vdbName) {

        if (vdbName == null) {
            ArgCheck.isNotNull(vdbName, RuntimeMetadataPlugin.Util.getString("VDBCreation.Invalid_VDB_name"));//$NON-NLS-1$
        }

        String desc = vdbDefn.getDescription(); 
        String guid = vdbDefn.getUUID(); 
        
        //runtimeProps = new Properties();

        runtimeProps.setProperty(RuntimeMetadataPropertyNames.RT_USER_VDB_NAME, vdbName);
        runtimeProps.setProperty(RuntimeMetadataPropertyNames.RT_USER_VDB_GUID, guid + "_" + new Date()); //$NON-NLS-1$
        runtimeProps.setProperty(RuntimeMetadataPropertyNames.RT_USER_VDB_GUID, guid );
        runtimeProps.setProperty(RuntimeMetadataPropertyNames.RT_USER_VDB_PRINCIPAL_NAME, thePrincipal);
        runtimeProps.setProperty(RuntimeMetadataPropertyNames.RT_USER_VDB_DESCRIPTION, desc);


    }

    /**
     *  Return the existing binding that will be used to replace the imported binding and
     * the model-to-binding mapping will have its routinguuid updated
     * @param cmc
     * @param binding
     * @param type
     * @param createdPSC
     * @return
     * @throws Exception
     * @since 4.2
     */
    protected ConnectorBinding addConfigurationObjects(ConfigurationModelContainer cmc, ConnectorBinding binding, ComponentType type, XMLConfigurationConnector writer, BasicConfigurationObjectEditor editor, Set addedTypes)throws Exception {
        boolean addType = false;

    	ComponentType tExist = null;
        if (type != null ) {
        	if ( ! addedTypes.contains(type.getID())) {
        
        		tExist = cmc.getComponentType(type.getFullName());
        		if (tExist == null) {
        			addType=true;
        		}
            }
        } else {
        	// if here, the type wasnt in the .def file, therefore it must already exist
        	
            tExist = cmc.getComponentType(binding.getComponentTypeID().getFullName());
            if (tExist == null) {
                throw new MetaMatrixException(RuntimeMetadataPlugin.Util.getString("VDBCreation.No_type_passed_and_bindingtype_not_found", new Object[] { binding.getFullName(), binding.getComponentTypeID().getFullName()})); //$NON-NLS-1$
           	
            }
            type = tExist;
        }
        

        boolean bindingExist = false;                               

        // determine if the binding exist,
        // if it already exist, then dont add the new binding
        LogManager.logWarning(LogCommonConstants.CTX_CONFIG,RuntimeMetadataPlugin.Util.getString("VDBCreation.Does_connectorbinding_exist", binding.getFullName()));//$NON-NLS-1$

        ConnectorBinding bExist = cmc.getConfiguration().getConnectorBinding( (ComponentDefnID)binding.getID());
        if (bExist != null) {
            bindingExist = true;

            LogManager.logWarning(LogCommonConstants.CTX_CONFIG,RuntimeMetadataPlugin.Util.getString("VDBCreation.Connectorbinding_exist", binding.getFullName())); //$NON-NLS-1$
        } else {
        	LogManager.logWarning(LogCommonConstants.CTX_CONFIG,RuntimeMetadataPlugin.Util.getString("VDBCreation.Connectorbinding_will_be_added", binding.getFullName())); //$NON-NLS-1$

        }

		//***********
		//  ADD THE COMPONENT TYPE
		//***********
        
        if (addType) {
            editor.createComponentType(ComponentType.CONNECTOR_COMPONENT_TYPE_CODE, type.getFullName(),
                    type.getParentComponentTypeID(), type.getSuperComponentTypeID(), true, false);

            for (Iterator itD=type.getComponentTypeDefinitions().iterator(); itD.hasNext(); ) {
                ComponentTypeDefn ctd = (ComponentTypeDefn) itD.next();

                editor.createComponentTypeDefn(type, ctd.getPropertyDefinition(), false);

            }
            
            addedTypes.add(type.getID());
        } else if (updateBindingProperties) {
            // if the bindings are being updated, the type needs to be updated to ensure
            // the matching componenttypedefns exists.
            editor.updateComponentType(tExist, type);
        }

		//***********
		//  Binding Processing
		//***********       
        
        if (vmsToDeployTo != null && vmsToDeployTo.size() > 0) {
            // after the first time the binding is created in this loop
            // the next iterations the binding doesnt need to be 
            // created again, however the bindingExist variable
            // still needs to stay the same
            boolean beenCreated = false;
            for (Iterator vmit=vmsToDeployTo.iterator(); vmit.hasNext();) {
                String processName = (String) vmit.next();
                VMComponentDefn depVM = getDeployedVM(processName, cmc);
                // the VM has to be deployed (which is to have 1 or more pscs deployed to it) in order
                // for the VM to be started
                // *** Currently this process will not try to start a VM ***
                if (depVM == null) {
                	LogManager.logWarning(LogCommonConstants.CTX_CONFIG,RuntimeMetadataPlugin.Util.getString("VDBCreation.0", processName)); //$NON-NLS-1$
                     continue;
                }
                
                if (bindingExist) {
                    // check to see if the binding is already deployed to this VM
                    DeployedComponent depBinding = getDeployedBinding(binding.getName(), depVM, cmc);
                    // if already deployed then goto next vm
                    if (depBinding != null) {
                        continue;
                    }
                }
                LogManager.logWarning(LogCommonConstants.CTX_CONFIG,RuntimeMetadataPlugin.Util.getString("VDBCreation.5", new Object[] {binding.getName(), processName} )); //$NON-NLS-1$

                processDeployment(depVM, cmc, (beenCreated?beenCreated:bindingExist), binding, type, editor);

                beenCreated = true;
            }            
            
        } else {
            
            boolean bindingDeployed = false;
            if (bindingExist) {
                // check to see if the binding is already deployed 
                DeployedComponent depBinding = getDeployedBinding(binding.getName(), cmc);
                // if already deployed the binding properties are not being updated then do nothing
                if (depBinding != null) {
                    bindingDeployed = true;
                }
            }

            if (bindingDeployed) {
            } else {                
                VMComponentDefn depVM = getDeployedVM(cmc);
                   // the VM has to be deployed (which is to have 1 or more pscs deployed to it) in order
                   // for the VM to be started
                   // *** Currently this process will not try to start a VM ***
                if (depVM == null) {
                       throw new MetaMatrixException(RuntimeMetadataPlugin.Util.getString("VDBCreation.4")); //$NON-NLS-1$
                }
                processDeployment(depVM, cmc, bindingExist, binding, type, editor);
            }                        
        }
        
        if (updateBindingProperties && bindingExist) {
            // update the existing binding properties
        	LogManager.logWarning(LogCommonConstants.CTX_CONFIG,RuntimeMetadataPlugin.Util.getString("VDBCreation.7", bExist.getName() )); //$NON-NLS-1$
            Properties props = (Properties) bExist.getProperties().clone();
            props.putAll((Properties)binding.getProperties().clone());                           
            editor.modifyProperties(bExist, props, ConfigurationObjectEditor.SET);            
        } 
        return bExist;
    }
    
    private void processDeployment(VMComponentDefn depVM, ConfigurationModelContainer cmc, boolean bindingExist, ConnectorBinding binding, ComponentType type, ConfigurationObjectEditor editor ) throws Exception {
 
 
//    	
//                final String pscName = depVM.getName() + "PSC";  //$NON-NLS-1$
//                ProductServiceConfigID did = new ProductServiceConfigID(pscName);
//                if (cmc.getConfiguration().getPSC(did) !=null) {
//                    pscdep = cmc.getConfiguration().getPSC(did);
//                    isPSCDeployed = true;
//                } else if (createdPSC.containsKey(pscName)) {
//                    pscdep = (ProductServiceConfig) createdPSC.get(pscName);
//                    isPSCDeployed = true;
//                } else  {
//                	LogManager.logWarning(LogCommonConstants.CTX_CONFIG,RuntimeMetadataPlugin.Util.getString("VDBCreation.11", new Object[] {binding.getName(), depVM.getName()} )); //$NON-NLS-1$
//                    
//                    pscdep = editor.createProductServiceConfig(cmc.getConfiguration(), ConnectorBindingType.CONNECTOR_PROD_TYPEID, pscName);
//                    createdPSC.put(pscdep.getName(), pscdep);
//                }
//

 //       ProductServiceConfigID deployPSCID = (ProductServiceConfigID) pscdep.getID();
                
        if (!bindingExist) {
//            if (isPSCDeployed) {
////                System.out.println("Binding Doesnt exist, PSC is Deployed  PSC: " + pscdep.getID());
//
//                ConnectorBinding defn = editor.createConnectorComponent(cmc.getConfiguration(),
//                                 (ComponentTypeID) type.getID(), 
//                                 binding.getName(), 
//                                 (ProductServiceConfigID) pscdep.getID());
//                             
//                editor.setRoutingUUID(defn, binding.getRoutingUUID());
//                editor.modifyProperties(defn, binding.getProperties(), ObjectEditor.ADD);   
//
//                pscdep = editor.addServiceComponentDefn(pscdep,
//                                     (ServiceComponentDefnID) defn.getID());
//
//            } else {
             
                ConnectorBinding defn =  editor.createConnectorComponent(cmc.getConfigurationID(),
                                 (ComponentTypeID) type.getID(), 
                                 binding.getName(), 
                                 null);
            
                editor.setRoutingUUID(defn, binding.getRoutingUUID());
    
                editor.modifyProperties(defn, binding.getProperties(), ObjectEditor.ADD);   
  
            // add to the psc, but this does not deploy
//                pscdep = editor.addServiceComponentDefn(pscdep,
//                                     (ServiceComponentDefnID) defn.getID());

            // deploy all the services for this psc since it is not deployed

 //              Collection dcs = editor.deployProductServiceConfig(cmc.getConfiguration(), pscdep, depVM.getHostID(), (VMComponentDefnID) depVM.getID());
 //              System.out.println("Binding Doesnt exist, PSC is NOT Deployed - VM: " + depVM.getVMComponentDefnID());


//                if (dcs.isEmpty()) {
//                    String msg = RuntimeMetadataPlugin.Util.getString("VDBCreation.Error_deploying_binding", new Object[]{defn.getName(), pscdep.getName()});//$NON-NLS-1$
//                    throw new Exception(msg);
//                }
                
 //           }
 //           createdPSC.put(pscdep.getName(), pscdep);
 

 //           LogManager.logWarning(LogCommonConstants.CTX_CONFIG,RuntimeMetadataPlugin.Util.getString("VDBCreation.3", new Object[] {binding.getName(), depVM.getName(), pscdep.getName()}) ); //$NON-NLS-1$
            
        } 
        
//        else {
//            //if its gotten this far, then the binding is not deployed
//            // but check if the PSC is alredy deployed
//            if (isPSCDeployed) {
////                System.out.println("Binding Exist, PSC is Deployed PSC: " + pscdep.getID());
//                
//                pscdep = editor.addServiceComponentDefn(pscdep, (ConnectorBindingID) binding.getID());
//                editor.deployServiceDefn(cmc.getConfiguration(),  binding, (ProductServiceConfigID) pscdep.getID());
//            } else {   
////                System.out.println("Binding Exist, PSC is NOt Deployed - VM: " + depVM.getVMComponentDefnID());
//                pscdep = editor.addServiceComponentDefn(pscdep, (ConnectorBindingID) binding.getID());
//            // at this point, the binding already exist, but has not been deployed 
//                editor.deployProductServiceConfig(cmc.getConfiguration(), pscdep, depVM.getHostID(), (VMComponentDefnID) depVM.getID());
//            }
//            createdPSC.put(pscdep.getName(), pscdep);
//            LogManager.logWarning(LogCommonConstants.CTX_CONFIG,RuntimeMetadataPlugin.Util.getString("VDBCreation.2", new Object[] {binding.getName(), depVM.getName(), pscdep.getName()}) ); //$NON-NLS-1$
//
//        }
        
        
       	editor.deployServiceDefn(cmc.getConfiguration(), binding, (VMComponentDefnID) depVM.getID() );
     	LogManager.logWarning(LogCommonConstants.CTX_CONFIG,RuntimeMetadataPlugin.Util.getString("VDBCreation.11", new Object[] {binding.getName(), depVM.getName()} )); //$NON-NLS-1$

    }
    
    private VMComponentDefn getDeployedVM(String processName,ConfigurationModelContainer cmc) throws Exception {
        Collection vms = cmc.getConfiguration().getVMComponentDefns();
        for (Iterator it=vms.iterator(); it.hasNext();) {
            VMComponentDefn vm = (VMComponentDefn) it.next();
            if (vm.getName().equalsIgnoreCase(processName)) {
                return vm;
            }
        }
        
        return null;
    }
    
    // find the first deployed VM
    private VMComponentDefn getDeployedVM(ConfigurationModelContainer cmc) throws Exception {

        Collection vms = cmc.getConfiguration().getVMComponentDefns();
        if (vms == null || vms.size() == 0) {
            return null;            
        }
        for (Iterator it=vms.iterator(); it.hasNext();) {
            VMComponentDefn vm = (VMComponentDefn) it.next();
            Collection depsvcs = cmc.getConfiguration().getDeployedServicesForVM(vm);
            
//            Collection depvms = cmc.getConfiguration().getDeployedComponents((VMComponentDefnID)vm.getID());
            if (depsvcs != null && depsvcs.size() > 0) {
                return vm;
//                return (DeployedComponent) depvms.iterator().next();
            }
        }
       
        return null;
    }
    
    
    // this find the deployed BInding in the specified VM
    private DeployedComponent getDeployedBinding(String bindingName, VMComponentDefn depVM, ConfigurationModelContainer cmc) throws Exception {
        Collection deployedServices = cmc.getConfiguration().getDeployedServicesForVM(depVM);
//        getDeployedServicesForVM(depVM);
        for (Iterator it = deployedServices.iterator(); it.hasNext(); ) {
            DeployedComponent dc = (DeployedComponent) it.next();
            if (dc.isDeployedConnector()) {
                if (dc.getServiceComponentDefnID().getName().equalsIgnoreCase(bindingName)) {
                    return dc;
                }
             
            }
        }
        return null;        
    }
    
    // this finds the deployed BInding on any VM
    private DeployedComponent getDeployedBinding(String bindingName, ConfigurationModelContainer cmc) throws Exception {
        Collection deployedServices = cmc.getConfiguration().getDeployedComponents();
        for (Iterator it = deployedServices.iterator(); it.hasNext(); ) {
            DeployedComponent dc = (DeployedComponent) it.next();
            if (dc.isDeployedConnector()) {
                if (dc.getServiceComponentDefnID().getName().equalsIgnoreCase(bindingName)) {
                    return dc;
                }
             
            }
        }
        return null;        
    }    
   

    protected Short getVisibility(String visibility) {
        if (visibility == null || visibility.length() == 0) {
              return new Short(ModelInfo.PRIVATE);
        } else if (visibility.equalsIgnoreCase(ModelInfo.PUBLIC_VISIBILITY)) {
              return new Short(ModelInfo.PUBLIC);
        }  else {
              return new Short(ModelInfo.PRIVATE);
        }

    }

    private XMLConfigurationConnector getWriter() throws Exception {
        return XMLConfigurationMgr.getInstance().getTransaction(thePrincipal);
    }



}