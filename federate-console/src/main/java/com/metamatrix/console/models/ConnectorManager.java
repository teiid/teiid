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

package com.metamatrix.console.models;

import java.util.*;

import com.metamatrix.common.actions.ModificationActionQueue;
import com.metamatrix.common.config.api.*;
import com.metamatrix.common.object.*;
import com.metamatrix.console.connections.ConnectionInfo;
import com.metamatrix.console.ui.views.connector.ConnectorBasicInfo;
import com.metamatrix.console.ui.views.connector.ConnectorDetailInfo;
import com.metamatrix.console.ui.views.connectorbinding.*;
import com.metamatrix.console.util.ExceptionUtility;
import com.metamatrix.core.util.MetaMatrixProductVersion;
import com.metamatrix.platform.admin.api.ConfigurationAdminAPI;

public class ConnectorManager extends Manager 
		implements BindingDataInterface {
    
    private ArrayList arylConnectors            = null;
    private ArrayList arylConnectorBindings     = null;
    private HashMap hmConnectorNameMap          = null;
//    private HashMap hmConnectorBindingsUUIDMap  = null;
    private HashMap hmUUIDConnectorBindingsMap  = null;

    public ConnectorManager(ConnectionInfo connection) {
        super(connection);
    }

    public ConfigurationAdminAPI getConfigurationAdminAPI() {
        return ModelManager.getConfigurationAPI(getConnection());
    }

    public ConnectorBasicInfo[] getConnectorBasics(boolean forceRefresh) throws Exception {
        ConnectorBasicInfo[] aryConnectorBasicInfo = null;
        try {
            getConnectors(forceRefresh);
            aryConnectorBasicInfo
                = new ConnectorBasicInfo[arylConnectors.size()];
        } catch (Exception e) {
            // ok for testing, but it is not illegal to get no connectors...
            ExceptionUtility.showMessage("Failed trying to retrieve Connectors", e); //$NON-NLS-1$
            return null;
        }
        ComponentType ctypeTemp         = null;
        int iCounter                    = 0;
        Iterator it = arylConnectors.iterator();
        while (it.hasNext()) {
            ctypeTemp = (ComponentType)it.next();

            if (ctypeTemp.isDeployable()) {
                aryConnectorBasicInfo[iCounter]
                    = getConnectorBasicInfo(ctypeTemp);
                iCounter++;
            }
        }
        return aryConnectorBasicInfo;
    }

    public ConnectorDetailInfo getDetailForConnector(ComponentType ctConnector)
            throws Exception {
        String sName            = ctConnector.getName();
//        String sTransClass = ""; //$NON-NLS-1$
        String sConnClass = ""; //$NON-NLS-1$
        

        String sChangedBy   = ctConnector.getLastChangedBy();
        java.util.Date dChangedDate = ctConnector.getLastChangedDate();
        String sCreatedBy   = ctConnector.getCreatedBy();
        java.util.Date dCreatedDate = ctConnector.getCreatedDate();
        String desc = (ctConnector.getDescription()==null?"":ctConnector.getDescription());//$NON-NLS-1$


        ConnectorDetailInfo cdiInfo
            = new ConnectorDetailInfo(sName,
                                      desc,
                                       sConnClass,
                                       dCreatedDate,
                                       sCreatedBy,          // created by
                                       dChangedDate,
                                       sChangedBy);        // reg by

        return cdiInfo;
    }

    public void deleteConnector(String name) throws Exception {
        deleteConnector((ComponentType) hmConnectorNameMap.get(name));
    }

    public void deleteConnector(ComponentType ctConnector) throws Exception {
        // Remove from local caches
        String connectorName = ctConnector.getName();
        hmConnectorNameMap.remove(connectorName);
        arylConnectors.remove(ctConnector);

        // get an appropriate editor
        ConfigurationObjectEditor coeEditorForDelete
            = getConnectorEditor();

        coeEditorForDelete.delete(ctConnector);

        java.util.List lstActions = null;
        try {
            // get the actions from the Editor
            lstActions
                = coeEditorForDelete.getDestination().getActions();
        } catch (Exception e) {
            ExceptionUtility
                .showMessage("Failed to get actions from the editor", e); //$NON-NLS-1$
        }

        try {
            // use the config api to put them on the server
            getConfigurationAdminAPI().executeTransaction(lstActions);
        } catch (Exception e) {
            ExceptionUtility
                .showMessage("Failed trying to delete the connector", e); //$NON-NLS-1$
        }
    }

    private ConfigurationObjectEditor getConnectorEditor() throws Exception {
        ConfigurationObjectEditor newComponentEditor
            = getConfigurationAdminAPI().createEditor();

        return newComponentEditor;
    }

    public BindingBasics[] getBindingBasics() throws Exception {
        ArrayList /* ConnectorAndBinding */ arylConnBindings = getConnectorBindings();
        ArrayList arylBindingBasics     = new ArrayList();
        BindingBasics bbTemp    = null;
        Iterator itConnBind = arylConnBindings.iterator();

        while (itConnBind.hasNext()) {
            ConnectorAndBinding cAndB = (ConnectorAndBinding)itConnBind.next();
            ServiceComponentDefn scdefnConnBind = cAndB.getBinding();
            ComponentType connector = cAndB.getConnector();
            bbTemp = new BindingBasics(scdefnConnBind.toString(),
                                       connector.getName(), null);
            arylBindingBasics.add(bbTemp);
        }
        BindingBasics[] aryBindingBasics
                = new BindingBasics[arylBindingBasics.size()];
        arylBindingBasics.toArray(aryBindingBasics);

        return aryBindingBasics;
    }


    public void deleteBinding(ServiceComponentDefn scdConnectorBinding)
            throws Exception {
        // Issue: we have been casting bindings to ServiceComponentDefn,
        //  which is an interface; now that we need to delete one, there is
        //  no method that can delete this particular animal.  So can
        //  we safely cast it back to 'ComponentDefn', and delete it in
        //  that form?  Or is it somehow legal to use it directly?
        //  (It seems to compile...)
        //


        // get an appropriate editor
        ConfigurationObjectEditor coeEditorForDelete
            = getConnectorBindingEditor();

        coeEditorForDelete.delete(scdConnectorBinding, getNextStartupConfig(), true);

        java.util.List lstActions = null;
        try {
            // get the actions from the Editor
            lstActions
                = coeEditorForDelete.getDestination().getActions();
        } catch (Exception e) {
            ExceptionUtility
                .showMessage("Failed to get actions from the editor", e); //$NON-NLS-1$
        }

        try {
            // use the config api to put them on the server
            getConfigurationAdminAPI().executeTransaction(lstActions);
            ModelManager.getConfigurationManager(getConnection()).setRefreshNeeded();
        } catch (Exception e) {
            ExceptionUtility
                .showMessage("Failed trying to save the connector on the server", e); //$NON-NLS-1$
        }

    }
    
    
    public void deleteBindings(Collection bindings)
                throws Exception {
// Issue: we have been casting bindings to ServiceComponentDefn,
//  which is an interface; now that we need to delete one, there is
//  no method that can delete this particular animal.  So can
//  we safely cast it back to 'ComponentDefn', and delete it in
//  that form?  Or is it somehow legal to use it directly?
//  (It seems to compile...)
//


    // get an appropriate editor
    ConfigurationObjectEditor coeEditorForDelete
        = getConnectorBindingEditor();
    
    Configuration ns = getNextStartupConfig();
    
    for (Iterator it=bindings.iterator(); it.hasNext();) {
        ConnectorBinding cb = (ConnectorBinding) it.next();
        coeEditorForDelete.delete(cb, ns, true);
    }
    
    java.util.List lstActions = null;
    try {
        // get the actions from the Editor
        lstActions
            = coeEditorForDelete.getDestination().getActions();
    } catch (Exception e) {
        ExceptionUtility
            .showMessage("Failed to get actions from the editor", e); //$NON-NLS-1$
    }
    
    try {
        // use the config api to put them on the server
        getConfigurationAdminAPI().executeTransaction(lstActions);
        ModelManager.getConfigurationManager(getConnection()).setRefreshNeeded();
    } catch (Exception e) {
        ExceptionUtility
            .showMessage("Failed trying to save the connector on the server", e); //$NON-NLS-1$
    }

}
    

    /*
     * Utility methods
     */
    public ArrayList /* ComponentType */ getConnectors() {
        // no-arg version defaults to false
        return getConnectors(false);
    }

    public ArrayList /* ComponentType */ getConnectors(boolean bForceRefresh) {
        if (bForceRefresh) {
            arylConnectors = null;
            hmConnectorNameMap = null;
        }
        if (arylConnectors == null) {
            ConfigurationAdminAPI configAPI = getConfigurationAdminAPI();

            //.... initialize above variables

            //****************************************************************
            //STEP 1 - get all connectors
            //****************************************************************
            boolean includeDeprecated = false;
            Collection allTypes = null;
            try {
                allTypes = configAPI.getAllComponentTypes(includeDeprecated);
            } catch (Exception e) {
                ExceptionUtility.showMessage("Failed trying to retrieve All Types", e); //$NON-NLS-1$
            }

            arylConnectors = new ArrayList(allTypes.size());
            ArrayList sortConnectors = new ArrayList(allTypes.size());
            hmConnectorNameMap = new HashMap(allTypes.size());
            Iterator iter = allTypes.iterator();
            ComponentType aType = null;
            while (iter.hasNext()) {
                aType = (ComponentType)iter.next();
                if (aType.getComponentTypeCode() == ComponentType.CONNECTOR_COMPONENT_TYPE_CODE) {
                    sortConnectors.add(aType.getID().getName());
                    hmConnectorNameMap.put(aType.getName(), aType);
                }
            }
            //"connectors" now has all connectors in it - they are ComponentType objects
            
            Collections.sort(sortConnectors);
            for (Iterator it=sortConnectors.iterator(); it.hasNext();) {
                Object o = it.next();
                arylConnectors.add(hmConnectorNameMap.get(o));
            }
            
        }
        return arylConnectors;
    }

    public Configuration getNextStartupConfig() throws Exception {
        Configuration cfg =
                ModelManager.getConfigurationAPI(getConnection()).getNextStartupConfiguration();
        return cfg;
    }

    public ConnectorBasicInfo getConnectorBasicInfo(ComponentType ctypeConnector)
        throws Exception {
        Configuration nextStartupConfig = getNextStartupConfig();
        String sName        = ""; //$NON-NLS-1$
        String sDesc        = ""; //$NON-NLS-1$
        int iBindingsCount  = 0;

        sName = ctypeConnector.getName();
        
        // get the id from this connector
        ComponentTypeID ctypeConnectorID = 
        		(ComponentTypeID)ctypeConnector.getID();

        Collection connectorBindingIDs =
                nextStartupConfig.getComponentDefnIDs(ctypeConnectorID);


        iBindingsCount =  (connectorBindingIDs==null?0:connectorBindingIDs.size());

        ConnectorBasicInfo cbiInfo = new ConnectorBasicInfo(sName, 
        		iBindingsCount, sDesc);
        return cbiInfo;
    }

    public void reportConnectorDetails(ComponentType ctConnector) {
        // Add to local cache
        hmConnectorNameMap.put(ctConnector.getName(), ctConnector);
        arylConnectors.add(ctConnector);
       
    }

    public String getValueForDefn(ComponentType ctCompType, String sDefnID) {
        String sResultValue                     = ""; //$NON-NLS-1$
        ComponentTypeDefn connectorTypeDefn     = null;
        ComponentTypeDefn ctdCompTypeDefn       = null;
        Iterator connectorTypeDefns             = null;
        ComponentTypeID cTypeID = (ComponentTypeID)ctCompType.getID();

        try {
            connectorTypeDefns =
                    getConfigurationAdminAPI().getComponentTypeDefinitions(
                    cTypeID).iterator();
        } catch (Exception e) {
            ExceptionUtility
                .showMessage("Failed retrieving a value for a Defn: " + sDefnID, e); //$NON-NLS-1$
        }
        while (connectorTypeDefns.hasNext()) {
            connectorTypeDefn = (ComponentTypeDefn)connectorTypeDefns.next();
            if (connectorTypeDefn.getFullName().equals(sDefnID)) {
                ctdCompTypeDefn = connectorTypeDefn;
            }
        }
        if (!(ctdCompTypeDefn == null)) {
            PropertyDefinition propDef = null;
            Object defaultValue = null;
            try {
                propDef = ctdCompTypeDefn.getPropertyDefinition();
                defaultValue = propDef.getDefaultValue();
                sResultValue = defaultValue.toString();
            } catch (Exception e) {
            }
        }
        return sResultValue;
    }

    // utility methods for ConnectorBindings

//    public HashMap /* ConnBindingUUID:ServiceComponentDefn */
//        getConnectorBindingsUUIDMap() throws Exception {
//        // no-arg version defaults to false
//        return getConnectorBindingsUUIDMap(false);
//    }
//
//    public HashMap /* ConnBindingUUID:ServiceComponentDefn */
//            getConnectorBindingsUUIDMap(boolean bForceRefresh)
//            throws Exception {
//        ArrayList arylConnBinds     = null;
//        if (bForceRefresh)
//            hmConnectorBindingsUUIDMap = null;
//
//        if (hmConnectorBindingsUUIDMap == null)
//        {
//            hmConnectorBindingsUUIDMap = new HashMap();
//
//            // retrieve the available Connector Bindings
//            arylConnBinds /* ConnectorAndBinding */
//                = getConnectorBindings(bForceRefresh);
//
//            Iterator itConnBinds = arylConnBinds.iterator();
//
//            while (itConnBinds.hasNext())
//            {
//                ServiceComponentDefn scdTemp
//                    = ((ConnectorAndBinding)itConnBinds.next()).getBinding();
//
//                hmConnectorBindingsUUIDMap.put(scdTemp.getRoutingUUID(),
//                                                //scdTemp.getName());
//                                                scdTemp.toString());
//            }
//        }
//        return hmConnectorBindingsUUIDMap;
//    }

    public HashMap /*<String (routing UUID) to String (binding to-string)>*/
            getUUIDConnectorBindingsMap(boolean bForceRefresh)
            throws Exception {
        ArrayList arylConnBinds     = null;
        if (bForceRefresh) {
            hmUUIDConnectorBindingsMap = null;
        }
        if (hmUUIDConnectorBindingsMap == null) {
            hmUUIDConnectorBindingsMap = new HashMap();

            // retrieve the available Connector Bindings
            arylConnBinds /* ConnectorAndBinding */
                = getConnectorBindings();

            Iterator itConnBinds = arylConnBinds.iterator();

            while (itConnBinds.hasNext()) {
                ServiceComponentDefn scdTemp =
                        ((ConnectorAndBinding)itConnBinds.next()).getBinding();
                hmUUIDConnectorBindingsMap.put(scdTemp.getRoutingUUID(),
                        scdTemp.toString());
            }
        }
        return hmUUIDConnectorBindingsMap;
    }

	public ArrayList /* ConnectorAndBinding */ getConnectorBindings()
            throws Exception {
        // no-arg version defaults to true
        return getConnectorBindings(true);
    }
    
    public Collection /* ConnectorBinding */ getAllConnectorBindings() throws Exception {
        
        return getNextStartupConfig().getConnectorBindings();
     
    }

    /**
     *
     * @param bForceRefresh
     * @return
     * @throws Exception
     */
    public ArrayList /* ConnectorAndBinding */ getConnectorBindings(
            boolean bForceRefresh) throws Exception {

        if (bForceRefresh) {
            arylConnectorBindings = null;
        }
        if ( arylConnectorBindings == null) {
            arylConnectorBindings       = new ArrayList();
            Configuration cfgNextStartupCfg = getNextStartupConfig();
			ComponentType ctConnector               = null;
            
            Iterator itCbs = cfgNextStartupCfg.getConnectorBindings().iterator();
            while (itCbs.hasNext()) {
            	ConnectorBinding cb = (ConnectorBinding) itCbs.next();
                ctConnector = getConnectorForConnectorBinding(cb);
                arylConnectorBindings.add(new ConnectorAndBinding(ctConnector,
                        cb));
            }
		}
        return arylConnectorBindings;
    }

    public ConnectorBinding getConnectorBindingByName(String bindingName) 
            throws Exception {
        ConnectorBinding theBinding = null;
        Configuration nextStartupCfg = getNextStartupConfig();
        Iterator it = nextStartupCfg.getConnectorBindings().iterator();
        while (it.hasNext() && (theBinding == null)) {
            ConnectorBinding cb = (ConnectorBinding)it.next();
            if (cb.getName().equals(bindingName)) {
                theBinding = cb;
            }
        }
        return theBinding;
    }

    public ConnectorBinding getConnectorBindingByUUID(String uuid) throws Exception {
        ConnectorBinding theBinding = null;
        Configuration nextStartupCfg = getNextStartupConfig();
        theBinding = nextStartupCfg.getConnectorBindingByRoutingID(uuid);
        return theBinding;
    }
        
    /**
     * Lookup and return a connector for the given name or <code>null</code>
     * if no connector exists for <tag>name</tag>.
     * @param name The connector's name.
     * @return The connector for the given name.
     */
    public ComponentType lookupConnector(String name) {
        ComponentType aConnector = null;
        aConnector = (ComponentType) hmConnectorNameMap.get(name);
        if ( aConnector == null ) {
            // Try refreshing if not found in Map
            getConnectors(true);
            aConnector = (ComponentType) hmConnectorNameMap.get(name);
        }
        return aConnector;
    }

    /**
     *
     * @param sName
     * @return
     * @throws Exception
     */
    public boolean connectorBindingNameAlreadyExists(String sName)
            throws Exception {
        boolean bNameExists             = false;
        Collection colConnBinds = getConnectorBindings(true);

        ConnectorAndBinding scdCBTemp          = null;
        Iterator itConnBinds = colConnBinds.iterator();

        while (itConnBinds.hasNext()) {
            scdCBTemp = (ConnectorAndBinding)itConnBinds.next();
            String sThisCBName = scdCBTemp.getBinding().toString();
            if (sThisCBName.equals(sName)) {
                bNameExists = true;
                break;
            }
        }
        return bNameExists;
    }

    public boolean connectorTypeNameAlreadyExists(String sName) throws Exception {
        boolean bNameExists = false;
        Collection colConnectorTypes = getConnectors(true);

        Iterator itCompTypes = colConnectorTypes.iterator();
        ComponentType ctConnectorType = null;

        while (itCompTypes.hasNext()) {
            ctConnectorType = (ComponentType)itCompTypes.next();
            String sThisConnectoryTypeName = ctConnectorType.getName();

            if (sThisConnectoryTypeName.equals(sName)) {
                bNameExists = true;
                break;
            }
        }

        return bNameExists;
    }

    public ComponentType getConnectorForConnectorBinding(
            ServiceComponentDefn scdBinding) {
		ComponentType ctConnector = null;

        ComponentTypeID connectorID = scdBinding.getComponentTypeID();

        try {
            ctConnector = getConfigurationAdminAPI().getComponentType(
                    connectorID);
        } catch (Exception e) {
            ExceptionUtility.showMessage(
                    "Failed to get a Connector from a Connector ID", e); //$NON-NLS-1$
        }
        return ctConnector;
    }

	// =============================================
    //  Methods for creating ConnectorBindings
    //
    // =============================================
    public ConfigurationObjectEditor getConnectorBindingEditor()
        throws Exception {
        // get config admin api
        ConfigurationAdminAPI configAPI = getConfigurationAdminAPI();

        // get the connector binding editor
        ConfigurationObjectEditor connectorBindingEditor = null;

        // (Scott's note) It is very important to only use the below editor for just
        // this purpose, and to dispose of it afterward.  The reason
        // is that we are going to share it's internal ModificationActionQueue
        // with the PropertiedObjectEditor

        connectorBindingEditor = configAPI.createEditor();

        return connectorBindingEditor;
    }

    public ConnectorBinding getTentativeConnectorBinding(ComponentType ctConnector,
            ConfigurationObjectEditor coe, String sConnBindName)
            throws Exception {
		ConnectorBinding connectorBinding = coe.createConnectorComponent(Configuration.NEXT_STARTUP_ID,
                                                                         (ComponentTypeID)ctConnector.getID(),
                                                                         sConnBindName,
                                                                         null);
        Properties defaultProps = ModelManager.getConfigurationManager(getConnection()).
                    getConfigModel(Configuration.NEXT_STARTUP_ID).
                    getDefaultPropertyValues((ComponentTypeID)ctConnector.getID());
        connectorBinding = (ConnectorBinding) coe.modifyProperties(connectorBinding, defaultProps, ConfigurationObjectEditor.SET);

 		return connectorBinding;
    }
    
    public ConnectorBinding copyConnectorBinding(ConnectorBinding original, ConfigurationObjectEditor coe,
        String sConnBindName) throws Exception {
        
        ConnectorBinding connectorBinding = coe.createConnectorComponent(Configuration.NEXT_STARTUP_ID, original, sConnBindName, null);        
        return connectorBinding;
    }
    

    public PropertiedObject getPropertiedObject(
            ServiceComponentDefn scdConnectorBinding) throws Exception {
        ConfigurationManager configurationManager = ModelManager.getConfigurationManager(getConnection());
        PropertiedObject connectorBindingPO = 
            configurationManager.getPropertiedObjectForComponentObject(scdConnectorBinding);
        return connectorBindingPO;
    }

    public void saveConnectorBinding(ModificationActionQueue maqActionQForBinding)
        	throws Exception {
        ConfigurationAdminAPI configAPI = getConfigurationAdminAPI();
        configAPI.executeTransaction(maqActionQForBinding.popActions());
        ModelManager.getConfigurationManager(getConnection()).setRefreshNeeded();
        getConnectorBindings(true);
    }

    public PropertiedObjectEditor getPropertiedObjectEditor(
            ModificationActionQueue maqActionQForBinding ) throws Exception {
        // this takes the editor used to create the empty Connector Binding;
        //  we do this so that this editor will contain the action(s) used
        //  to create the beast, and the actions used to enter values into it.

        PropertiedObjectEditor propertiedObjectEditor
            = new ConfigurationPropertiedObjectEditor(getConnection(), maqActionQForBinding);
        return propertiedObjectEditor;
    }

    public PropertiedObjectEditor getPropertiedObjectEditor() throws Exception {
        // this takes the editor used to create the empty Connector Binding;
        //  we do this so that this editor will contain the action(s) used
        //  to create the beast, and the actions used to enter values into it.

        PropertiedObjectEditor propertiedObjectEditor
            = new ConfigurationPropertiedObjectEditor(getConnection());
        return propertiedObjectEditor;
    }

    public void setEnableForBindingInPSC(ServiceComponentDefn scdBinding,
    		ProductServiceConfig psc,
    		boolean bEnabled, ConfigurationObjectEditor coe)
            throws Exception {
        // Ask Scott what this boolean does, and what value we should use:
        boolean bDeleteDeployedComponents = true;

        coe.setEnabled(getNextStartupConfig(), scdBinding, psc, bEnabled, bDeleteDeployedComponents);
    }

    public void setEnableForBindingInPSCByConfig(
                                          Configuration cfg,
                                          ServiceComponentDefn scdBinding,
                                          ProductServiceConfig psc, 
                                          boolean bEnabled,
                                          ConfigurationObjectEditor coe)
            throws Exception {
        boolean bDeleteDeployedComponents = true;
        coe.setEnabled(cfg,
                        scdBinding,
                        psc, 
                        bEnabled, bDeleteDeployedComponents);
    }

	/**
	 * Method to commit actions to create a conector binding.  The 
	 * ConfigurationObjectEditor argument is expected to to have the connector,
	 * the connector binding name, and the properties.
	 * 
	 * @param binding  ServiceComponentDefn for the binding
	 * @param coe  editor expected to have the connector, the binding name, and the binding properties
	 * @param pscs  array of PSCs for which the binding is to be enabled
	 */
	public void createConnectorBinding(ServiceComponentDefn binding,
			ConfigurationObjectEditor coe, ProductServiceConfig[] pscs) 
			throws Exception {
		ServiceComponentDefnID bindingID = (ServiceComponentDefnID)binding.getID();
		Configuration nextStartupConfig = getNextStartupConfig();
		for (int i = 0; i < pscs.length; i++) {
			coe.addServiceComponentDefn(pscs[i], bindingID);
			coe.deployServiceDefn(nextStartupConfig, binding,
					(ProductServiceConfigID)pscs[i].getID());
                    
		}
		getConfigurationAdminAPI().executeTransaction(
				coe.getDestination().popActions());
        ModelManager.getConfigurationManager(getConnection()).setRefreshNeeded();
                
		getConnectorBindings(true);
        
        
	}
    
    
    /**
     * Method to commit actions to create a conector binding.  The 
     * ConfigurationObjectEditor argument is expected to to have the connector,
     * the connector binding name, and the properties.
     * 
     * @param bindings is a Collection of bindings to create for each PSC in the array.
     * @param coe  editor expected to have the connector, the binding name, and the binding properties
     * @param pscs  array of PSCs for which the binding is to be enabled
     */
    public void createConnectorBinding(Collection bindings,
            ConfigurationObjectEditor coe, ProductServiceConfig[] pscs) 
            throws Exception {
        Configuration nextStartupConfig = getNextStartupConfig();
        
        for (Iterator it=bindings.iterator(); it.hasNext();) {
            ConnectorBinding cb = (ConnectorBinding) it.next();
            ConnectorBindingID cbID = (ConnectorBindingID) cb.getID();
            
            for (int i = 0; i < pscs.length; i++) {
                coe.addServiceComponentDefn(pscs[i], cbID);
                coe.deployServiceDefn(nextStartupConfig, cb,
                        (ProductServiceConfigID)pscs[i].getID());
                        
            }
        }
        getConfigurationAdminAPI().executeTransaction(
                coe.getDestination().popActions());
        ModelManager.getConfigurationManager(getConnection()).setRefreshNeeded();
                
        getConnectorBindings(true);
        
        
    }    
    
    
    /**
     * Method to create a new connector based on another connector.  The 
     * ConfigurationObjectEditor argument is expected to to have the connector,
     * the connector binding name, and the properties.
     * 
     * @param binding  ServiceComponentDefn for the binding
     * @param newName is the new name to assign to the binding
     * 
     * @param coe  editor expected to have the connector, the binding name, and the binding properties
     */
    public ConnectorBinding createConnectorBinding(ConnectorBinding oldBinding,
                                                   String newName,
            ConfigurationObjectEditor coe) 
            throws Exception {
        
        //create a copy of the original connector binding
        ConnectorBinding newBinding = coe.createConnectorComponent(Configuration.NEXT_STARTUP_ID, oldBinding, newName, null);
        ConnectorBindingID newBindingID = (ConnectorBindingID) newBinding.getID();
        
        //deploy to the same PSCS as the original connector binding        
        ConnectorBindingID oldBindingID = (ConnectorBindingID) oldBinding.getID();
        Configuration nextStartupConfig = getNextStartupConfig();        
        Collection pscs = nextStartupConfig.getPSCsForServiceDefn(oldBindingID);        
        
        for (Iterator iter = pscs.iterator(); iter.hasNext(); ) {
            ProductServiceConfig psc = (ProductServiceConfig) iter.next();
            coe.addServiceComponentDefn(psc, newBindingID);
            coe.deployServiceDefn(nextStartupConfig, newBinding, (ProductServiceConfigID) psc.getID());
                    
        }
        
        //execute the transaction and return the results
        getConfigurationAdminAPI().executeTransaction(coe.getDestination().popActions());
        ModelManager.getConfigurationManager(getConnection()).setRefreshNeeded();
                
        getConnectorBindings(true);
        return getConnectorBindingByName(newName);
        
    }    
	
    public ServiceComponentDefn getBindingCopyFromPsc(ProductServiceConfig psc,
            ServiceComponentDefn scdBinding, ConnectionInfo connection) 
            throws Exception {
        ServiceComponentDefn scdBindingCopy = getBindingFromPSC(
                getNextStartupConfig(), scdBinding.getRoutingUUID(),  psc);
        return scdBindingCopy;
    }

    public ServiceComponentDefn getBindingCopyFromPscByConfig(
                                                Configuration cfg,
                                                ProductServiceConfig psc,
                                                ServiceComponentDefn scdBinding)
            throws Exception {
        ServiceComponentDefn scdBindingCopy = getBindingFromPSC(cfg,
                                 scdBinding.getRoutingUUID(),
                                 psc);
        return scdBindingCopy;
    }

	/**
     * @return Collection of all ProductServiceConfig objects for the
     * "Connectors" product type
     */
    public Collection getAllConnectorsPSCs() throws Exception {
    	return getAllConnectorsPSCsByConfig( getNextStartupConfig() );
	}

    /**
     * @return Collection of all ProductServiceConfig objects for the
     * "Connectors" product type
     */
    public Collection getAllConnectorsPSCsByConfig(Configuration cfg)
            throws Exception {
        ProductTypeID connectorProductTypeID =
                new ProductTypeID(MetaMatrixProductVersion.CONNECTOR_PRODUCT_TYPE_NAME);
        Iterator itAllConnectorPSCs =
                cfg.getComponentDefnIDs(connectorProductTypeID).iterator();
        ProductServiceConfigID pscID        = null;
        ProductServiceConfig psc            = null;
        Collection colResult                = new HashSet();

        while (itAllConnectorPSCs.hasNext()) {
            pscID = (ProductServiceConfigID)itAllConnectorPSCs.next();
            psc = (ProductServiceConfig)cfg.getComponentDefn(pscID);
            colResult.add(psc);
        }
        return colResult;
    }

    /**
     * @return the copy of the "global" binding that was placed in the indicated
     * PSC
     */
    private ServiceComponentDefn getBindingFromPSC(Configuration config,
            String routingUUID, ProductServiceConfig psc) {
        Iterator iter = psc.getServiceComponentDefnIDs().iterator();
        ServiceComponentDefnID bindingID = null;
        ServiceComponentDefn binding = null;
        while (iter.hasNext()) {
            bindingID = (ServiceComponentDefnID)iter.next();
            binding = (ServiceComponentDefn)config.getComponentDefn(bindingID);
            if (binding.getRoutingUUID().equals(routingUUID)) {
                return binding;
            }
        }
        //should never get here!!!
        return null;
    }
}//end ConnectorManager
