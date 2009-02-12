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

package com.metamatrix.console.models;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Properties;

import com.metamatrix.common.actions.ModificationActionQueue;
import com.metamatrix.common.config.api.AuthenticationProvider;
import com.metamatrix.common.config.api.ComponentDefn;
import com.metamatrix.common.config.api.ComponentType;
import com.metamatrix.common.config.api.ComponentTypeID;
import com.metamatrix.common.config.api.Configuration;
import com.metamatrix.common.config.api.ConfigurationObjectEditor;
import com.metamatrix.common.config.api.ServiceComponentDefn;
import com.metamatrix.common.object.PropertiedObject;
import com.metamatrix.common.object.PropertiedObjectEditor;
import com.metamatrix.console.connections.ConnectionInfo;
import com.metamatrix.console.ui.views.authorization.ProvidersChangedEvent;
import com.metamatrix.console.ui.views.authorization.ProvidersChangedListener;
import com.metamatrix.console.util.ExceptionUtility;
import com.metamatrix.platform.admin.api.ConfigurationAdminAPI;

public class AuthenticationProviderManager extends Manager {
    
    private List allProviders            = null;
    private HashMap providerNameMap          = null;
    private final HashSet providersChangedListeners = new HashSet();

    public AuthenticationProviderManager(ConnectionInfo connection) {
        super(connection);
    }

    public ConfigurationAdminAPI getConfigurationAdminAPI() {
        return ModelManager.getConfigurationAPI(getConnection());
    }

    /**
     * Delete the provider from the startup config
     * @param provider the provider ComponentDefn
     */
    public void deleteProvider(ComponentDefn provider)
            throws Exception {
        // Issue: we have been casting bindings to ComponentDefn,
        //  which is an interface; now that we need to delete one, there is
        //  no method that can delete this particular animal.  So can
        //  we safely cast it back to 'ComponentDefn', and delete it in
        //  that form?  Or is it somehow legal to use it directly?
        //  (It seems to compile...)
        //


        // get an appropriate editor
        ConfigurationObjectEditor coeEditorForDelete = getAuthenticationProviderEditor();

        coeEditorForDelete.delete(provider, getNextStartupConfig());

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
                .showMessage("Failed trying to save the configuration on the server", e); //$NON-NLS-1$
        }

    }
    
    public void addProvidersChangeListener(final ProvidersChangedListener listener) {
        if(listener != null) {
            this.providersChangedListeners.add(listener);
        }
    }
    
    public void removeProvidersChangeListener(final ProvidersChangedListener listener) {
        if(listener != null) {
            this.providersChangedListeners.remove(listener);
        }
    }
    
    private void fireProvidersChangedEvent(final int type, final Object changedObject) {
        final ProvidersChangedEvent pce = new ProvidersChangedEvent(type, changedObject);
        final Iterator pcListeners = providersChangedListeners.iterator();
        while(pcListeners.hasNext() ) {
            ProvidersChangedListener next = (ProvidersChangedListener)pcListeners.next();
            next.providersChanged(pce);
        }
    }
    
    
    /**
     * Delete the collection of providers from the startup config
     * @param providers the list of provider ComponentDefns
     */
    public void deleteProviders(Collection providers)
                throws Exception {
		// Issue: we have been casting providers to ComponentDefn,
		//  which is an interface; now that we need to delete one, there is
		//  no method that can delete this particular animal.  So can
		//  we safely cast it back to 'ComponentDefn', and delete it in
		//  that form?  Or is it somehow legal to use it directly?
		//  (It seems to compile...)
		//
	
	
	    // get an appropriate editor
	    ConfigurationObjectEditor coeEditorForDelete = getAuthenticationProviderEditor();
	    
	    Configuration ns = getNextStartupConfig();
	    
	    for (Iterator it=providers.iterator(); it.hasNext();) {
	        AuthenticationProvider provider = (AuthenticationProvider) it.next();
	        coeEditorForDelete.delete(provider, ns, true);
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
	            .showMessage("Failed trying to save the configuration on the server", e); //$NON-NLS-1$
	    }
        
        //If we made it this far fire change events
        for (Iterator it=providers.iterator(); it.hasNext();) {
            AuthenticationProvider provider = (AuthenticationProvider) it.next();
            fireProvidersChangedEvent(ProvidersChangedEvent.DELETED, provider);
        }
        
	
	}
    

    /**
     * Get all AuthenticationProviders from the configuration
     * @return the collection of all providers
     */
    public Collection /* AuthenticationProvider */ getAllProviders() throws Exception {
        
        return getNextStartupConfig().getAuthenticationProviders();
     
    }
    
    /*
     * Utility methods
     */
    public List /* ComponentType */ getAllProviderTypes() {
        // no-arg version defaults to false
        return getAllProviderTypes(false);
    }

    /**
     * Get all provider types
     * @return the collection of provider types
     */
    public List /* ComponentType */ getAllProviderTypes(boolean bForceRefresh) {
        if (bForceRefresh) {
        	allProviders = null;
            providerNameMap = null;
        }
        if (allProviders == null) {
            ConfigurationAdminAPI configAPI = getConfigurationAdminAPI();

            //.... initialize above variables

            //****************************************************************
            //STEP 1 - get all Providers
            //****************************************************************
            boolean includeDeprecated = false;
            Collection allTypes = null;
            try {
                allTypes = configAPI.getAllComponentTypes(includeDeprecated);
            } catch (Exception e) {
                ExceptionUtility.showMessage("Failed trying to retrieve All Types", e); //$NON-NLS-1$
            }

            allProviders = new ArrayList(allTypes.size());
            ArrayList sortProviders = new ArrayList(allTypes.size());
            providerNameMap = new HashMap(allTypes.size());
            Iterator iter = allTypes.iterator();
            ComponentType aType = null;
            while (iter.hasNext()) {
                aType = (ComponentType)iter.next();
                if (aType.getComponentTypeCode() == ComponentType.AUTHPROVIDER_COMPONENT_TYPE_CODE) {
                	sortProviders.add(aType.getID().getName());
                    providerNameMap.put(aType.getName(), aType);
                }
            }
            
            // Current provider types are "Custom", "File", "LDAP".
            // Apply reverse ordering, to get them in the following order "LDAP", "File", "Custom" - most common to least common
            Collections.sort(sortProviders,Collections.reverseOrder());
            
            for (Iterator it=sortProviders.iterator(); it.hasNext();) {
                Object o = it.next();
                allProviders.add(providerNameMap.get(o));
            }
            
        }
        return allProviders;
    }

    public Configuration getNextStartupConfig() throws Exception {
        Configuration cfg =
                ModelManager.getConfigurationAPI(getConnection()).getNextStartupConfiguration();
        return cfg;
    }

    public boolean providerTypeNameAlreadyExists(String sName) throws Exception {
        boolean bNameExists = false;
//        Collection colProviderTypes = getAllProviders(true);
//
//        Iterator itProviderTypes = colProviderTypes.iterator();
//        ComponentType ctProviderType = null;
//
//        while (itProviderTypes.hasNext()) {
//        	ctProviderType = (ComponentType)itProviderTypes.next();
//            String sThisProviderTypeName = ctProviderType.getName();
//
//            if (sThisProviderTypeName.equals(sName)) {
//                bNameExists = true;
//                break;
//            }
//        }

        return bNameExists;
    }

    public ComponentType getAuthProvider(ServiceComponentDefn serviceDefn) {
		ComponentType ctAuthProvider = null;

        ComponentTypeID authProviderID = serviceDefn.getComponentTypeID();

        try {
        	ctAuthProvider = getConfigurationAdminAPI().getComponentType(
        			authProviderID);
        } catch (Exception e) {
            ExceptionUtility.showMessage(
                    "Failed to get Membership Domain Provider from ID", e); //$NON-NLS-1$
        }
        return ctAuthProvider;
    }

	// =============================================
    //  Methods for creating ConnectorBindings
    //
    // =============================================
    public ConfigurationObjectEditor getAuthenticationProviderEditor()
        throws Exception {
        // get config admin api
        ConfigurationAdminAPI configAPI = getConfigurationAdminAPI();

        // get the authentication provider editor
        ConfigurationObjectEditor authenticationProviderEditor = null;

        // (Scott's note) It is very important to only use the below editor for just
        // this purpose, and to dispose of it afterward.  The reason
        // is that we are going to share it's internal ModificationActionQueue
        // with the PropertiedObjectEditor

        authenticationProviderEditor = configAPI.createEditor();

        return authenticationProviderEditor;
    }

    public AuthenticationProvider getTentativeAuthenticationProvider(ComponentType ctProvider,
            ConfigurationObjectEditor coe, String sProviderName)
            throws Exception {
    	AuthenticationProvider provider = coe.createAuthenticationProviderComponent(Configuration.NEXT_STARTUP_ID,
                                                                       (ComponentTypeID)ctProvider.getID(),
                                                                       sProviderName);
        Properties defaultProps = ModelManager.getConfigurationManager(getConnection()).
                    getConfigModel(Configuration.NEXT_STARTUP_ID).
                    getDefaultPropertyValues((ComponentTypeID)ctProvider.getID());
        provider = (AuthenticationProvider) coe.modifyProperties(provider, defaultProps, ConfigurationObjectEditor.SET);

 		return provider;
    }
    
    public AuthenticationProvider copyAuthenticationProvider(AuthenticationProvider original, ConfigurationObjectEditor coe,
        String sProviderName) throws Exception {
        
    	AuthenticationProvider provider = coe.createAuthenticationProviderComponent(Configuration.NEXT_STARTUP_ID, original, sProviderName);        
        return provider;
    }
    

    public PropertiedObject getPropertiedObject(
            ComponentDefn cdAuthProvider) throws Exception {
        ConfigurationManager configurationManager = ModelManager.getConfigurationManager(getConnection());
        PropertiedObject connectorBindingPO = 
            configurationManager.getPropertiedObjectForComponentObject(cdAuthProvider);
        return connectorBindingPO;
    }

    public void saveProvider(ModificationActionQueue maqActionQForProvider)
        	throws Exception {
        ConfigurationAdminAPI configAPI = getConfigurationAdminAPI();
        configAPI.executeTransaction(maqActionQForProvider.popActions());
        ModelManager.getConfigurationManager(getConnection()).setRefreshNeeded();
//        getAllProviders(true);
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

	/**
	 * Method to commit actions to create a conector binding.  The 
	 * ConfigurationObjectEditor argument is expected to to have the connector,
	 * the connector binding name, and the properties.
	 * 
	 * @param binding  ServiceComponentDefn for the binding
	 * @param coe  editor expected to have the connector, the binding name, and the binding properties
	 * @param pscs  array of PSCs for which the binding is to be enabled
	 */
	public void createAuthenticationProvider(AuthenticationProvider provider,
			ConfigurationObjectEditor coe) 
			throws Exception {
		
		Configuration config = getNextStartupConfig();

		coe.addAuthenticationProvider(config,provider);
        		
		getConfigurationAdminAPI().executeTransaction(
				coe.getDestination().popActions());
        ModelManager.getConfigurationManager(getConnection()).setRefreshNeeded();
        
        //If we made it this far, fire events
        fireProvidersChangedEvent(ProvidersChangedEvent.NEW, provider);                
	}
        
}//end AuthenticationProviderManager
