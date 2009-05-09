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

package com.metamatrix.platform.config.service;

import java.io.InputStream;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import com.metamatrix.admin.api.exception.security.InvalidSessionException;
import com.metamatrix.api.exception.MetaMatrixComponentException;
import com.metamatrix.api.exception.security.AuthorizationException;
import com.metamatrix.common.actions.ActionDefinition;
import com.metamatrix.common.config.api.ComponentDefn;
import com.metamatrix.common.config.api.ComponentDefnID;
import com.metamatrix.common.config.api.ComponentObject;
import com.metamatrix.common.config.api.ComponentType;
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
import com.metamatrix.common.config.api.ServiceComponentDefn;
import com.metamatrix.common.config.api.ServiceComponentDefnID;
import com.metamatrix.common.config.api.VMComponentDefn;
import com.metamatrix.common.config.api.VMComponentDefnID;
import com.metamatrix.common.config.api.exceptions.ConfigurationException;
import com.metamatrix.common.config.api.exceptions.InvalidConfigurationException;
import com.metamatrix.common.config.model.BasicConfigurationObjectEditor;
import com.metamatrix.common.config.model.ComponentCryptoUtil;
import com.metamatrix.common.config.model.ConfigurationModelContainerImpl;
import com.metamatrix.common.config.xml.XMLConfigurationImportExportUtility;
import com.metamatrix.common.log.LogManager;
import com.metamatrix.common.util.LogCommonConstants;
import com.metamatrix.common.util.PropertiesUtils;
import com.metamatrix.platform.PlatformPlugin;
import com.metamatrix.platform.config.ConfigMessages;
import com.metamatrix.platform.config.ConfigPlugin;
import com.metamatrix.platform.config.api.service.ConfigurationServiceInterface;
import com.metamatrix.platform.config.spi.xml.XMLConfigurationConnector;
import com.metamatrix.platform.config.spi.xml.XMLConfigurationMgr;
import com.metamatrix.platform.service.ServicePlugin;
import com.metamatrix.platform.service.api.exception.ServiceException;
import com.metamatrix.platform.service.controller.AbstractService;
import com.metamatrix.platform.util.ErrorMessageKeys;
import com.metamatrix.platform.util.LogMessageKeys;


/**
*
*  Caching only Hosts and ComponentTypes - but these will be updated by a scheduled thread
*/

public class ConfigurationServiceImpl extends AbstractService implements ConfigurationServiceInterface {

/**
     * Flag denoting whether this service is closed and may not accept new work.
     */
//    private boolean serviceIsClosed = false;

    private static final String CONTEXT = LogCommonConstants.CTX_CONFIG;

    public ConfigurationServiceImpl() {
        super();
    }

   // -----------------------------------------------------------------------------------
    //                 S E R V I C E - R E L A T E D    M E T H O D S
    // -----------------------------------------------------------------------------------

    /**
     * Perform initialization and commence processing. This method is called only once.
     */
    protected void initService(Properties env) throws Exception {
        LogManager.logInfo(CONTEXT, ServicePlugin.Util.getString(LogMessageKeys.CONFIG_0002, new Object[] { getInstanceName()}));
    }

    /**
     * Close the service to new work if applicable. After this method is called
     * the service should no longer accept new work to perform but should continue
     * to process any outstanding work. This method is called by die().
     */
    protected void closeService() throws Exception {
        String instanceName = this.getInstanceName().toString();
        LogManager.logDetail(CONTEXT, instanceName + ": closing"); //$NON-NLS-1$
    }

    /**
     * Wait until the service has completed all outstanding work. This method
     * is called by die() just before calling dieNow().
     */
    protected void waitForServiceToClear() throws Exception {
    }

    /**
     * Terminate all processing and reclaim resources. This method is called by dieNow()
     * and is only called once.
     */
    protected void killService() {
//        this.sessionCount = 0;
    }


    public ConfigurationObjectEditor createEditor() throws ConfigurationException {
        return new BasicConfigurationObjectEditor(true);
    }

    /**
     * Returns the ID of the operational <code>Configuration</code>, which should reflect
     * the desired runtime state of the system.
     * @return ID of operational configuration
     * @throws ConfigurationException if an error occurred within or during communication with the Configuration Service.
     */
    public ConfigurationID getCurrentConfigurationID() throws ConfigurationException {
        return Configuration.NEXT_STARTUP_ID;
    }

    /**
     * Returns the ID of the next startup <code>Configuration</code>, which should reflect
     * the desired runtime state of the system.
     * @return ID of next startup configuration
     * @throws ConfigurationException if an error occurred within or during communication with the Configuration Service.
     */
    public ConfigurationID getNextStartupConfigurationID() throws ConfigurationException {
        return Configuration.NEXT_STARTUP_ID;
    }

    /**
     * Returns the operational <code>Configuration</code>, which should reflect
     * the desired runtime state of the system.
     * @return Configuration that is currently in use
     * @throws ConfigurationException if an error occurred within or during communication with the Configuration Service.
     */
    public Configuration getCurrentConfiguration() throws ConfigurationException {
        return getReadOnlyConfigurationModel().getConfiguration();
    }

    /**
     * Returns the next startup <code>Configuration</code>, the Configuration
     * that the system will next boot up with (once it is entirely shut down).
     * @return Configuration that the system will next start up with.
     * @throws ConfigurationException if an error occurred within or during communication with the Configuration Service.
     */
    public Configuration getNextStartupConfiguration() throws ConfigurationException{
        return getReadOnlyConfigurationModel().getConfiguration();
    }

    public ConfigurationModelContainer getConfigurationModel(String configName) throws ConfigurationException {
         return getReadOnlyConfigurationModel();
	}

    /**
     * <p>Returns a Collection containing the Configuration object for the specified
     * ConfigurationID id, and also any dependant objects needed to fully
     * define this configuration, such as Host objects, ComponentType
     * objects, and ComponentTypeDefn objects.</p>
     *
     * <p>A Configuration instance contains all of the
     * <code>ComponentDefn</code> objects that "belong" to just that
     * Configuration model: VM component definitions, service
     * component definitions, product service configurations, and
     * deployed components.  Objects such as Host objects,
     * ComponentType objects, ComponentTypeDefn, Resources, and
     * ConnectorBinding objects describe or support
     * ComponentDefns, but are not contained by a Configuration.  Therefore,
     * they are included in this Collection for convenience.</p>
     *
     * <p>The Collection will contain instances of
     * {@link com.metamatrix.common.namedobject.BaseObject}.
     * Specifically, this Map should contain the objects for:
     * one configuration object, one or more Host objects, one or more
     * ComponentType objects, and one or more ComponentTypeDefn objects.</p>
     *
     * <p>This method is intended to facilitate exporting a configuration
     * to XML.</p>
     *
     * <p>Here is what the Collection would contain at runtime:
     * <pre>
     * Configuration instance
     * Host instance1
     * Host instance2
     * ...
     * ConnectorBinding instance1
     * ConnectorBinding instance2
     * ...
     * SharedResource intance1
     * SharedResource instance
     * ...
     * ComponentType instance1
     * ComponentType instance2
     * ...
     * ComponentTypeDefn instance1
     * ComponentTypeDefn instance2
     * </pre></p>
     *
     * @param configID ID Of a Configuration
     * @return Collection of BaseObject instances
     * @throws ConfigurationException if an error occurred within or during communication with the Configuration Service.
     */
    public Collection getConfigurationAndDependents(ConfigurationID configID) throws ConfigurationException {
		return getReadOnlyConfigurationModel().getAllObjects();
    }

    public ComponentType getComponentType(ComponentTypeID id) throws ConfigurationException {
        if ( id == null) {
            throw new IllegalArgumentException(PlatformPlugin.Util.getString(ErrorMessageKeys.CONFIG_0045, "id")); //$NON-NLS-1$
        }

		ComponentType type = getReadOnlyConfigurationModel().getComponentType(id.getFullName());
		if (type != null) {
			LogManager.logDetail(CONTEXT, "Found component type " + id); //$NON-NLS-1$
		} else {
			LogManager.logDetail(CONTEXT, "No component type found for " + id); //$NON-NLS-1$
		}
		return type;
    }

    public Collection getAllComponentTypes(boolean includeDeprecated) throws ConfigurationException {
//		XMLConfigurationConnector transaction = getConnection(null);
		Map types = getReadOnlyConfigurationModel().getComponentTypes();
		Collection result = new LinkedList(types.values());

		if (result.size() > 0) {
			LogManager.logDetail(CONTEXT, "Found all component types"); //$NON-NLS-1$

		} else {
			throw new ConfigurationException(ErrorMessageKeys.CONFIG_0049,PlatformPlugin.Util.getString(ErrorMessageKeys.CONFIG_0049));
		}
		return result;

    }

    public Collection getComponentTypeDefinitions(ComponentTypeID componentTypeID) throws ConfigurationException {
        Collection result = getComponentTypeDefinitions(getReadOnlyConfigurationModel(), componentTypeID);

        if (result != null && result.size() > 0) {
            LogManager.logDetail(CONTEXT, new Object[] {"Found component type definitions for ", componentTypeID} ); //$NON-NLS-1$

        } else {
            LogManager.logTrace(CONTEXT, new Object[] {"Couldn't find component type definitions for ", componentTypeID} ); //$NON-NLS-1$
        }

        if (result == null) {
            result = new ArrayList(1);
        }
        return result;
    }

    public Collection getComponentTypeDefinitions(ConfigurationModelContainer config, ComponentTypeID componentTypeID) {
        ComponentType t = config.getComponentType(componentTypeID.getFullName());
        if (t!= null) {
            return t.getComponentTypeDefinitions();
        }
        return Collections.EMPTY_LIST;
    }    

    private Collection getDependentComponentTypeDefinitions(ComponentTypeID componentTypeID) throws ConfigurationException {
        Collection defns = getDependentComponentTypeDefinitions(getReadOnlyConfigurationModel(), componentTypeID);
        return defns;
    }

    private Collection getDependentComponentTypeDefinitions(ConfigurationModelContainer config, ComponentTypeID componentTypeID) throws ConfigurationException {

        Collection types = new ArrayList(config.getComponentTypes().values());

        Collection result= getSuperComponentTypeDefinitions(null, null, types, componentTypeID, config);

        if (result != null && result.size() > 0) {
            LogManager.logDetail(CONTEXT, new Object[] {"Found dependent component type definitions for ", componentTypeID} ); //$NON-NLS-1$
        } else {
            result = new ArrayList(1);
        }
        return result;
    }

    /**
     * This method calls itself recursively to return a Collection of all
     * ComponentTypeDefn objects defined for the super-type of the
     * componentTypeID parameter.  The equality of each PropertyDefn object contained
     * within each ComponentTypeDefn is the criteria to determine if a
     * defn exists in the sub-type already, or not.
     * @param defnMap Map of PropertyDefn object to the ComponentTypeDefn
     * object containing that PropertyDefn
     * @param defns return-by-reference Collection, built recursively
     * @param componentTypes Collection of all possible ComponentType
     * objects
     * @param componentTypeID the type for which super-type ComponentTypeDefn
     * objects are sought
     * @param transaction The transaction to operate within
     * @return Collection of all super-type ComponentTypeDefn objects (which
     * are <i>not</i> overridden by sup-types)
     */
    private Collection getSuperComponentTypeDefinitions(Map defnMap, Collection defns,
                                                Collection componentTypes,
                                                ComponentTypeID componentTypeID,
                                                ConfigurationModelContainer config) throws ConfigurationException {
        if (defnMap == null) {
            defnMap = new HashMap();
        }

        if (defns == null) {
            defns = new ArrayList();
        }
        ComponentType type = null;
        for (Iterator it = componentTypes.iterator(); it.hasNext(); ) {
            ComponentType ct = (ComponentType) it.next();
            if (componentTypeID.equals(ct.getID())) {
                type = ct;
            }
        }

        if (type == null) {
            throw new ConfigurationException(ErrorMessageKeys.CONFIG_0053, PlatformPlugin.Util.getString(ErrorMessageKeys.CONFIG_0053, componentTypeID));
        }

        if (type.getSuperComponentTypeID() == null) {
            return defns;
        }

        Collection superDefns = getComponentTypeDefinitions(config, type.getSuperComponentTypeID());
        ComponentTypeDefn sDefn;
        if (superDefns != null && superDefns.size() > 0) {
            Iterator it = superDefns.iterator();
            while (it.hasNext()) {
                sDefn = (ComponentTypeDefn) it.next();
                //this map has been changed to be keyed
                //on the PropertyDefn object of a ComponentTypeDefn,
                //instead of the i.d. of the ComponentTypeDefn
                if (!defnMap.containsKey(sDefn.getPropertyDefinition())) {
                    defnMap.put(sDefn.getPropertyDefinition(), sDefn);
                    defns.add(sDefn);
                }
            }
        }

        return getSuperComponentTypeDefinitions(defnMap, defns, componentTypes, type.getSuperComponentTypeID(), config);
    }

    public Collection getAllComponentTypeDefinitions(ComponentTypeID typeID)  throws ConfigurationException {
        Collection defns = getComponentTypeDefinitions(typeID);
        Collection inheritedDefns = getDependentComponentTypeDefinitions(typeID);

        //We want the final, returned Collection to NOT have any
        //duplicate objects in it.  The two Collections above may have
        //duplicates - one in inheritedDefns which is a name and a default
        //value for a super-type, and one in defns which is a name AND A
        //DIFFERENT DEFAULT VALUE, from the specified type, which overrides
        //the default value of the supertype.  We want to only keep the
        //BasicComponentTypeDefn corresponding to the sub-type name and default
        //value.
        //For example, type "JDBCConnector" has a ComponentType for the
        //property called "ServiceClassName" and a default value equal to
        //"com.metamatrix.connector.jdbc.JDBCConnectorTranslator".  The
        //super type "Connector" also defines a "ServiceClassName" defn,
        //but defines no default values.  Or worse yet, it might define
        //in invalid default value.  So we only want to keep the right
        //defn and value.

        Iterator inheritedIter =  inheritedDefns.iterator();
        Iterator localIter = defns.iterator();

        ComponentTypeDefn inheritedDefn = null;
        ComponentTypeDefn localDefn = null;

        while (localIter.hasNext()){
            localDefn = (ComponentTypeDefn)localIter.next();
            while (inheritedIter.hasNext()){
                inheritedDefn = (ComponentTypeDefn)inheritedIter.next();
                if (localDefn.getPropertyDefinition().equals(inheritedDefn.getPropertyDefinition())){
                    inheritedIter.remove();
                }
            }
            inheritedIter = inheritedDefns.iterator();
        }

        defns.addAll(inheritedDefns);


        return defns;
    }


    public Host getHost(HostID hostID) throws ConfigurationException {
        if ( hostID == null ) {
            throw new IllegalArgumentException(PlatformPlugin.Util.getString(ErrorMessageKeys.CONFIG_0045, "hostID")); //$NON-NLS-1$
        }

        Collection hosts = getHosts();
        for (Iterator it = hosts.iterator(); it.hasNext(); ) {
             Host h = (Host) it.next();
             if (h.getID().equals(hostID)) {
                return h;
             }
        }

        return null;
    }


    public Collection getHosts() throws ConfigurationException {
		Collection hosts = getReadOnlyConfigurationModel().getConfiguration().getHosts();
		if (hosts == null) {
			hosts = Collections.EMPTY_LIST;
		}
		return hosts;
    }


     /**
     * Returns a <code>ComponentDefn</code> for the specified <code>ComponentDefnID</code>.
     * </br>
     * @param configurationID is the configuration for which the component exist
     * @param componentDefnID is the component being requested
     * @return ComponentDefn
     * @throws ConfigurationException if an error occurred within or during communication with the Configuration Service.
     */
    public ComponentDefn getComponentDefn(ConfigurationID configurationID, ComponentDefnID componentDefnID) throws ConfigurationException{
    	if (componentDefnID == null) {
            throw new ConfigurationException(ConfigMessages.CONFIG_0045,ConfigPlugin.Util.getString(ConfigMessages.CONFIG_0045,"ComponentDefnID")); //$NON-NLS-1$
        }
        return getReadOnlyConfigurationModel().getConfiguration().getComponentDefn(componentDefnID);
    }

  /**
     * Returns a Collection of {@link com.metamatrix.common.config.api.ResourceDescriptor ResourceDescriptor}
     * for all internal resources defined to the system.  The internal resources are not managed with
     * the other configuration related information.  They are not dictated based on which configuration
     * they will operate (i.e., next startup or operational);
     * @throws AuthorizationException if caller is not authorized to perform this method.
     * @throws InvalidSessionException if the <code>callerSessionID</code> is not valid or is expired.
     * @throws MetaMatrixComponentException if an error occurred in communicating with a component.
     */
    public Collection getResources() throws ConfigurationException{
        return getReadOnlyConfigurationModel().getResources();
    }

    /**
     * Save the resource changes based on each {@link com.metamatrix.common.config.api.ResourceDescriptor ResourceDescriptor}
     * in the collection.
     * @param resourceDescriptors for the resources to be changed          *
     * @throws AuthorizationException if caller is not authorized to perform this method.
     * @throws InvalidSessionException if the <code>callerSessionID</code> is not valid or is expired.
     * @throws MetaMatrixComponentException if an error occurred in communicating with a component.
     */
    public void saveResources(Collection resourceDescriptors, String principalName)
    	throws ConfigurationException {
        
    	XMLConfigurationConnector transaction = this.getConnection(principalName);

        transaction.saveResources(resourceDescriptors, principalName);

        transaction.commit();                   // commit the transaction
    }

    // --------------------------------------------------------------
    //                A C T I O N     M E T H O D S
    // --------------------------------------------------------------

    /**
     * Execute as a single transaction the specified action, and optionally
     * return the set of objects or object IDs that were affected/modified by the action.
     * @param action the definition of the action to be performed on data within
     * the repository.
     * @param principalName of the person executing the transaction
     * @return the set of objects that were affected by this transaction.
     * @throws IllegalArgumentException if the action is null
     * or if the result specification is invalid
     * @throws ConfigurationException if an error occurred within or during
     * communication with the Configuration Service.
     */
    public Set executeTransaction(ActionDefinition action, String principalName) 
    	throws ConfigurationException{
        if ( action == null ) {
            throw new IllegalArgumentException(PlatformPlugin.Util.getString(ErrorMessageKeys.CONFIG_0045, "action")); //$NON-NLS-1$
        }
        List actions = new ArrayList(1);
        actions.add(action);
        return this.executeTransaction(actions, principalName);
    }

    /**
     * Execute as a single transaction the specified actions, and optionally
     * return the set of objects or object IDs that were affected/modified by the action.
     * @param actions the ordered list of actions that are to be performed on data within
     * the repository.
     * @param principalName of the person executing the transaction
     * @return the set of objects that were affected by this transaction.
     * @throws IllegalArgumentException if the action is null
     * or if the result specification is invalid
     * @throws ConfigurationException if an error occurred within or during
     * communication with the Configuration Service.
     */
     public Set executeTransaction(List actions, String principalName) throws ConfigurationException {
        if ( actions == null ) {
            throw new IllegalArgumentException(PlatformPlugin.Util.getString(ErrorMessageKeys.CONFIG_0045, "actions")); //$NON-NLS-1$
        }
        LogManager.logDetail(CONTEXT, new Object[]{"Executing transaction for user ", principalName, " with ",new Integer(actions.size())," action(s)" }); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
        Set result = new HashSet();
        if ( actions.isEmpty() ) {
            return result;
        }

        XMLConfigurationConnector transaction = this.getConnection(principalName);
        result = transaction.executeActions(actions);
        transaction.commit();                   // commit the transaction
        return result;
    }

    protected void addProperty(Properties source, String sourceName, Properties props, String propName) {
        String value = source.getProperty(sourceName);
        if (value != null) {
            props.setProperty(propName, value);
        }
    }



    // ----------------------------------------------------------------------------------------
    //                 I N T E R N A L     M E T H O D S
    // ----------------------------------------------------------------------------------------


    protected XMLConfigurationConnector getConnection(String principal) throws ConfigurationException {
    	return XMLConfigurationMgr.getInstance().getTransaction(principal==null?this.getInstanceName():principal);
    }
    
    private ConfigurationModelContainerImpl getReadOnlyConfigurationModel() throws ConfigurationException {
    	return XMLConfigurationMgr.getInstance().getConfigurationModel(Configuration.NEXT_STARTUP_ID);
    }    

    /**
     * @see com.metamatrix.platform.config.api.service.ConfigurationServiceInterface#addHost(java.lang.String,
     *      java.util.Properties)
     * @since 4.3
     */
    public Host addHost(String hostName, String principalName, Properties properties) throws ConfigurationException {
        com.metamatrix.common.config.api.Host host = null;

        ConfigurationObjectEditor editor = null;
        try {
            editor = createEditor();

            ConfigurationModelContainer config = this.getConfigurationModel(Configuration.NEXT_STARTUP);

            Properties defaultProps = config.getDefaultPropertyValues(Host.HOST_COMPONENT_TYPE_ID);
            Properties allProps = PropertiesUtils.clone(defaultProps, false);
            allProps.putAll(properties);

            
            host = editor.createHost(hostName);
            host = (com.metamatrix.common.config.api.Host)editor.modifyProperties(host, allProps, ConfigurationObjectEditor.SET);

            executeTransaction(editor.getDestination().popActions(), principalName); 

        } catch (Exception theException) {
            clearActions(editor);
            //final Object[] params = new Object[] {this.getClass().getName(), theException.getMessage()};
            final Object[] params = new Object[] {
                hostName
            };
            final String msg = PlatformPlugin.Util.getString("ConfigurationServiceImpl.Error_creating_New_Host", params); //$NON-NLS-1$

            throw new ConfigurationException(theException, msg); 
        }
        return host;
    }
    
    
    /**
     * @see com.metamatrix.platform.config.api.service.ConfigurationServiceInterface#addProcess(java.lang.String,
     *      java.lang.String, java.util.Properties)
     * @since 4.3
     */
    public VMComponentDefn addProcess(String processName,
                                      String hostName,
                                      String principalName,
                                      Properties properties) throws ConfigurationException {

        VMComponentDefn processDefn = null;

        ConfigurationObjectEditor editor = null;
        try {
            ConfigurationModelContainer config = this.getConfigurationModel(Configuration.NEXT_STARTUP);
            editor = createEditor();
            Host host = getHost(new HostID(hostName));
            if (host != null) {

            } else {
                final Object[] params = new Object[] {
                    hostName
                };
                final String msg = PlatformPlugin.Util.getString("ConfigurationServiceImpl.Host_not_found", params); //$NON-NLS-1$
                throw new ConfigurationException(msg);

            }

            // grab the default properties
            Properties defaultProps = config.getDefaultPropertyValues(VMComponentDefn.VM_COMPONENT_TYPE_ID);

            // create defn first
            processDefn = editor.createVMComponentDefn((ConfigurationID) config.getConfiguration().getID(),
                                                       (HostID)host.getID(),
                                                       VMComponentDefn.VM_COMPONENT_TYPE_ID,
                                                       processName);
            
            Properties allProps = PropertiesUtils.clone(defaultProps, false);
            allProps.putAll(properties);
            if (processDefn != null) {
                processDefn = (VMComponentDefn)editor.modifyProperties(processDefn, allProps, ConfigurationObjectEditor.SET);

                // create deployed component next
                executeTransaction(editor.getDestination().popActions(), principalName); 
            }

        } catch (Exception theException) {
            clearActions(editor);
            final Object[] params = new Object[] {
                processName, hostName
            };
            final String msg = PlatformPlugin.Util.getString("ConfigurationServiceImpl.Error_creating_Process", params); //$NON-NLS-1$
            throw new ConfigurationException(theException, msg);
        }
        return processDefn;
    }
    
    /**
     * Set System Property in Next Configuration
     * 
     * @param propertyName
     * @param propertyValue
     * @param principalName
     * @throws ConfigurationException
     * @throws InvalidSessionException
     * @throws AuthorizationException
     * @since 4.3
     */
    public void setSystemPropertyValue(String propertyName,
                                       String propertyValue,
                                       String principalName) throws ConfigurationException{
        ConfigurationObjectEditor editor = null;
        try {
            editor = createEditor();
            Configuration nextStartupConfig = getNextStartupConfiguration();
            editor.setProperty(nextStartupConfig, propertyName, propertyValue);
            executeTransaction(editor.getDestination().popActions(), principalName);

        } catch (Exception theException) {
            clearActions(editor);
            final Object[] params = new Object[] {
                propertyName, theException.getMessage()
            };
            final String msg = PlatformPlugin.Util.getString("ConfigurationServiceImpl.Error_setting_Property", params); //$NON-NLS-1$
            throw new ConfigurationException(theException, msg);
        }
    }
    
    
    /**
     * Set System Property in Next Configuration
     * 
     * @param properties
     * @param principalName
     * @throws ConfigurationException
     * @throws InvalidSessionException
     * @throws AuthorizationException
     * @since 4.3
     */
    public void updateSystemPropertyValues(Properties properties,
                                        String principalName) throws ConfigurationException {
        ConfigurationObjectEditor editor = null;
        try {
            editor = createEditor();
            Configuration nextStartupConfig = getNextStartupConfiguration();
            
            for (Iterator iter = properties.keySet().iterator(); iter.hasNext(); ) {
                String key = (String) iter.next();
                String value = properties.getProperty(key);
                editor.setProperty(nextStartupConfig, key, value);
            }
            
            executeTransaction(editor.getDestination().popActions(), principalName);

        } catch (Exception theException) {
            clearActions(editor);
            final Object[] params = new Object[] {
                theException.getMessage(), properties
            };
            final String msg = PlatformPlugin.Util.getString("ConfigurationServiceImpl.Error_updating_Properties", params); //$NON-NLS-1$
            throw new ConfigurationException(theException, msg);
        }
    }
    
    
    /** 
     * @see com.metamatrix.platform.config.api.service.ConfigurationServiceInterface#createConnectorBinding(java.lang.String, java.lang.String, java.lang.String, java.lang.String, java.util.Properties)
     * @since 4.3
     */
    public ConnectorBinding createConnectorBinding(String connectorBindingName,
                                                   String connectorType,
                                                   String vmName,
                                                   String principalName,
                                                   Properties properties) throws ConfigurationException {

        ConnectorBinding binding = null;
        ConfigurationObjectEditor editor = null;
        Configuration config = null;

        try {
            ComponentType ctConnector = getComponentType(connectorType, false);
            if (ctConnector == null) {
                final Object[] params = new Object[] {
                    connectorType
                };
                final String msg = PlatformPlugin.Util.getString("ConfigurationServiceImpl.Connector_Type_not_found", params); //$NON-NLS-1$

                throw new MetaMatrixComponentException(msg);
            }
            editor = createEditor();
            config = getNextStartupConfiguration();

            binding = createConnectorBinding(ctConnector, editor, connectorBindingName);
            binding = (ConnectorBinding)editor.modifyProperties(binding, properties, ConfigurationObjectEditor.SET);

 
            if (vmName != null) {
            	// for now, deploy the binding to all vms
 //           	if (vmName.equalsIgnoreCase("all")) { //$NON-NLS-1$
            		Collection<VMComponentDefn> vms = config.getVMComponentDefns();
            		for (Iterator<VMComponentDefn> it=vms.iterator(); it.hasNext();) {
            			 VMComponentDefn vm = it.next();
            			 
            			 DeployedComponent dc = editor.deployServiceDefn(config, binding,  (VMComponentDefnID)vm.getID());
            		}
            		
//            	} else {
//            		// TODO:  the method from serveradminapi passes in "ALL" and its not currently
//            		// called from anywhere else
//            	}
//            	
            }
            executeTransaction(editor.getDestination().popActions(), principalName);

        } catch (Exception theException) {
            clearActions(editor);
            final Object[] params = new Object[] {
                this.getClass().getName(), theException.getMessage()
            };

            throw new ConfigurationException(theException,PlatformPlugin.Util.getString("ConfigurationServiceImpl.Error_creating_Connector_Binding", params)); //$NON-NLS-1$

        }
        return binding;
    }

    private ComponentType getComponentType(String connectorName, boolean deprecated) throws ConfigurationException {
        Collection arylConnectors = getAllComponentTypes(deprecated);
        Iterator itConnectors = arylConnectors.iterator();
        while (itConnectors.hasNext()) {
            ComponentType ctConnector = (ComponentType)itConnectors.next();
            if (ctConnector.getName().equals(connectorName)) {
                return ctConnector;
            }
        }
        return null;
    }

    private ConnectorBinding createConnectorBinding(ComponentType ctConnector, ConfigurationObjectEditor coe, String sConnBindName)  {
        ConnectorBinding connectorBinding = coe.createConnectorComponent(Configuration.NEXT_STARTUP_ID, (ComponentTypeID)ctConnector.getID(),sConnBindName,null);
        return connectorBinding;
    }
    
    
    public Object modify(ComponentObject theObject,Properties theProperties, String principalName) throws ConfigurationException{
        ConfigurationObjectEditor editor = null;
        try {
            editor = createEditor();
            Object obj = editor.modifyProperties(theObject, theProperties, ConfigurationObjectEditor.SET);
            executeTransaction(editor.getDestination().popActions(), principalName);
            return obj;
        } catch (ConfigurationException e) {
            clearActions(editor);
            throw e;
        }
    }
    
    /**
     * the name is nullable, if not specified, then the name in the inputStream (connector type file) will be used
     */
    public ComponentType importConnectorType(InputStream inputStream, String name, String principalName) throws ConfigurationException {
        ComponentType newType = null;
        ConfigurationObjectEditor editor = createEditor();

        try {
            XMLConfigurationImportExportUtility util = new XMLConfigurationImportExportUtility();
            newType = util.importComponentType(inputStream, editor, name);

            executeTransaction(editor.getDestination().popActions(), principalName);

        } catch (Exception theException) {
            clearActions(editor);
            final String msg = PlatformPlugin.Util.getString("ConfigurationServiceImpl.Error_importing_connector_type", new Object[] {name, theException.getMessage()}); //$NON-NLS-1$
            throw new ConfigurationException(theException, msg); 

        }
        return newType;

    }
    
    

    /** 
     * @see com.metamatrix.platform.config.api.service.ConfigurationServiceInterface#importConnectorBinding(java.io.InputStream, java.lang.String, java.lang.String, java.lang.String)
     * @since 4.3
     */
    public ConnectorBinding importConnectorBinding(InputStream inputStream,
                                                   String name,
                                                   String vmName,
                                                   String principalName) throws ConfigurationException{
        ConnectorBinding newBinding = null;
        ConfigurationObjectEditor editor = createEditor();

        try {
            XMLConfigurationImportExportUtility util = new XMLConfigurationImportExportUtility();
            newBinding = util.importConnectorBinding(inputStream, editor, name);

            
            //deploy to the specified PSC
            Configuration config = getNextStartupConfiguration();
            if (vmName != null && !vmName.equals("")) { //$NON-NLS-1$
            	Collection<VMComponentDefn> vms = null;
            	if (vmName.equalsIgnoreCase("all")) {
            		vms = this.getCurrentConfiguration().getVMComponentDefns();
            		
            	} else {
            		
            	}
            	
            	if (vms != null) {
            		ServiceComponentDefnID bindingID = (ServiceComponentDefnID) newBinding.getID();
            		for (Iterator<VMComponentDefn> it=vms.iterator(); it.hasNext();) {
            			VMComponentDefn vm = it.next();
            			 editor.deployServiceDefn(config, newBinding, (VMComponentDefnID) vm.getID() );
            		}
            	}
            }            
            
            executeTransaction(editor.getDestination().popActions(), principalName);

        } catch (Exception theException) {
            clearActions(editor);
            final Object[] params = new Object[] {
                name, theException.getMessage()
            };
            final String msg = PlatformPlugin.Util.getString("ConfigurationServiceImpl.Error_importing_connector_binding", params); //$NON-NLS-1$
            throw new ConfigurationException(theException, msg); 

        }
        return newBinding;
    }

    /** 
     * @see com.metamatrix.platform.config.api.service.ConfigurationServiceInterface#delete(com.metamatrix.common.config.api.ComponentObject, boolean)
     * @since 4.3
     */
    public void delete(ComponentObject theObject, boolean theDeleteDependenciesFlag, String principalName)
    	throws ConfigurationException {
    	
        ConfigurationObjectEditor editor = null;

        try {
            editor = createEditor();
            Configuration config = getNextStartupConfiguration();

            editor.delete(theObject, config, theDeleteDependenciesFlag);
            // the editor won't have any actions if the ComponentObject
            // being deleted does not have any DeployedComponents under it,
            // a NPE is thrown if executeTransaction() is called
            if (editor.getDestination().getActionCount() != 0) {
                executeTransaction(editor.getDestination().popActions(),principalName);
            }
        } catch (ConfigurationException e) {
            clearActions(editor);
            throw e;
        } catch (ServiceException e) {
        	clearActions(editor);
            throw e;
        } 
    }

	private void clearActions(ConfigurationObjectEditor editor) {
		if (editor != null) {
		    editor.getDestination().popActions();
		}
	}

    /** 
     * @see com.metamatrix.platform.config.api.service.ConfigurationServiceInterface#delete(com.metamatrix.common.config.api.ComponentType, java.lang.String)
     * @since 4.3
     */
    public void delete(ComponentType componentType, String principalName) 
    	throws ConfigurationException {
        
        ConfigurationObjectEditor editor = null;
		try {
			editor = createEditor();
			editor.delete(componentType);
			executeTransaction(editor.getDestination().popActions(),principalName);
		} catch (ConfigurationException e) {
			clearActions(editor);
			throw e;
		}
    }
    
    /**
     * Deploys the ServiceComponentDefns 
     * contained by the Configuration, onto the specified Host and VM.
     * 
     * @param theHost host on which the services will be deployed
     * @param theProcess VM on which the services will be deployed
     * @param serviceName Name of the ServiceComponentDefn
     * @param principalName User Name deploying the Services
     * 
     * @return DeployedComponent of the ServiceComponentDefns that was deployed
     * 
     * @throws ConfigurationException
     * @since 6.1
     */
    
    public DeployedComponent deployService(VMComponentDefnID theProcessID, String serviceName, String principalName) throws ConfigurationException {

        DeployedComponent deployComponent = null;

        ConfigurationObjectEditor editor = null;
        try {
            editor = createEditor();
            Configuration config = getNextStartupConfiguration();
            ServiceComponentDefn scd = config.getServiceComponentDefn(serviceName);
 
            
            deployComponent = editor.deployServiceDefn(config, scd,  theProcessID);

            
            executeTransaction(editor.getDestination().popActions(), principalName);
        } catch (ConfigurationException theException) {
            clearActions(editor);
            throw theException;
        }
        return deployComponent;
    }
    

    /**
     * Check whether the encrypted properties for the specified ComponentDefns can be decrypted.
     * @param defns List<ComponentDefn>
     * @return List<Boolean> in the same order as the paramater <code>defns</code>.
     * For each, true if the properties could be decrypted for that defn.
     * @throws ConfigurationException
     * @since 4.3
     */
    public List checkPropertiesDecryptable(List defns) throws ConfigurationException{
        
        List results = new ArrayList(defns.size());
        
        //for each ComponentDefn
        for (Iterator iter = defns.iterator(); iter.hasNext();) {
            ComponentDefn defn = (ComponentDefn) iter.next();
            Collection componentTypeDefns = getComponentTypeDefinitions(defn.getComponentTypeID());
    
            boolean result = ComponentCryptoUtil.checkPropertiesDecryptable(defn, componentTypeDefns);
            results.add(new Boolean(result));
        }
        
        return results;
    }   
}

