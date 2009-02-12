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

package com.metamatrix.platform.config.spi.xml;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import com.metamatrix.common.config.api.ComponentDefn;
import com.metamatrix.common.config.api.ComponentDefnID;
import com.metamatrix.common.config.api.ComponentObjectID;
import com.metamatrix.common.config.api.ComponentType;
import com.metamatrix.common.config.api.ComponentTypeID;
import com.metamatrix.common.config.api.Configuration;
import com.metamatrix.common.config.api.ConfigurationID;
import com.metamatrix.common.config.api.ConfigurationModelContainer;
import com.metamatrix.common.config.api.Host;
import com.metamatrix.common.config.api.HostID;
import com.metamatrix.common.config.api.SharedResource;
import com.metamatrix.common.config.api.exceptions.ConfigurationException;
import com.metamatrix.common.connection.ManagedConnection;
import com.metamatrix.common.transaction.TransactionException;
import com.metamatrix.platform.config.ConfigMessages;
import com.metamatrix.platform.config.ConfigPlugin;

public class XMLConfigurationReader  {


    private static XMLConfigurationMgr configMgr = XMLConfigurationMgr.getInstance();
    private ConfigUserTransactionFactory factory;

//	private ManagedConnection mgdConnection;

 //   private static ConfigurationObjectEditor editor;


	public XMLConfigurationReader(ManagedConnection mgdConnection) {
//		this.mgdConnection = mgdConnection;

//		editor = new BasicConfigurationObjectEditor(false);

	    factory = new ConfigUserTransactionFactory();


	}

    public Host getHost(HostID hostID) throws ConfigurationException{

        ConfigurationModelContainer config = getConfigurationModel(Configuration.NEXT_STARTUP_ID);
        return config.getConfiguration().getHost(hostID.getFullName());

    }

    public Collection getHosts() throws ConfigurationException{

        ConfigurationModelContainer config = getConfigurationModel(Configuration.NEXT_STARTUP_ID);

        return config.getConfiguration().getHosts();
    }

    /**
     * Obtain the Date that represents the time the server was started
     * @return Date
     * @throws ConfigurationException if an business error occurred within or during communication with the Configuration Service.
     */
    public java.util.Date getServerStartupTime() throws ConfigurationException {
        java.util.Date timestamp = configMgr.getServerStartupTime();
        return timestamp;
    }


    public ComponentType getComponentType(ComponentTypeID typeID) throws ConfigurationException{
        if ( typeID == null ) {
            throw new ConfigurationException(ConfigMessages.CONFIG_0127, ConfigPlugin.Util.getString(ConfigMessages.CONFIG_0127));
        }

   		ConfigurationModelContainer model = getConfigurationModel(Configuration.NEXT_STARTUP_ID);

		return model.getComponentType(typeID.getFullName());

    }


    public Collection getComponentTypes(boolean includeDeprecated) throws ConfigurationException{

        ConfigurationModelContainer config = getConfigurationModel(Configuration.NEXT_STARTUP_ID);

        Map typeMap = config.getComponentTypes();
        Collection types = new ArrayList(typeMap.size());
        types.addAll(typeMap.values());
//        for (Iterator it=typeMap.values().iterator(); it.hasNext(); ) {
//            types.add(it.next());
//        }

        return types;
    }
    
    public Collection getProductTypes(boolean includeDeprecated) throws ConfigurationException  {
        
        ConfigurationModelContainer config = getConfigurationModel(Configuration.NEXT_STARTUP_ID);

        Collection intypes = config.getProductTypes();
        Collection types = new ArrayList(intypes.size());
        types.addAll(intypes);

        return types;
        
    }



   public String getComponentPropertyValue(ComponentObjectID componentObjectID, ComponentTypeID typeID, String propertyName) throws ConfigurationException{


        String result = null;
/*
            UniqueID uid = getUniqueUID(componentObjectID, jdbcConnection);

            if (componentObjectID instanceof DeployedComponentID) {
                sql = JDBCConfigurationTranslator.SELECT_PROPERTY_VALUE_FOR_DEPLOYED_COMPONENT;
                statement = jdbcConnection.prepareStatement(sql);
                statement.setInt(1,(int)uid.getValue());
                statement.setString(2,propertyName);

            } else {

                sql = JDBCConfigurationTranslator.SELECT_PROPERTY_VALUE_FOR_COMPONENT;
                statement = jdbcConnection.prepareStatement(sql);
                statement.setInt(1,(int)uid.getValue());
                statement.setString(2,typeID.getFullName());
                statement.setString(3,propertyName);
            }

*/
        return result;
    }
/*
    public ProductServiceConfig getProductServiceConfig(ServiceComponentDefnID serviceID) throws ConfigurationException{
        ProductServiceConfig result = null;

            ConfigurationID configurationID = getDesignatedConfigurationID(serviceID.getParentFullName());
 //           ComponentDefn defn = getComponentDefinition(serviceID, configurationID);
 //           if (defn == null) {
 //               throw new ConfigurationException("Service Component was not found for id " + serviceID );
 //           }

            ConfigurationModelContainer config = getConfigurationModel(configurationID);

            ProductServiceConfigID pscID = config.getConfiguration().getPSCForServiceDefn(serviceID);

            if (pscID == null) {
                throw new ConfigurationException("No PSC was found to contain service id " + serviceID );
            }

            result = (ProductServiceConfig) config.getConfiguration().getComponentDefn(pscID);

            if (pscID == null) {
                throw new ConfigurationException("Configuration Error: Matched PSC ID to service id " + serviceID + " but no PSC object found" );
            }

        return result;
    }
*/


    public ComponentDefn getComponentDefinition(ComponentDefnID targetID) throws ConfigurationException{
        if (targetID == null) {
            throw new ConfigurationException(ConfigMessages.CONFIG_0045,ConfigPlugin.Util.getString(ConfigMessages.CONFIG_0045,"ComponentDefnID")); //$NON-NLS-1$
        }

        ConfigurationID configurationID = getDesignatedConfigurationID(targetID.getParentFullName());

        ComponentDefn defn = getComponentDefinition(targetID, configurationID);

/* Should already be populated
 *
        if (defn instanceof ProductServiceConfig){
            defn = populateProductServiceConfig((ProductServiceConfig)defn);
        }
*/
        return defn;

    }

    public ComponentDefn getComponentDefinition(ComponentDefnID targetID, ConfigurationID configurationID) throws ConfigurationException{

        if (targetID == null) {
            throw new ConfigurationException(ConfigMessages.CONFIG_0045,ConfigPlugin.Util.getString(ConfigMessages.CONFIG_0045,"ComponentDefnID")); //$NON-NLS-1$
        }
        ComponentDefn defn = null;


        if (configurationID == null) {
            configurationID = getDesignatedConfigurationID(targetID.getParentFullName());
        }

        ConfigurationModelContainer config = getConfigurationModel(configurationID);
        defn = config.getConfiguration().getComponentDefn(targetID);


/** should already be populated
        if (defn instanceof ProductServiceConfig){
            defn = populateProductServiceConfig((ProductServiceConfig)defn);
        }

*/
        return defn;

    }



    public Map getComponentDefinitions(ConfigurationID configID ) throws ConfigurationException {

        ConfigurationModelContainer config = getConfigurationModel(configID);

        return config.getConfiguration().getComponentDefns();
    }


    public Collection getConnectionPools(ConfigurationID configurationID ) throws ConfigurationException {

   		ConfigurationModelContainer model = getConfigurationModel(configurationID);

		return model.getConnectionPools();

    }

/*
    private ComponentType buildComponentType(ResultSet results, boolean includeDeprecated ) throws ConfigurationException, SQLException {

        //    set default type code
        int compTypeCode = ComponentType.CONFIGURATION_COMPONENT_TYPE_CODE;



        if (isComponentTypeOfTypeConfiguration(results)) {
        } else if (isComponentTypeOfTypeConnector(results, jdbcConnection)) {
            compTypeCode = ComponentType.CONNECTOR_COMPONENT_TYPE_CODE;
        } else if (isComponentTypeOfTypeProduct(results, jdbcConnection)) {
            compTypeCode = ComponentType.PRODUCT_COMPONENT_TYPE_CODE;
        } else if (isComponentTypeofTypeResource(results, jdbcConnection)) {
            compTypeCode = ComponentType.RESOURCE_COMPONENT_TYPE_CODE;
        } else {
            compTypeCode = ComponentType.SERVICE_COMPONENT_TYPE_CODE;
        }


        ComponentType type = JDBCConfigurationTranslator.getComponentType(compTypeCode, results);
        Collection defns = null;

        if (includeDeprecated) {
            defns = getComponenTypeDefinitions( (ComponentTypeID) type.getID(), jdbcConnection);
            type = editor.setComponentTypeDefinitions(type, defns);

        } else if (!type.isDeprecated()) {
            defns = getComponenTypeDefinitions( (ComponentTypeID) type.getID(), jdbcConnection);
            type = editor.setComponentTypeDefinitions(type, defns);

        }

        if (type instanceof ProductType){
            String sql = null;
            PreparedStatement statement = null;
            try{
                sql = JDBCConfigurationTranslator.SELECT_COMPONENT_TYPES_BY_PARENT_NAME;
                statement = jdbcConnection.prepareStatement(sql);
                statement.setString(1, type.getFullName());

                if ( ! statement.execute() ) {
                    throw new ConfigurationException("Failed to execute the query \"" + sql + "\"");
                }
                ResultSet moreResults = statement.getResultSet();

                // check here for the next row, because this is flaky if you pass the results
                // back to the calling method to do the checking.
                if (moreResults.next()) {
                    type = populateProductType(moreResults, (ProductType)type);
                }

            } catch ( SQLException e ) {
                throw new ConfigurationException(e, "Failed to execute the query \"" + sql + "\" and/or process the results");
            } catch ( Exception e ) {
                if (e instanceof ConfigurationException){
                    throw (ConfigurationException)e;
                }
                throw new ConfigurationException(e);
            } finally {
                if ( statement != null ) {
                    try {
                        statement.close();
                        statement=null;
                    } catch ( SQLException e ) {
                        e.printStackTrace();
                        System.out.println("Unable to close the statement for query \"" + sql + "\"");
                    }
                }
            }
        }

        return type;
    }
*/
    /**
     * Takes a ProductType, and puts into it the ComponentTypeID objects
     * representing legal service types for that product type.
     */
/*
    private ProductType populateProductType(ResultSet results, ProductType prodType) throws ConfigurationException, SQLException {
        Iterator serviceTypes = JDBCConfigurationTranslator.getComponentTypes(ComponentType.SERVICE_COMPONENT_TYPE_CODE, results).iterator();
        ComponentType serviceType = null;
        while (serviceTypes.hasNext()){
            serviceType = (ComponentType)serviceTypes.next();
            prodType = editor.addServiceComponentType(prodType, serviceType);
        }
        return prodType;
    }
*/


    public Collection getComponenTypeDefinitions(ComponentTypeID componentTypeID ) throws ConfigurationException{

        ConfigurationModelContainer config =  getConfigurationModel(Configuration.NEXT_STARTUP_ID);
        ComponentType t = config.getComponentType(componentTypeID.getFullName());
        if (t!= null) {
            return t.getComponentTypeDefinitions();
        }
        return Collections.EMPTY_LIST;

    }

    /**
     * The results should contain 1 or more rows of property defn information.  Only the
     * uid will obtained from the results in order to get the allowed values.
     */
/*
    private List getPropertyDefnAllowedValues(ComponentTypeDefnID typeDefnID, ComponentTypeID typeID, UniqueID uniqueID ) throws ConfigurationException{

        if ( jdbcConnection == null) {
            throw new ConfigurationException("The current (JDBC) configuration reader is not connected");
        }

        List result = null;
        String sql = null;
        PreparedStatement statement = null;
        try {

            sql = JDBCConfigurationTranslator.SELECT_ALL_PROPERTY_DEFN_ALLOWED_VALUES;
            statement = jdbcConnection.prepareStatement(sql);
            statement.setInt(1, (int)uniqueID.getValue());

            if ( ! statement.execute() ) {
                throw new ConfigurationException("Failed to execute the query \"" + sql + "\"");
            }
            ResultSet resultValues = statement.getResultSet();

            if (resultValues.next()) {
                result = JDBCConfigurationTranslator.getPropertyDefnAllowedValues(typeDefnID, typeID, resultValues);
            }

        } catch ( SQLException e ) {
            throw new ConfigurationException(e, "Failed to execute the query \"" + sql + "\" with parameter(s) \"" + uniqueID + "\" and/or process the results");
        } catch ( Exception e ) {
            if (e instanceof ConfigurationException){
                throw (ConfigurationException)e;
            }
            throw new ConfigurationException(e);
        } finally {
            if ( statement != null ) {
                try {
                    statement.close();
                    statement=null;
                } catch ( SQLException e ) {
                    e.printStackTrace();
                    System.out.println("Unable to close the statement for query \"" + sql + "\"");
                }
            }
        }

        return result;
    }
*/


    /**
    * Returns a UIDCollection of all the deployed components for the configuration.  These
    * components will be complete with properties
    */
    public List getDeployedComponents(ConfigurationID configurationID ) throws ConfigurationException {
    // 1st get the deployed components and then add their properties

        ConfigurationModelContainer config = getConfigurationModel(configurationID);


        Collection dcs = config.getConfiguration().getDeployedComponents();
        List result = new LinkedList();
        result.addAll(dcs);
        return result;

    }

/*
    public Map getResourcesProperties() throws ConfigurationException{
        return configMgr.getResourcesProperties();
    }
*/
    /**
     * @returns Collection of type ResourceDescriptor
     */
    public Collection getResources() throws ConfigurationException{
    	ConfigurationModelContainer cmc = getConfigurationModel(Configuration.NEXT_STARTUP_ID);

        return cmc.getResources();

    }


   public SharedResource getResource(String resourceName ) throws ConfigurationException {
    	ConfigurationModelContainer cmc = getConfigurationModel(Configuration.NEXT_STARTUP_ID);

   		return cmc.getResource(resourceName);

    }

       /**
     * Returns the int startup state, use constants in
     * {@link com.metamatrix.common.config.StartupStateController} to
     * interpret the meaning
     */
    public int getServerStartupState() throws ConfigurationException{
        return configMgr.getServerStartupState();
    }



    public boolean doesResourceExist(String resourceName ) throws ConfigurationException {
        SharedResource rd = getResource(resourceName);
        return rd != null;
    }


    public boolean isDefinitionDeployable(ComponentDefnID defnID ) throws ConfigurationException{
        ComponentDefn defn = getComponentDefinition(defnID);

        ComponentType type = getComponentType(defn.getComponentTypeID());

        return type.isDeployable();
    }

    // ------------------------------------------------------------------------
    // PRIVATE METHODS
    // ------------------------------------------------------------------------
/*
    private ComponentDefn getComponentDefinition(String componentName, String parentName, Connection jdbcConnection) throws ConfigurationException{
        Configuration config = getConfigurationByName(parentName);


       ComponentDefnID id = editor.



        return null;
    }
*/
//    /**
//    *   return true if the passed result set is the Connector component type or
//    *   it is a sub type of the Connector component type.
//    */
//    private boolean isComponentTypeOfTypeConnector(ComponentType type) throws ConfigurationException, SQLException {
//        return isComponentTypeOfIndicatedType(type, ConnectorBindingType.COMPONENT_TYPE_NAME);
//    }
//
//    /**
//    *   return true if the passed result set is the Service component type or
//    *   it is a sub type of the Service component type.
//    */
//    private boolean isComponentTypeOfTypeService(ComponentType type ) throws ConfigurationException, SQLException {
//        return isComponentTypeOfIndicatedType(type, ServiceComponentType.COMPONENT_TYPE_NAME);
//    }
//
//    /**
//     * Return true if the passed result set is the Product component type or
//     * it is a sub type of the Product component type.
//     */
//    private boolean isComponentTypeOfTypeProduct(ComponentType type ) throws ConfigurationException, SQLException {
//        return isComponentTypeOfIndicatedType(type, ProductType.COMPONENT_TYPE_NAME);
//    }

//    /**
//    *   return true if the passed result set if is the Resource component type or
//    *   it is a sub type of the Resource component type.
//    */
//    private boolean isComponentTypeofTypeResource(ComponentType type ) throws ConfigurationException, SQLException {
//        return isComponentTypeOfIndicatedType(type, SharedResourceComponentType.COMPONENT_TYPE_NAME);
//    }

//    /**
//     * Return true if the passed result set is the indicated component type or
//     * it is a sub type of the indicated component type.
//     * @param typeName indicated type name
//     */
//    private boolean isComponentTypeOfIndicatedType(ComponentType type , String typeName) throws ConfigurationException, SQLException {
//
//        String name =  type.getFullName();
//    // if the result row is a service type then return true
//        if (name.equals(typeName)) {
//            return true;
//        }
//
//        String superName = type.getSuperComponentTypeID().getFullName();
//
//    // traverse the super type hierarchy to determine
//    // if this result row has a super type of indicated type
//        while (true) {
//            if (superName == null) {
//                return false;
//            } else if (superName.equals(typeName)) {
//                return true;
//            }
//
//            type = getComponentType(type.getSuperComponentTypeID());
//
//            superName = type.getSuperComponentTypeID().getFullName();
//        }
//    }


//    /**
//    *   return true if the passed result set is the Service component type or
//    *   it is a super class of the Service component type.
//    */
//    private boolean isComponentTypeOfTypeConfiguration(ComponentType type) throws ConfigurationException, SQLException {
//
//        // if it has a supername then it is not part of the configuration types
//        String superName = type.getSuperComponentTypeID().getFullName();
//        if (superName != null) {
//            return false;
//        } else {
//            String name =  type.getFullName();
//            if (name.equals(ServiceComponentType.COMPONENT_TYPE_NAME)) {
//                return false;
//            }
//
//            return true;
//        }
//    }


    /**
     * Returns ID of one of the well-known
     * {@link SystemConfigurationNames system configurations}, either
     * the
     * {@link SystemConfigurationNames#OPERATIONAL operational configuration},
     * the
     * {@link SystemConfigurationNames#NEXT_STARTUP next startup configuration},
     * or the
     * {@link SystemConfigurationNames#STARTUP startup configuration}.  Use
     * {@link SystemConfigurationNames} to supply the String parameter.  Will
     * return null if the designation parameter is invalid.
     * @param designation String indicating which of the system configurations
     * is desired; use one of the {@link SystemConfigurationNames} constants
     * @param jdbcConnection connection to the proper config database
     * @return the desired ConfigurationID
     * @throws ConfigurationException if an error occurred within or during
     * communication with the Configuration Service.
     */
    public ConfigurationID getDesignatedConfigurationID(String designation ) throws ConfigurationException{
        // This was changed to public so installation could use the method

		if (designation.startsWith(Configuration.NEXT_STARTUP) ) {
            return Configuration.NEXT_STARTUP_ID;
        } else if (designation.startsWith(Configuration.STARTUP)) {
            return Configuration.STARTUP_ID;
        } else {
            throw new ConfigurationException(ConfigMessages.CONFIG_0128, ConfigPlugin.Util.getString(ConfigMessages.CONFIG_0128, designation ));
        }


    }




    /**
     * used specifically by the CurrentConfiguration via
     * JDBCCurrentConfigurationReader
     */
    public Properties getDesignatedConfigurationProperties(String designation ) throws ConfigurationException{

        ConfigurationModelContainer config = getConfigurationModel(getDesignatedConfigurationID(designation));

        return config.getConfiguration().getProperties();

    }



    /**
     * Returns the Configuration for the given name, or null if the ID is
     * invalid.
     * @param name of a valid Configuration
     * @param configurationID ID of a valid Configuration
     * @return jdbcConnection Connection to the configuration data source
     * @throws ConfigurationException if a problem occurs communicating with
     * the data source
     */
    public Configuration getDesignatedConfiguration(String name) throws ConfigurationException {
        ConfigurationID id = getDesignatedConfigurationID(name);

        return getDesignatedConfiguration(id);

    }

   	public ConfigurationModelContainer getConfigurationModel(ConfigurationID configID) throws ConfigurationException {
   	        ConfigurationModelContainer config = configMgr.getConfigurationModel(configID);

   			return config;
   	}


    /**
     * Returns one of the well-known
     * {@link SystemConfigurationNames system configurations}, either
     * the
     * {@link SystemConfigurationNames#OPERATIONAL operational configuration},
     * the
     * {@link SystemConfigurationNames#NEXT_STARTUP next startup configuration},
     * or the
     * {@link SystemConfigurationNames#STARTUP startup configuration}.  Use
     * {@link SystemConfigurationNames} to supply the String parameter.  Will
     * return null if the designation parameter is invalid.
     * @param designation String indicating which of the system configurations
     * is desired; use one of the {@link SystemConfigurationNames} constants
     * @return the desired Configuration
     * @throws ConfigurationException if an error occurred within or during
     * communication with the Configuration Service.
     */
    public Configuration getDesignatedConfiguration(ConfigurationID configurationID ) throws ConfigurationException{

		ConfigurationModelContainer model = getConfigurationModel(configurationID);

        return model.getConfiguration();

    }


    public Collection getMonitoredComponentTypes(boolean includeDeprecated) throws ConfigurationException{
        return Collections.EMPTY_LIST;
    }

    public ConfigUserTransaction getTransaction(String principal) throws ConfigTransactionException  {
		ConfigUserTransaction userTrans = null;
		try {

	        userTrans = factory.createReadTransaction(principal);

	        userTrans.begin();

	        return userTrans;

	    } catch (TransactionException te) {
	    	if (userTrans != null) {
				try {
					userTrans.rollback();
				} catch (Exception e) {
				}

	    	}
			if (te instanceof ConfigTransactionException) {
				throw (ConfigTransactionException) te;
			}


			throw new ConfigTransactionException(te, ConfigMessages.CONFIG_0129, ConfigPlugin.Util.getString(ConfigMessages.CONFIG_0129, principal));

	    }


    }



}
