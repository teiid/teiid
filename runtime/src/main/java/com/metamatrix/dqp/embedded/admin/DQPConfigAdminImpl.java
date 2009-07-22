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

package com.metamatrix.dqp.embedded.admin;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Properties;
import java.util.StringTokenizer;

import org.teiid.adminapi.AdminComponentException;
import org.teiid.adminapi.AdminException;
import org.teiid.adminapi.AdminObject;
import org.teiid.adminapi.AdminOptions;
import org.teiid.adminapi.AdminProcessingException;
import org.teiid.adminapi.AdminStatus;
import org.teiid.adminapi.ConfigurationAdmin;
import org.teiid.adminapi.LogConfiguration;
import org.teiid.adminapi.VDB;

import com.metamatrix.admin.objects.MMAdminObject;
import com.metamatrix.admin.objects.MMAdminStatus;
import com.metamatrix.api.exception.MetaMatrixComponentException;
import com.metamatrix.common.application.exception.ApplicationLifecycleException;
import com.metamatrix.common.config.api.ComponentType;
import com.metamatrix.common.config.api.ComponentTypeDefn;
import com.metamatrix.common.config.api.ComponentTypeID;
import com.metamatrix.common.config.api.ConfigurationModelContainer;
import com.metamatrix.common.config.api.ConnectorArchive;
import com.metamatrix.common.config.api.ConnectorBinding;
import com.metamatrix.common.config.api.ConnectorBindingType;
import com.metamatrix.common.config.api.ExtensionModule;
import com.metamatrix.common.config.model.BasicConnectorArchive;
import com.metamatrix.common.config.model.BasicExtensionModule;
import com.metamatrix.common.log.LogManager;
import com.metamatrix.common.util.crypto.CryptoException;
import com.metamatrix.common.util.crypto.CryptoUtil;
import com.metamatrix.common.vdb.api.VDBArchive;
import com.metamatrix.common.vdb.api.VDBDefn;
import com.metamatrix.core.vdb.VDBStatus;
import com.metamatrix.dqp.embedded.DQPEmbeddedPlugin;
import com.metamatrix.dqp.embedded.DQPEmbeddedProperties;
import com.metamatrix.dqp.embedded.configuration.ConnectorConfigurationReader;
import com.metamatrix.dqp.embedded.configuration.ConnectorConfigurationWriter;
import com.metamatrix.dqp.embedded.configuration.ServerConfigFileWriter;
import com.metamatrix.dqp.embedded.configuration.VDBConfigurationReader;
import com.metamatrix.dqp.service.ConfigurationService;
import com.metamatrix.jdbc.EmbeddedConnectionFactoryImpl;


/** 
 * DQP implementation of the Config Admin API
 * @since 4.3
 */
public class DQPConfigAdminImpl extends BaseAdmin implements ConfigurationAdmin {
    
    public DQPConfigAdminImpl(EmbeddedConnectionFactoryImpl manager) {
        super(manager);
    }

    /** 
     * @see org.teiid.adminapi.ConfigurationAdmin#setSystemProperty(java.lang.String, java.lang.String)
     * @since 4.3
     */
    public void setSystemProperty(String propertyName,String propertyValue) 
        throws AdminException {
        try {
            // actually we should notify the DQP for this, then let DQP call the 
            // the Configuration Service. Since we do not have dynamic properties on
            // DQP this should be OK for now.
            getConfigurationService().setSystemProperty(propertyName, propertyValue);            
        } catch (MetaMatrixComponentException e) {
        	throw new AdminComponentException(e);
        }
    }
    
   
    /** 
     * @see org.teiid.adminapi.ConfigurationAdmin#updateSystemProperties(java.util.Properties)
     * @since 4.3
     */
    public void updateSystemProperties(Properties properties) throws AdminException {
        try {
            // actually we should notify the DQP for this, then let DQP call the 
            // the Configuration Service. Since we do not have dynamic properties on
            // DQP this should be OK for now.
            getConfigurationService().updateSystemProperties(properties);            
        }catch(MetaMatrixComponentException e) {
        	throw new AdminComponentException(e);
        }
    }

    /** 
     * @see org.teiid.adminapi.ConfigurationAdmin#setProperty(java.lang.String, java.lang.String, java.lang.String, java.lang.String)
     * @since 4.3
     */
    public void setProperty(String identifier, String className, String propertyName, String propertyValue) 
        throws AdminException {

        Properties properties = new Properties();
        properties.setProperty(propertyName, propertyValue);
        
        updateProperties(identifier, className, properties);
        
    }
    
    /** 
     * @see org.teiid.adminapi.ConfigurationAdmin#updateProperties(java.lang.String, java.lang.String, java.util.Properties)
     * @since 4.3
     */
    public void updateProperties(String identifier, String className, Properties properties) throws AdminException {
        
        Collection adminObjects = getAdminObjects(identifier, className);        
        if (adminObjects == null || adminObjects.size() == 0) {
            throw new AdminProcessingException(DQPEmbeddedPlugin.Util.getString("Admin.No_Objects_Found", identifier, className)); //$NON-NLS-1$
        }
        if (adminObjects.size() > 1) {
            throw new AdminProcessingException(DQPEmbeddedPlugin.Util.getString("Admin.Multiple_Objects_Found", identifier, className)); //$NON-NLS-1$
        }
        AdminObject adminObject = (AdminObject) adminObjects.iterator().next();
        
        
        int typeCode = MMAdminObject.getObjectType(className);
        switch(typeCode) {
        
            case MMAdminObject.OBJECT_TYPE_SYSTEM_OBJECT:
                updateSystemProperties(properties);
                break;
        
            case MMAdminObject.OBJECT_TYPE_CONNECTOR_BINDING:
                try {
                    // there is lot of engineering under here..
                    String bindingName = adminObject.getName();
                    ConnectorBinding binding = getConfigurationService().getConnectorBinding(bindingName);
                    ComponentTypeID id = binding.getComponentTypeID();
                    ConnectorBindingType type = getConfigurationService().getConnectorType(id.getName());
                    
                    
                    //encrypt the properties
                    Properties encryptedProperties = new Properties();
                    encryptedProperties.putAll(properties);
                    
                    for (Iterator iter = properties.keySet().iterator(); iter.hasNext(); ) {
                        String propertyName = (String) iter.next();
                        boolean needsEncryption = isMaskedProperty(propertyName, type);
                        if (needsEncryption) {
                            String propertyValue = properties.getProperty(propertyName);
                            propertyValue = encryptString(propertyValue);
                            encryptedProperties.put(propertyName, propertyValue);
                        }
                    }
                    
                    //update the configuration
                    binding = ConnectorConfigurationReader.addConnectorBindingProperties(binding, encryptedProperties);
                    getConfigurationService().updateConnectorBinding(binding);
                    
                } catch (MetaMatrixComponentException e) {
                	throw new AdminComponentException(e);
                } 
                break;
       
            default:
                throw new AdminProcessingException(DQPEmbeddedPlugin.Util.getString("Admin.can_not_set_property")); //$NON-NLS-1$
            }
        }

    /** 
     * @see org.teiid.adminapi.ConfigurationAdmin#addConnectorType(java.lang.String, char[])
     * @since 4.3
     */
    public void addConnectorType(String deployName, char[] cdkFile) 
        throws AdminException {
        try {
            if (deployName == null || !deployName.matches(MULTIPLE_WORDS_REGEX)) {
                throw new AdminProcessingException(DQPEmbeddedPlugin.Util.getString("Admin.Invalid_ct_name")); //$NON-NLS-1$                
            }
            if (cdkFile == null || cdkFile.length == 0) {
                throw new AdminProcessingException(DQPEmbeddedPlugin.Util.getString("Admin.Invalid_ct_source")); //$NON-NLS-1$
            }
            
            // This is only place we check the existence in admin. Generally the Admin is not the
            // guy to decide, if it can take in or not, it should be the service. I did not 
            // want add in the configuration service beacuse, it may need to allow this behavior 
            // in case we are updating.  
            if (getConfigurationService().getConnectorType(deployName) == null) {            
                ConnectorBindingType type = ConnectorConfigurationReader.loadConnectorType(cdkFile);            
                saveConnectorType(type);
            }
            else {
                throw new AdminProcessingException(DQPEmbeddedPlugin.Util.getString("Admin.Connector_type_exists", deployName)); //$NON-NLS-1$
            }
        } catch (MetaMatrixComponentException e) {
        	throw new AdminComponentException(e);
		}            
    }

    /** 
     * @see org.teiid.adminapi.ConfigurationAdmin#deleteConnectorType(java.lang.String)
     * @since 4.3
     */
    public void deleteConnectorType(String deployName) 
        throws AdminException {
        try {
            if (deployName == null || !deployName.matches(MULTIPLE_WORDS_REGEX)) {
                throw new AdminProcessingException(DQPEmbeddedPlugin.Util.getString("Admin.Invalid_ct_name")); //$NON-NLS-1$                
            }            
            getConfigurationService().deleteConnectorType(deployName);
        } catch (MetaMatrixComponentException e) {
        	throw new AdminComponentException(e);
		}         
    }

    /** 
     * @see org.teiid.adminapi.ConfigurationAdmin#addConnectorBinding(java.lang.String, java.lang.String, java.util.Properties, AdminOptions)
     * @since 4.3
     */
    public org.teiid.adminapi.ConnectorBinding addConnectorBinding(String deployName, String type, Properties properties, AdminOptions options) 
        throws AdminException {
        // if the options object is null treat as if it is IGNORE as default
        if (options == null) {
            options = new AdminOptions(AdminOptions.OnConflict.IGNORE);
        }

        if (deployName == null || !deployName.matches(MULTIPLE_WORDS_REGEX)) {
            throw new AdminProcessingException(DQPEmbeddedPlugin.Util.getString("Admin.Invalid_cb_name")); //$NON-NLS-1$                
        }
        
        if (type == null || !type.matches(MULTIPLE_WORDS_REGEX)) {
            throw new AdminProcessingException(DQPEmbeddedPlugin.Util.getString("Admin.Invalid_ct_name")); //$NON-NLS-1$                
        }
                
        ConnectorBinding binding = null;
        try {
            // Check if the binding exists already, if does take action based on user
            // preferences in the admin options
            if (bindingExists(deployName)) {
                // Based on users preference, either add or replace or ignore 
                if (options.containsOption(AdminOptions.OnConflict.EXCEPTION)) {
                    throw new AdminProcessingException(DQPEmbeddedPlugin.Util.getString("Admin.addBindingEixists", deployName)); //$NON-NLS-1$
                }
                else if (options.containsOption(AdminOptions.OnConflict.IGNORE)) {
                    binding = getDataService().getConnectorBinding(deployName);
                    return (org.teiid.adminapi.ConnectorBinding) convertToAdminObjects(binding);
                }
            }
            
            // Get the connector type
            ConnectorBindingType ctype = getConfigurationService().getConnectorType(type);
            if (ctype == null) {
                throw new AdminProcessingException(DQPEmbeddedPlugin.Util.getString("Admin.connector_type_not_exists", type)); //$NON-NLS-1$
            }
            
            // Build the connector binding with informatin we know.
            binding = ConnectorConfigurationReader.loadConnectorBinding(deployName, properties, ctype);
            
            // Check that the connector binding passwords can be decrypted
            AdminStatus status = checkDecryption(binding, ctype);
            if ( status.getCode() == AdminStatus.CODE_DECRYPTION_FAILED && 
                 ! options.containsOption(AdminOptions.BINDINGS_IGNORE_DECRYPT_ERROR)) {
                throw new AdminProcessingException(status.getCode(), status.getMessage());
            }
            
            // now that all of the input parameters validated, add the connector binding
            binding = addConnectorBinding(deployName, binding, ctype, true);
            
        } catch (MetaMatrixComponentException e) {
        	throw new AdminComponentException(e);
        }
        return (org.teiid.adminapi.ConnectorBinding) convertToAdminObjects(binding);
    }

    boolean bindingExists(String name) throws MetaMatrixComponentException {
        ConnectorBinding binding = getDataService().getConnectorBinding(name);
        return (binding != null);
    }
    
    boolean bindingTypeExists(String name) throws MetaMatrixComponentException {
        ConnectorBindingType type = getConfigurationService().getConnectorType(name);
        return (type != null);
    }
    
    /** 
     * @see org.teiid.adminapi.ConfigurationAdmin#addConnectorBinding(java.lang.String, char[], AdminOptions)
     * @since 4.3
     */
    public org.teiid.adminapi.ConnectorBinding addConnectorBinding(String deployName, char[] xmlFile, AdminOptions options) 
        throws AdminException {
        
        // if the options object is null treat as if it is IGNORE as default
        if (options == null) {
            options = new AdminOptions(AdminOptions.OnConflict.IGNORE);
        }

        if (deployName == null || !deployName.matches(MULTIPLE_WORDS_REGEX)) {
            throw new AdminProcessingException(DQPEmbeddedPlugin.Util.getString("Admin.Invalid_cb_name")); //$NON-NLS-1$                
        }
        if (xmlFile == null || xmlFile.length == 0) {
            throw new AdminProcessingException(DQPEmbeddedPlugin.Util.getString("Admin.Invalid_cb_source")); //$NON-NLS-1$
        }
                
        ConnectorBinding binding = null;
        try {
            // Check if the binding exists already, if does take action based on user
            // preferences in the admin options
            if (bindingExists(deployName)) {
                // Based on users preference, either add or replace or ignore 
                if (options.containsOption(AdminOptions.OnConflict.EXCEPTION)) {
                    throw new AdminProcessingException(DQPEmbeddedPlugin.Util.getString("Admin.addBindingEixists", deployName)); //$NON-NLS-1$
                }
                else if (options.containsOption(AdminOptions.OnConflict.IGNORE)) {
                    binding = getDataService().getConnectorBinding(deployName);
                    return (org.teiid.adminapi.ConnectorBinding) convertToAdminObjects(binding);
                }
            }
            
            // now we are in situation we do have the connector or overwriting it.
            // before we add the connector binding we need to add the connector type
            // as the connector binding only references to type by identifier.
            ConnectorBindingType type = ConnectorConfigurationReader.loadConnectorType(xmlFile);
            
            // Check if the binding type exists already, if does take action based on user
            // preferences in the admin options, same rules apply as binding.            
            if (bindingTypeExists(type.getName())) {
                if (options.containsOption(AdminOptions.OnConflict.EXCEPTION)) {
                    throw new AdminProcessingException(DQPEmbeddedPlugin.Util.getString("Admin.addBinding_type_exists", deployName, type.getName())); //$NON-NLS-1$
                }                
            }
            
            binding = ConnectorConfigurationReader.loadConnectorBinding(deployName, xmlFile);
            
            // Check that the connector binding passwords can be decrypted
            AdminStatus status = checkDecryption(binding, type);
            if ( status.getCode() == AdminStatus.CODE_DECRYPTION_FAILED && 
                 ! options.containsOption(AdminOptions.BINDINGS_IGNORE_DECRYPT_ERROR)) {
                throw new AdminProcessingException(status.getCode(), status.getMessage());
            }

            // now that all of the input parameters validated, add the connector binding
            binding = addConnectorBinding(deployName, binding, type, true);
                                
        } catch (MetaMatrixComponentException e) {
        	throw new AdminComponentException(e);
        }
        
        return (org.teiid.adminapi.ConnectorBinding) convertToAdminObjects(binding);
    }

    /**
     * Helper method to add the connector binding.. 
     * @param deployName
     * @param binding
     * @param type
     * @param options
     * @throws AdminException
     */
    ConnectorBinding addConnectorBinding(String deployName, ConnectorBinding binding, ConnectorBindingType type, boolean replace) 
        throws AdminException {
        // Make sure we have both correctly configured
        if (type != null && binding != null) {
            if (binding.getComponentTypeID().getName().equals(type.getName())) {
                try {
                    
                    // First add the connector type if one is not already in here.
                    if (getConfigurationService().getConnectorType(type.getName()) == null || replace) {
                        saveConnectorType(type);
                    }
                    // Now add the connector binding.
                    binding = getConfigurationService().addConnectorBinding(deployName, binding, replace);
                    return binding;
                } catch (MetaMatrixComponentException e) {
                	throw new AdminComponentException(e);
                } 
            }
            throw new AdminProcessingException(DQPEmbeddedPlugin.Util.getString("Admin.connector_load_failed_wrong_type", deployName));  //$NON-NLS-1$                    
        }
        throw new AdminProcessingException(DQPEmbeddedPlugin.Util.getString("Admin.connector_load_failed_wrong_contents", deployName));  //$NON-NLS-1$        
    }
    
    /** 
     * @see org.teiid.adminapi.ConfigurationAdmin#deleteConnectorBinding(java.lang.String)
     * @since 4.3
     */
    public void deleteConnectorBinding(String identifier) 
        throws AdminException {
        try {
            if (identifier == null || !identifier.matches(MULTIPLE_WORD_WILDCARD_REGEX)) {
                throw new AdminProcessingException(DQPEmbeddedPlugin.Util.getString("Admin.Invalid_cb_name")); //$NON-NLS-1$
            }
            getConfigurationService().deleteConnectorBinding(identifier);
        } catch (MetaMatrixComponentException e) {
        	throw new AdminComponentException(e);
        }          
    }

    /** 
     * @see org.teiid.adminapi.ConfigurationAdmin#addVDB(java.lang.String, byte[], char[], AdminOptions)
     * @since 4.3
     */
    private VDB addVDB(String deployName, byte[] vdbFile, char[] defFile, AdminOptions options) 
        throws AdminException {
        
        // if the options object is null treat as if it is BINDINGS_ADD as default
        if (options == null) {
            options = new AdminOptions(AdminOptions.OnConflict.IGNORE);
        }
        
        if (deployName == null || !deployName.matches(SINGLE_WORD_REGEX)) {
            throw new AdminProcessingException(DQPEmbeddedPlugin.Util.getString("Admin.Invalid_vdb_name")); //$NON-NLS-1$                
        }
        if (vdbFile == null || vdbFile.length == 0) {
            throw new AdminProcessingException(DQPEmbeddedPlugin.Util.getString("Admin.Invalid_vdb_source")); //$NON-NLS-1$
        }        
        
        if (defFile == null) {
            DQPEmbeddedPlugin.logInfo("Admin.load_combined_vdb", new Object[] {deployName}); //$NON-NLS-1$
        }
        
        VDBArchive vdb = null;
        try {
        	// Load the VDB from the files
        	if (defFile == null) {
        		vdb = VDBConfigurationReader.loadVDB(deployName, vdbFile);
        	}
        	else {
        		vdb = VDBConfigurationReader.loadVDB(deployName, defFile, vdbFile);    
        	}

            // Add the connector binding in the VDB to the system
            validateConnectorBindingsInVdb(vdb, options);
            
            // now deploy the VDB into the system. Flag is to 
            VDBArchive deployedVDB = getConfigurationService().addVDB(vdb, !options.containsOption(AdminOptions.OnConflict.IGNORE));

            // If the connector bindings are correctly initialized and VDB is active
            // start the bindings automatically.
            if ( (deployedVDB.getStatus() == VDBStatus.ACTIVE) ||
              	  (deployedVDB.getStatus() == VDBStatus.ACTIVE_DEFAULT) ) {
                try {
                    startVDBConnectorBindings(deployedVDB);
                } catch (MetaMatrixComponentException e) {
                } catch (ApplicationLifecycleException e) {
                    // we can safely ignore these because the cause of the not starting is already recorded
                    // and more likely VDB deployment succeeded.
                }
            }
            
            return (VDB) convertToAdminObjects(deployedVDB);
        } catch (MetaMatrixComponentException e) {
        	throw new AdminComponentException(e);
        }
    }

    /** 
     * Start the connector bindings in the given VDB
     * @param vdb
     */
    private void startVDBConnectorBindings(VDBArchive vdb) throws MetaMatrixComponentException,
        ApplicationLifecycleException {
        
    	VDBDefn def = vdb.getConfigurationDef();
    	Collection<ConnectorBinding> bindings = def.getConnectorBindings().values();
 	 	for (ConnectorBinding binding:bindings) {
 	 		getDataService().startConnectorBinding(binding.getDeployedName());
 	 	}        
    }
    
    /**
     * Validate the connector bindings in a VDB. Since the connector bindings in VDB
     * are VDB scoped there is no meaning for the admin options provided. Just check
     * the decrypt properties.
     */
    void validateConnectorBindingsInVdb(VDBArchive vdb, AdminOptions options) 
        throws MetaMatrixComponentException, AdminProcessingException, AdminException {
        
    	VDBDefn def = vdb.getConfigurationDef();
    	
        int version = 0; 
        VDBArchive existing = null;
        do {
            version++;
            existing = getConfigurationService().getVDB(def.getName(), String.valueOf(version));            
        } while(existing != null);
                
        // Look for the connector bindings in the VDB            
        // Based on users preference, either add or replace or throw exception
        List vdbbindings = new ArrayList(def.getConnectorBindings().values());        
        
        for (Iterator i = vdbbindings.iterator(); i.hasNext();) {        
            ConnectorBinding binding = (ConnectorBinding)i.next();

            String deployName = binding.getDeployedName();
            if (deployName == null) {
            	deployName = binding.getFullName();
            }
            
            if (bindingExists(deployName)) {
                if (options.containsOption(AdminOptions.OnConflict.EXCEPTION)) {
                    throw new AdminProcessingException(DQPEmbeddedPlugin.Util.getString("Admin.addBindingEixists", binding.getDeployedName())); //$NON-NLS-1$
                }                    
            }
            
            // when the binding is not found it falls in "add", "overwrite" or "ignore"
            // first two cases we need to add.
            ConnectorBindingType type = (ConnectorBindingType)def.getConnectorType(binding.getComponentTypeID().getName());
            
            // Check that the connector binding passwords can be decrypted
            AdminStatus status = checkDecryption(binding, type);
            if ( status.getCode() == AdminStatus.CODE_DECRYPTION_FAILED && 
                 ! options.containsOption(AdminOptions.BINDINGS_IGNORE_DECRYPT_ERROR)) {
                throw new AdminProcessingException(status.getCode(), status.getMessage());
            }            
        }
    }

    /**
     * Check that the properties of the specified ConnectorBinding can be decrypted.
     * @param 
     * @return
     * @since 4.3
     */
    private AdminStatus checkDecryption(ConnectorBinding binding, ConnectorBindingType type) {
        
        try {
            Properties props = binding.getProperties();
            Iterator it = props.keySet().iterator();
            while (it.hasNext()) {
                String name = (String)it.next();
                if (isMaskedProperty(name, type)) {
                    decryptProperty(props.getProperty(name));
                }
            }
        } catch (CryptoException e) {
        	return new MMAdminStatus(AdminStatus.CODE_DECRYPTION_FAILED, "AdminStatus.CODE_DECRYPTION_FAILED", binding.getFullName()); //$NON-NLS-1$
        }
        return new MMAdminStatus(AdminStatus.CODE_SUCCESS, "AdminStatus.CODE_SUCCESS"); //$NON-NLS-1$
    }
       
    /**
     * Check to see if the property read is a masked/encoded property 
     * @param propName
     * @param type
     * @return
     */
    boolean isMaskedProperty(String  propName, ComponentType type) {
        if (type != null) {
            ComponentTypeDefn typeDef = type.getComponentTypeDefinition(propName);
            if (typeDef != null && typeDef.getPropertyDefinition().isMasked()) {
                return true;
            }
        }
        return false;
    }
    
    /**
     * Decrypt the given property using the Crypto libraries. 
     * @param value
     * @return decrypted property.
     */
    String decryptProperty(String value) throws CryptoException{
        if (value != null && value.length() > 0) {
           return CryptoUtil.stringDecrypt(value);
        }
        return value;
    }     
    
    /** 
     * @see org.teiid.adminapi.ConfigurationAdmin#addVDB(java.lang.String, byte[], AdminOptions)
     * @since 4.3
     */
    public VDB addVDB(String deployName, byte[] vdbFile, AdminOptions options) 
        throws AdminException {
        return addVDB(deployName, vdbFile, null, options);
    }
        
    
    /** 
     * @see org.teiid.adminapi.ConfigurationAdmin#addExtensionModule(java.lang.String, java.lang.String, byte[], java.lang.String)
     * @since 4.3
     */
    public void addExtensionModule(String type, String sourceName, byte[] source, String description) 
        throws AdminException {
        try {
            if (sourceName == null) {
                throw new AdminProcessingException(DQPEmbeddedPlugin.Util.getString("Admin.Invalid_ext_source_name")); //$NON-NLS-1$                
            }            
            if (source == null || source.length == 0) {
                throw new AdminProcessingException(DQPEmbeddedPlugin.Util.getString("Admin.Invalid_ext_source")); //$NON-NLS-1$
            }
            if (!sourceName.endsWith(".jar") && !sourceName.endsWith(".xmi")) { //$NON-NLS-1$ //$NON-NLS-2$
                throw new AdminProcessingException(DQPEmbeddedPlugin.Util.getString("Admin.Invalid_ext_module")); //$NON-NLS-1$                
            }   
            ExtensionModule previousModule = null;

            try {
                previousModule = getConfigurationService().getExtensionModule(sourceName);
            }catch(MetaMatrixComponentException e) {
                // this is OK, we did not find any thing
            }
            
            if ( previousModule == null) {
                // Now add it.
                BasicExtensionModule extModule = new BasicExtensionModule(sourceName, type, description, source);
                getConfigurationService().saveExtensionModule(extModule);
            }
            else {
                throw new AdminProcessingException(DQPEmbeddedPlugin.Util.getString("Admin.extension_module_exists", sourceName)); //$NON-NLS-1$
            }
        } catch (MetaMatrixComponentException e) {
        	throw new AdminComponentException(e);
        }
    }

    /** 
     * @see org.teiid.adminapi.ConfigurationAdmin#deleteExtensionModule(java.lang.String)
     * @since 4.3
     */
    public void deleteExtensionModule(String sourceName) 
        throws AdminException {        
        try {
            if (sourceName == null) {
                throw new AdminProcessingException(DQPEmbeddedPlugin.Util.getString("Admin.Invalid_ext_source_name")); //$NON-NLS-1$                
            }                        
            getConfigurationService().deleteExtensionModule(sourceName);
        } catch (MetaMatrixComponentException e) {
        	throw new AdminComponentException(e);
        }        
    }

    /** 
     * @see org.teiid.adminapi.ConfigurationAdmin#assignBindingToModel(java.lang.String, java.lang.String, java.lang.String, java.lang.String)
     * @since 4.3
     */
    public void assignBindingToModel(String deployedConnectorBindingName, String vdbName, String vdbVersion, String modelName) 
        throws AdminException {

        if (deployedConnectorBindingName == null || !deployedConnectorBindingName.matches(MULTIPLE_WORD_WILDCARD_REGEX)) {
            throw new AdminProcessingException(DQPEmbeddedPlugin.Util.getString("Admin.Invalid_cb_name")); //$NON-NLS-1$                
        }
        
        if (vdbName == null || vdbVersion == null || !vdbName.matches(SINGLE_WORD_REGEX)) {
            throw new AdminProcessingException(DQPEmbeddedPlugin.Util.getString("Admin.Invalid_vdb_name")); //$NON-NLS-1$
        }

        if (modelName == null || !modelName.matches(MULTIPLE_WORDS_REGEX)) {
            throw new AdminProcessingException(DQPEmbeddedPlugin.Util.getString("Admin.Invalid_model_name")); //$NON-NLS-1$
        }
        
        // find the connector binding if found in the configuration service
        // add to the vdb binding.
        try {
            ConnectorBinding binding = getDataService().getConnectorBinding(deployedConnectorBindingName);
            if (binding != null) {
                List list = new ArrayList();
                list.add(binding);            
                getConfigurationService().assignConnectorBinding(vdbName, vdbVersion, modelName, (ConnectorBinding[])list.toArray(new ConnectorBinding[list.size()]));
            }
            else {
                throw new AdminProcessingException(DQPEmbeddedPlugin.Util.getString("Admin.Vdb_or_Model_notfound")); //$NON-NLS-1$
            }
        } catch (MetaMatrixComponentException e) {
        	throw new AdminComponentException(e);
        }                        
    }
    
    public void assignBindingsToModel(String[] deployedConnectorBindingName, String vdbName, String vdbVersion, String modelName) throws AdminException {
        if (deployedConnectorBindingName == null || deployedConnectorBindingName.length == 0) {
            throw new AdminProcessingException(DQPEmbeddedPlugin.Util.getString("Admin.Invalid_cb_name")); //$NON-NLS-1$                
        }
        
        if (vdbName == null || vdbVersion == null || !vdbName.matches(SINGLE_WORD_REGEX)) {
            throw new AdminProcessingException(DQPEmbeddedPlugin.Util.getString("Admin.Invalid_vdb_name")); //$NON-NLS-1$
        }

        if (modelName == null || !modelName.matches(MULTIPLE_WORDS_REGEX)) {
            throw new AdminProcessingException(DQPEmbeddedPlugin.Util.getString("Admin.Invalid_model_name")); //$NON-NLS-1$
        }
        
        // find the connector binding if found in the configuration service
        // add to the vdb binding.
        try {
        	List list = new ArrayList();
        	for (int i = 0; i < deployedConnectorBindingName.length; i++) {
        		ConnectorBinding binding = getDataService().getConnectorBinding(deployedConnectorBindingName[i]);
                if (binding != null) {
                    list.add(binding);
                }
        	}
            
            if (!list.isEmpty()) {
                getConfigurationService().assignConnectorBinding(vdbName, vdbVersion, modelName, (ConnectorBinding[])list.toArray(new ConnectorBinding[list.size()]));
            }
            else {
                throw new AdminProcessingException(DQPEmbeddedPlugin.Util.getString("Admin.Vdb_or_Model_notfound")); //$NON-NLS-1$
            }
        } catch (MetaMatrixComponentException e) {
        	throw new AdminComponentException(e);
        }                        
    	
    }

    /** 
     * @see org.teiid.adminapi.ConfigurationAdmin#getLogConfiguration()
     * @since 4.3
     */
    public LogConfiguration getLogConfiguration() 
        throws AdminException {
            return (LogConfiguration)convertToAdminObjects(LogManager.getLogConfigurationCopy());
    }

    /** 
     * @see org.teiid.adminapi.ConfigurationAdmin#setLogConfiguration(org.teiid.adminapi.LogConfiguration)
     * @since 4.3
     */
    public void setLogConfiguration(LogConfiguration config) 
        throws AdminException {
        LogManager.setLogConfiguration((com.metamatrix.common.log.LogConfiguration)convertToNativeObjects(config));
    }

    /** 
     * @see org.teiid.adminapi.ConfigurationAdmin#exportExtensionModule(java.lang.String)
     * @since 4.3
     */
    public byte[] exportExtensionModule(String sourceName) throws AdminException {
        try {
            if (sourceName == null) {
                throw new AdminProcessingException(DQPEmbeddedPlugin.Util.getString("Admin.Invalid_ext_source_name")); //$NON-NLS-1$                
            }            
            
            ExtensionModule extModule = getConfigurationService().getExtensionModule(sourceName);            
            return extModule.getFileContents();
        } catch (MetaMatrixComponentException e) {
        	throw new AdminComponentException(e);
        }
    }

    /** 
     * @see org.teiid.adminapi.ConfigurationAdmin#exportConfiguration()
     * @since 4.3
     */
    public char[] exportConfiguration() throws AdminException {
        try {
            ConfigurationModelContainer model = getConfigurationService().getSystemConfiguration();
            return ServerConfigFileWriter.writeToCharArray(model);
        } catch (MetaMatrixComponentException e) {
        	throw new AdminComponentException(e);
        }
    }

    /** 
     * @see org.teiid.adminapi.ConfigurationAdmin#exportConnectorBinding(java.lang.String)
     * @since 4.3
     */
    public char[] exportConnectorBinding(String identifier) 
        throws AdminException {
        try {
            if (identifier == null || !identifier.matches(MULTIPLE_WORD_WILDCARD_REGEX)) {
                throw new AdminProcessingException(DQPEmbeddedPlugin.Util.getString("Admin.Invalid_cb_name")); //$NON-NLS-1$                
            }
            
            List bindingList = getDataService().getConnectorBindings();
            List matchedList = new ArrayList();
            for (Iterator i = bindingList.iterator(); i.hasNext();) {
                ConnectorBinding binding = (ConnectorBinding)i.next();
                if (matches(identifier, binding.getDeployedName())) {
                    matchedList.add(binding);
                }
            }
            
            if (!matchedList.isEmpty()) {
                ConnectorBinding[] bindings = (ConnectorBinding[])matchedList.toArray(new ConnectorBinding[matchedList.size()]);
                ConnectorBindingType[] types = new ConnectorBindingType[bindings.length];
                
                for (int i = 0; i < bindings.length; i++) {
                    types[i] = getConfigurationService().getConnectorType(bindings[i].getComponentTypeID().getName());                
                }
                return ConnectorConfigurationWriter.writeToCharArray(bindings, types);
            }
            throw new AdminProcessingException(DQPEmbeddedPlugin.Util.getString("Admin.Connector_binding_does_not_exists", identifier)); //$NON-NLS-1$
        } catch (MetaMatrixComponentException e) {
        	throw new AdminComponentException(e);
        } 
    }

    /** 
     * @see org.teiid.adminapi.ConfigurationAdmin#exportConnectorType(java.lang.String)
     * @since 4.3
     */
    public char[] exportConnectorType(String identifier) 
        throws AdminException {
        try {
            if (identifier == null || !identifier.matches(MULTIPLE_WORDS_REGEX)) {
                throw new AdminProcessingException(DQPEmbeddedPlugin.Util.getString("Admin.Invalid_ct_name")); //$NON-NLS-1$                
            }
            
            List typesList = getConfigurationService().getConnectorTypes();
            List matchedList = new ArrayList();
            for (Iterator i = typesList.iterator(); i.hasNext();) {
                ConnectorBindingType type = (ConnectorBindingType)i.next();
                if (matches(identifier, type.getName())) {
                    matchedList.add(type);
                }
            }
            
            if (!matchedList.isEmpty()) {
                ConnectorBindingType[] types = (ConnectorBindingType[])matchedList.toArray(new ConnectorBindingType[matchedList.size()]);
                return ConnectorConfigurationWriter.writeToCharArray(types);
            }
            throw new AdminProcessingException(DQPEmbeddedPlugin.Util.getString("Admin.Connector_type_does_not_exists", identifier)); //$NON-NLS-1$
        } catch (MetaMatrixComponentException e) {
        	throw new AdminComponentException(e);
        } 
    }

    /**  
     * @see org.teiid.adminapi.ConfigurationAdmin#exportVDB(java.lang.String, java.lang.String)
     * @since 4.3
     */
    public byte[] exportVDB(String name, String version) 
        throws AdminException {     
    	
        try {
            if (name == null || version == null || !name.matches(SINGLE_WORD_REGEX)) {
                throw new AdminProcessingException(DQPEmbeddedPlugin.Util.getString("Admin.Invalid_vdb_name")); //$NON-NLS-1$                
            }
            
            VDBArchive vdb = getConfigurationService().getVDB(name, version); 
            if (vdb != null) {
                return VDBArchive.writeToByteArray(vdb);
            }
            throw new AdminProcessingException(DQPEmbeddedPlugin.Util.getString("Admin.vdb_does_not_exists", name, version)); //$NON-NLS-1$
        } catch (MetaMatrixComponentException e) {
        	throw new AdminComponentException(e);
        }
    }

    /** 
     * @see org.teiid.adminapi.ConfigurationAdmin#addConnectorArchive(byte[], org.teiid.adminapi.AdminOptions)
     * @since 4.3.2
     */
    public void addConnectorArchive(byte[] contents, AdminOptions options) throws AdminException {
        
        // if the options object is null treat as if it is IGNORE as default
        if (options == null) {
            options = new AdminOptions(AdminOptions.OnConflict.IGNORE);
        }

        if (contents == null || contents.length == 0) {
            throw new AdminProcessingException(DQPEmbeddedPlugin.Util.getString("Admin.Invalid_ct_source")); //$NON-NLS-1$
        }
        
        try {            
            // Load the connector Archive from the file            
            HashSet previouslyAddedModules = new HashSet();
            ConnectorArchive archive = ConnectorConfigurationReader.loadConnectorArchive(contents);
            ConnectorBindingType[] connectorTypes = archive.getConnectorTypes();
                           
            // Loop through each type and add all of them based on the option.
            for (int typeIndex = 0; typeIndex < connectorTypes.length; typeIndex++) {
                
                // first make sure we do not already have this connector type
                String connectorName = connectorTypes[typeIndex].getName();
                ConnectorBindingType type = getConfigurationService().getConnectorType(connectorName);
                if (type == null) {
                    type = connectorTypes[typeIndex];
                    ExtensionModule[] extModules = archive.getExtensionModules(type);
                    checkDuplicateExtensionModules(extModules, options, previouslyAddedModules);
                    saveConnectorType(type);

                } else {
                
                    // if not asked to overwrite/skip writing them
                    if (options.containsOption(AdminOptions.OnConflict.EXCEPTION)) {
                        throw new AdminProcessingException(DQPEmbeddedPlugin.Util.getString("Admin.Connector_type_exists", connectorName)); //$NON-NLS-1$            
                    } else if (options.containsOption(AdminOptions.OnConflict.IGNORE)) {
                        continue;
                    } else if (options.containsOption(AdminOptions.OnConflict.OVERWRITE)){
                        deleteConnectorType(connectorName);
                        // Now that we know we need to add this to configuration; let's get on with it
                        type = connectorTypes[typeIndex];
                        ExtensionModule[] extModules = archive.getExtensionModules(type);
                        checkDuplicateExtensionModules(extModules, options, previouslyAddedModules);
                        saveConnectorType(type);
                    }                
                }
                // Now that we know we need to add this to configuration; let's get on with it
                type = connectorTypes[typeIndex];
                ExtensionModule[] extModules = archive.getExtensionModules(type);
                checkDuplicateExtensionModules(extModules, options, previouslyAddedModules);
            }
            
            // Now add the extension modules
            for (Iterator i = previouslyAddedModules.iterator(); i.hasNext();) {
                ExtensionModule extModule = (ExtensionModule)i.next();
                addExtensionModule(extModule.getModuleType(), extModule.getFullName(), extModule.getFileContents(), extModule.getDescription());
            }        
                        
        } catch (MetaMatrixComponentException e) {
            throw new AdminComponentException(e);
        }
    }

    /**
     * This method checks the passed in connector type's extension module is not already in the
     * system, if it there takes the appropriate action. Otherwise keeps tracks of all modules 
     * to add. 
     * @param type - connector type
     * @param extModules - Extension modules for the Connector Type
     * @param options - Admin Options
     * @param ignorableModules - Modules which are already added, can be ignored for adding
     */
    void checkDuplicateExtensionModules(ExtensionModule[] extModules, AdminOptions options, HashSet ignorableModules) 
        throws AdminException  {

        // Now check if the the extension modules are already there        
        for (int i = 0; i < extModules.length; i++) {
            boolean add = true;
            
            String moduleName = extModules[i].getFullName();
            ExtensionModule previousModule = null;
            
            // see if we can ignore this, because we may have just added this during import of
            // another connector type through this archive
            if (ignorableModules.contains(extModules[i])) {
                continue;
            }
            
            // we have not already added this this time around, now check if this available 
            // from configuration service
            try {
                previousModule = getConfigurationService().getExtensionModule(moduleName);
            }catch(MetaMatrixComponentException e) {
                // this is OK, we did not find any thing
            }
            
            // if we found it take appropriate action.
            if(previousModule != null && options.containsOption(AdminOptions.OnConflict.EXCEPTION)) {
                throw new AdminProcessingException(DQPEmbeddedPlugin.Util.getString("Admin.extension_module_exists", previousModule.getFullName())); //$NON-NLS-1$
            }
            else if (previousModule != null && options.containsOption(AdminOptions.OnConflict.IGNORE)) {
                add = false;
            }
            else if (previousModule != null && options.containsOption(AdminOptions.OnConflict.OVERWRITE)) {
                // since we are overwrite, first delete and then add, there is no safe way to overwrite
                deleteExtensionModule(previousModule.getFullName());
            }
                            
            // Now keep track what extension modules to add; also to ignore in future
            // adds
            if (add) {                
                ignorableModules.add(extModules[i]);
            }
        }                    
    }
    
    /** 
     * @see org.teiid.adminapi.ConfigurationAdmin#exportConnectorArchive(java.lang.String)
     * @since 4.3
     */
    public byte[] exportConnectorArchive(String identifier) throws AdminException {
        try {
            if (identifier == null || !identifier.matches(MULTIPLE_WORD_WILDCARD_REGEX)) {
                throw new AdminProcessingException(DQPEmbeddedPlugin.Util.getString("Admin.Invalid_ct_name")); //$NON-NLS-1$                
            }
            
            // first build the connector archive object
            BasicConnectorArchive archive = new BasicConnectorArchive();
            List connectorTypes = getConfigurationService().getConnectorTypes();
            
            for (Iterator i = connectorTypes.iterator(); i.hasNext();) {
                ConnectorBindingType type = (ConnectorBindingType)i.next();
                
                // If the types name matches with the pattern sent in add to archive
                if (type != null && matches(identifier, type.getName())) {
                    
                    // Add connector type first
                    archive.addConnectorType(type);
    
                    // Get the extension modules required for the type
                    String[] extModules = type.getExtensionModules();
                    for (int m = 0; m < extModules.length; m++) {
                        // Get the extension module from the configuration and add to the archive
                        ExtensionModule extModule = getConfigurationService().getExtensionModule(extModules[m]);
                        if (extModule != null) {
                            archive.addExtensionModule(type, extModule);
                        }
                        else {
                            throw new AdminProcessingException(DQPEmbeddedPlugin.Util.getString("DataService.ext_module_not_found", extModules[m])); //$NON-NLS-1$                            
                        }                        
                    }                    
                }
            }
                
            // if no types found to the identifier pattern, then throw an exception
            if (archive.getConnectorTypes().length == 0) {
                throw new AdminProcessingException(DQPEmbeddedPlugin.Util.getString("Admin.Connector_type_does_not_exists", identifier)); //$NON-NLS-1$    
            }
                            
            // now convert the object into file form
            return ConnectorConfigurationWriter.writeToByteArray(archive);
           
        } catch (MetaMatrixComponentException e) {
        	throw new AdminComponentException(e);
        } 
    }
    
    private void saveConnectorType(ConnectorBindingType type) throws MetaMatrixComponentException {
        getConfigurationService().saveConnectorType(type);
    }
    
    
    @Override
	public void addUDF(byte[] modelFileContents, String classpath)
			throws AdminException {
		if (modelFileContents == null || modelFileContents.length == 0) {
			throw new AdminProcessingException(DQPEmbeddedPlugin.Util.getString("Admin.Invalid_UDF_contents")); //$NON-NLS-1$               
		}

		try {
		
			getConfigurationService().unloadUDF();
			
			// delete any extension module by the same name first
			try {
				deleteExtensionModule(ConfigurationService.USER_DEFINED_FUNCTION_MODEL);
			} catch (AdminException e) {
				// if not found then it is OK to fail
			}
			
			// add the function definitions as extension modules
			addExtensionModule(ExtensionModule.FUNCTION_DEFINITION_TYPE,ConfigurationService.USER_DEFINED_FUNCTION_MODEL,modelFileContents, "User Defined Functions File"); //$NON-NLS-1$
			
	        String commonpath = getConfigurationService().getSystemProperties().getProperty(DQPEmbeddedProperties.COMMON_EXTENSION_CLASPATH, ""); //$NON-NLS-1$
	        
	        StringBuilder sb = new StringBuilder();
	        if (classpath != null && classpath.length() > 0 ) {
	        	StringTokenizer st = new StringTokenizer(classpath, ";"); //$NON-NLS-1$
	        	while (st.hasMoreTokens()) {
	        		String partpath = st.nextToken();
	        		if (commonpath.indexOf(partpath) == -1) {
	        			sb.append(partpath).append(";"); //$NON-NLS-1$
	        		}
	        	}
	        }
	        setSystemProperty(DQPEmbeddedProperties.COMMON_EXTENSION_CLASPATH, sb.toString()+commonpath);
			
			
			// then update the properties
			Properties p = new Properties();
			p.setProperty(DQPEmbeddedProperties.COMMON_EXTENSION_CLASPATH, classpath);
			getConfigurationService().updateSystemProperties(p);
			// reload the new UDF
			getConfigurationService().loadUDF();
		} catch (MetaMatrixComponentException e) {
			throw new AdminComponentException(e);
		}
	}

	@Override
	public void deleteUDF() throws AdminException {
		try {
			getConfigurationService().unloadUDF();
			deleteExtensionModule(ConfigurationService.USER_DEFINED_FUNCTION_MODEL); 
		} catch (MetaMatrixComponentException e) {
			throw new AdminComponentException(e);
		}
	}
	
	@Override
	public void extensionModuleModified(String name) throws AdminException {
		try {
			getConfigurationService().clearClassLoaderCache();
			getConfigurationService().loadUDF();
		} catch (MetaMatrixComponentException e) {
			throw new AdminComponentException(e);
		}		
	}    
}
