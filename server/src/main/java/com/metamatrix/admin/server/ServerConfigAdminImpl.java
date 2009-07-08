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

package com.metamatrix.admin.server;

import java.io.BufferedOutputStream;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.StringTokenizer;

import org.teiid.adminapi.AdminComponentException;
import org.teiid.adminapi.AdminException;
import org.teiid.adminapi.AdminObject;
import org.teiid.adminapi.AdminOptions;
import org.teiid.adminapi.AdminProcessingException;
import org.teiid.adminapi.AdminStatus;
import org.teiid.adminapi.LogConfiguration;
import org.teiid.adminapi.ProcessObject;
import org.teiid.adminapi.ScriptsContainer;
import org.teiid.adminapi.SystemObject;
import org.teiid.adminapi.VDB;
import org.teiid.transport.SSLConfiguration;

import com.metamatrix.admin.AdminPlugin;
import com.metamatrix.admin.api.exception.security.InvalidSessionException;
import com.metamatrix.admin.api.server.ServerConfigAdmin;
import com.metamatrix.admin.objects.MMAdminObject;
import com.metamatrix.admin.objects.MMAdminStatus;
import com.metamatrix.admin.objects.MMConnectorBinding;
import com.metamatrix.admin.objects.MMLogConfiguration;
import com.metamatrix.admin.objects.MMScriptsContainer;
import com.metamatrix.admin.objects.MMService;
import com.metamatrix.api.exception.MetaMatrixComponentException;
import com.metamatrix.api.exception.MetaMatrixProcessingException;
import com.metamatrix.api.exception.security.AuthorizationException;
import com.metamatrix.common.actions.ModificationActionQueue;
import com.metamatrix.common.actions.ModificationException;
import com.metamatrix.common.config.CurrentConfiguration;
import com.metamatrix.common.config.api.AuthenticationProvider;
import com.metamatrix.common.config.api.ComponentDefnID;
import com.metamatrix.common.config.api.ComponentType;
import com.metamatrix.common.config.api.ComponentTypeID;
import com.metamatrix.common.config.api.Configuration;
import com.metamatrix.common.config.api.ConfigurationModelContainer;
import com.metamatrix.common.config.api.ConfigurationObjectEditor;
import com.metamatrix.common.config.api.ConnectorArchive;
import com.metamatrix.common.config.api.ConnectorBinding;
import com.metamatrix.common.config.api.ConnectorBindingType;
import com.metamatrix.common.config.api.DeployedComponent;
import com.metamatrix.common.config.api.ExtensionModule;
import com.metamatrix.common.config.api.Host;
import com.metamatrix.common.config.api.HostID;
import com.metamatrix.common.config.api.HostType;
import com.metamatrix.common.config.api.ServiceComponentDefn;
import com.metamatrix.common.config.api.ServiceComponentDefnID;
import com.metamatrix.common.config.api.VMComponentDefn;
import com.metamatrix.common.config.api.VMComponentDefnID;
import com.metamatrix.common.config.api.VMComponentDefnType;
import com.metamatrix.common.config.api.exceptions.ConfigurationException;
import com.metamatrix.common.config.api.exceptions.InvalidConfigurationException;
import com.metamatrix.common.config.model.BasicConfigurationObjectEditor;
import com.metamatrix.common.config.model.BasicConnectorArchive;
import com.metamatrix.common.config.model.BasicExtensionModule;
import com.metamatrix.common.config.model.ConfigurationModelContainerAdapter;
import com.metamatrix.common.config.model.ConfigurationModelContainerImpl;
import com.metamatrix.common.config.util.ConfigObjectsNotResolvableException;
import com.metamatrix.common.config.util.ConfigurationPropertyNames;
import com.metamatrix.common.config.util.InvalidConfigurationElementException;
import com.metamatrix.common.config.xml.XMLConfigurationImportExportUtility;
import com.metamatrix.common.extensionmodule.ExtensionModuleDescriptor;
import com.metamatrix.common.extensionmodule.exception.DuplicateExtensionModuleException;
import com.metamatrix.common.extensionmodule.exception.ExtensionModuleNotFoundException;
import com.metamatrix.common.extensionmodule.exception.InvalidExtensionModuleTypeException;
import com.metamatrix.common.log.LogManager;
import com.metamatrix.common.util.LogContextsUtil;
import com.metamatrix.common.util.PropertiesUtils;
import com.metamatrix.common.util.crypto.CryptoException;
import com.metamatrix.common.util.crypto.CryptoUtil;
import com.metamatrix.common.util.crypto.Cryptor;
import com.metamatrix.common.util.crypto.cipher.SymmetricCryptor;
import com.metamatrix.common.vdb.api.ModelInfo;
import com.metamatrix.common.vdb.api.VDBArchive;
import com.metamatrix.common.vdb.api.VDBDefn;
import com.metamatrix.core.util.ObjectConverterUtil;
import com.metamatrix.metadata.runtime.RuntimeMetadataCatalog;
import com.metamatrix.metadata.runtime.api.Model;
import com.metamatrix.metadata.runtime.api.VirtualDatabase;
import com.metamatrix.metadata.runtime.api.VirtualDatabaseException;
import com.metamatrix.metadata.runtime.api.VirtualDatabaseID;
import com.metamatrix.metadata.runtime.vdb.defn.VDBCreation;
import com.metamatrix.metadata.runtime.vdb.defn.VDBDefnFactory;
import com.metamatrix.platform.config.spi.xml.XMLConfigurationMgr;
import com.metamatrix.platform.registry.ClusteredRegistryState;
import com.metamatrix.platform.service.api.exception.ServiceException;
import com.metamatrix.server.admin.apiimpl.MaterializationLoadScriptsImpl;
import com.metamatrix.server.admin.apiimpl.RuntimeMetadataHelper;
import com.metamatrix.server.util.ServerPropertyNames;

/**
 * @since 4.3
 */
public class ServerConfigAdminImpl extends AbstractAdminImpl implements
                                                            ServerConfigAdmin {

    /*
     * Connection Constants
     */
    private static final String CONNECTION_PROPERTY_DRIVER = "Driver"; //$NON-NLS-1$
    private static final String CONNECTION_PROPERTY_PASSWORD = "Password"; //$NON-NLS-1$
    private static final String CONNECTION_PROPERTY_USER = "User"; //$NON-NLS-1$
    private static final String CONNECTION_PROPERTY_URL = "URL"; //$NON-NLS-1$

 //   private static String METAMATRIXPROCESS_PSC = ProductServiceConfigID.STANDARD_CONNECTOR_PSC; 
 //   private static String PLATFORM_STANDARD_PSC = ProductServiceConfigID.STANDARD_PLATFORM_PSC; 
 //   private static String QUERY_ENGINE_PSC = ProductServiceConfigID.METAMATRIX_SERVER_QUERY_ENGINE_PSC;
    private static String FUNCTION_DEFINITIONS_MODEL = "FunctionDefinitions.xmi"; //$NON-NLS-1$

    
    public ServerConfigAdminImpl(ServerAdminImpl parent, ClusteredRegistryState registry) {
        super(parent, registry);
    }

    /**
     * @throws AdminException
     * @see com.metamatrix.admin.api.server.ServerConfigAdmin#addConnectorBinding(java.lang.String, java.lang.String,
     *      java.util.Properties, AdminOptions)
     * @since 4.3
     */
    public org.teiid.adminapi.ConnectorBinding addConnectorBinding(String connectorBindingName,
                                    String connectorTypeIdentifier,
                                    Properties properties,
                                    AdminOptions options) throws AdminException {

        org.teiid.adminapi.ConnectorBinding newBinding = null;
        
        if (connectorBindingName == null) {
            throw new AdminProcessingException(AdminServerPlugin.Util.getString("ServerConfigAdminImpl.Name_can_not_be_null")); //$NON-NLS-1$
        }
        if (connectorTypeIdentifier == null) {
            throw new AdminProcessingException(AdminServerPlugin.Util.getString("ServerConfigAdminImpl.Connector_Type_can_not_be_null")); //$NON-NLS-1$
        }
        if (properties == null) {
            throw new AdminProcessingException(AdminServerPlugin.Util.getString("ServerConfigAdminImpl.Properties_can_not_be_null")); //$NON-NLS-1$
        }
        
        // Check if binding allready exists and look at admin options
        Collection existingBindings = 
            parent.getConnectorBindings(AdminObject.WILDCARD + AdminObject.DELIMITER + connectorBindingName);
        Collection newBindingNames = new ArrayList(1);
        newBindingNames.add(connectorBindingName);
        Collection updateBindingNames = getBindingNamesToUpdate(existingBindings, newBindingNames, options);

        if ( updateBindingNames.size() > 0 && updateBindingNames.iterator().next().equals(connectorBindingName) ) {
            // Add the new binding
            // Check that binding password is decryptable
            AdminStatus status = checkDecryption(properties, connectorBindingName, connectorTypeIdentifier);
                if (status.getCode() == AdminStatus.CODE_DECRYPTION_FAILED
                    && !options.containsOption(AdminOptions.BINDINGS_IGNORE_DECRYPT_ERROR)) {
                throw new AdminProcessingException(status.getCode(), status.getMessage());
            }
            
            ConnectorBinding binding = null;
            try {
                binding = getConfigurationServiceProxy().createConnectorBinding(connectorBindingName,
                                                                                connectorTypeIdentifier,
                                                                                "ALL",  // deploy to all vms
                                                                                getUserName(),
                                                                                properties);
                if (binding == null) {
                    this.throwProcessingException("ServerConfigAdminImpl.Connector_Binding_was_null", new Object[] {connectorBindingName}); //$NON-NLS-1$
                }
                            
            } catch (ConfigurationException e) {
                throw new AdminComponentException(e);
            } catch (ServiceException e) {
            	throw new AdminComponentException(e);
            }
        
            Collection newBindings = 
                parent.getConnectorBindings(AdminObject.WILDCARD + AdminObject.DELIMITER + connectorBindingName);
            newBinding = (org.teiid.adminapi.ConnectorBinding)newBindings.iterator().next();
        } else {
            // We didn't add the new connector binding. Return the existing.
            if (existingBindings != null && existingBindings.size() > 0) {
                // Only expecting one existing binding
                newBinding = (org.teiid.adminapi.ConnectorBinding)existingBindings.iterator().next();
            }
        }
        return newBinding;
    }

    /**
     * @param connectorBindingName, is nullable, indicates the name to assign to the importing {@link ConnectorBinding}.  
     * 	If it is null, then the name defined in the xmlFile will be used.
     * @param xmlFile is an xml formatted file that defines the connector binding 
     * @see com.metamatrix.admin.api.server.ServerConfigAdmin#addConnectorBinding(java.lang.String, char[], AdminOptions)
     * @since 4.3
     */
    public org.teiid.adminapi.ConnectorBinding addConnectorBinding(String connectorBindingName,
                                    char[] xmlFile, AdminOptions options) throws AdminException {
        org.teiid.adminapi.ConnectorBinding newBinding = null;

        if (xmlFile == null) {
            throw new AdminProcessingException(AdminServerPlugin.Util.getString("ServerConfigAdminImpl.CDK_File_Name_can_not_be_null")); //$NON-NLS-1$
        }
        
        // first, read the xmlfile to determine the defined connnector binding name
        // because majority of the time, the file will determine the name
        InputStream is = ObjectConverterUtil.convertToInputStream(xmlFile);
        
        XMLConfigurationImportExportUtility ciu = new XMLConfigurationImportExportUtility();
        ConnectorBinding binding = null;
        try {
            binding = ciu.importConnectorBinding(is, new BasicConfigurationObjectEditor(false), connectorBindingName);
        } catch (ConfigObjectsNotResolvableException e) {
        	throw new AdminComponentException(e);
        } catch (InvalidConfigurationElementException e) {
        	throw new AdminComponentException(e);
        } catch (IOException e) {
        	throw new AdminComponentException(e);
        } finally {
        	if (is != null) {
        		try {
					is.close();
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
        	}
        }

        // reassign the name in cases where null was passed in
        connectorBindingName = binding.getName();
        
        return this.addConnectorBinding(connectorBindingName, binding.getComponentTypeID().getFullName(), binding.getProperties(), options);

    }
    
    /**
     * @param name, is nullable, indicates the name to assign to the connector type
     * 		If name is null, then the name defined in the cdkFile will be used.
     * @throws MetaMatrixComponentException
     * @throws MetaMatrixProcessingException
     * @see com.metamatrix.admin.api.server.ServerConfigAdmin#addConnectorType(java.lang.String, char[])
     * @since 4.3
     */
    public void addConnectorType(String name,
                                 char[] cdkFile) throws AdminException {
        ComponentType connectorType = null;

        if (cdkFile == null) {
            throw new AdminProcessingException(AdminServerPlugin.Util.getString("ServerConfigAdminImpl.CDK_File_Name_can_not_be_null")); //$NON-NLS-1$
        }

        InputStream is = null;
        if (name == null || name.trim().length() == 0) {
            is = ObjectConverterUtil.convertToInputStream(cdkFile);
            
            XMLConfigurationImportExportUtility ciu = new XMLConfigurationImportExportUtility();
            try {
            	connectorType = ciu.importComponentType(is, new BasicConfigurationObjectEditor(false), name);
                // reassign name in case it was passed in
                name = connectorType.getFullName();
            } catch (InvalidConfigurationElementException e) {
            	throw new AdminComponentException(e);
            } catch (IOException e) {
            	throw new AdminComponentException(e);
            } finally {
            	if (is != null) {
            		try {
    					is.close();
    				} catch (IOException e) {
    					// TODO Auto-generated catch block
    					e.printStackTrace();
    				}
            	}
            }
        }
            

       try {           
            is = ObjectConverterUtil.convertToInputStream(cdkFile);
        
            connectorType = getConfigurationServiceProxy().importConnectorType(is, name, getUserName());
            if (connectorType == null) {
                throwProcessingException("ServerConfigAdminImpl.Connector_Type_was_null", new Object[] {name}); //$NON-NLS-1$
            }
        } catch (ConfigurationException e) {
        	throw new AdminComponentException(e);
        } catch (ServiceException e) {
        	throw new AdminComponentException(e);
        } finally {
            	if (is != null) {
            		try {
    					is.close();
    				} catch (IOException e) {
    					// TODO Auto-generated catch block
    					e.printStackTrace();
    				}
            	}
            }

    }

    /** 
     * @see org.teiid.adminapi.ConfigurationAdmin#addConnectorArchive(byte[], org.teiid.adminapi.AdminOptions)
     * @since 4.3
     */
    public void addConnectorArchive(byte[] contents, AdminOptions options) throws AdminException {
        if (options == null) {
            options = new AdminOptions(AdminOptions.OnConflict.IGNORE);
        }
                
        if (contents == null || contents.length == 0) {
            throw new AdminProcessingException(AdminServerPlugin.Util.getString("ServerConfigAdminImpl.CDK_File_Name_can_not_be_null")); //$NON-NLS-1$
        }
        
        XMLConfigurationImportExportUtility util = new XMLConfigurationImportExportUtility();
        InputStream in = ObjectConverterUtil.convertToInputStream(contents);
                
        try {
			// Load the connector Archive from the file            
			HashSet previouslyAddedModules = new HashSet();
			HashSet typesToAdd = new HashSet();
			ConnectorArchive archive = util.importConnectorArchive(in, new BasicConfigurationObjectEditor());
			ConnectorBindingType[] connectorTypes = archive.getConnectorTypes();
			               
			// Loop through each type and add all of them based on the option.
			for (int typeIndex = 0; typeIndex < connectorTypes.length; typeIndex++) {
			    
			    // first make sure we do not already have this connector type
			    String connectorName = connectorTypes[typeIndex].getName();
			    ConnectorBindingType type = (ConnectorBindingType)this.getComponentType(connectorName);
			                    
			    // if exists and option is to throw exception then throw exception
			    if (type != null && options.containsOption(AdminOptions.OnConflict.EXCEPTION)) {
			        throwProcessingException("ServerConfigAdminImpl.Connector_Type_already_exists", new Object[] {connectorName}); //$NON-NLS-1$
			    }               
			    else if (type != null && options.containsOption(AdminOptions.OnConflict.IGNORE)) {
			        continue;
			    }
			    else if (type != null && options.containsOption(AdminOptions.OnConflict.OVERWRITE)){
			        deleteConnectorType(connectorName);
			    }
			    
			    // Now that we know we need to add this to configuration; let's get on with it
			    type = connectorTypes[typeIndex];
			    ExtensionModule[] extModules = archive.getExtensionModules(type);
			    checkAddingConnectorType(type, extModules, options, previouslyAddedModules);
			    typesToAdd.add(type);
			}
			
			// Now that we over the admin options crap, now go ahead and add the types
			// and modules.
			for (Iterator i = typesToAdd.iterator(); i.hasNext();) {
			    ConnectorBindingType type = (ConnectorBindingType)i.next();
			    // Now add/overwrite the connector type to the system
			    // first add the connector type, here it is little odd that we export and
			    // import, however the configurationProxy is written based on streams not
			    // on the objects that is way.             
			    ByteArrayOutputStream baos = new ByteArrayOutputStream(10*1024);
			    util.exportComponentType(baos, type, getPropertiesForExporting());                                    
			    addConnectorType(type.getName(), ObjectConverterUtil.bytesToChar(baos.toByteArray(), null));
			    baos.close();
			}
			
			// Now add the extension modules
			for (Iterator i = previouslyAddedModules.iterator(); i.hasNext();) {
			    ExtensionModule extModule = (ExtensionModule)i.next();
			    addExtensionModule(extModule.getModuleType(), extModule.getFullName(), extModule.getFileContents(), extModule.getDescription());
			}
		} catch (InvalidConfigurationElementException e) {
			throw new AdminComponentException(e);
		} catch (MetaMatrixComponentException e) {
			throw new AdminComponentException(e);
		} catch (IOException e) {
			throw new AdminComponentException(e);
		} catch(MetaMatrixProcessingException e){
			throw new AdminProcessingException(e);
		}	finally {
            try{in.close();}catch(IOException e) {}
        }                            
    }    
	
   /** 
     * @see org.teiid.adminapi.ConfigurationAdmin#addAuthroizationProvider(String, String, Properties)
     * @param domainname is the name to be assigned to the newly created {@link AuthenticationProvider}
     * @param provdertypename is the type of provider to create.  
     * @param properties are the settings specified by the providertype to be used
     * @since 5.6
     */
    public void addAuthorizationProvider(String domainprovidername, String providertypename, Properties properties) throws AdminException {

        if (domainprovidername == null) {
            throw new AdminProcessingException(AdminServerPlugin.Util.getString("ServerConfigAdminImpl.Provider_name_can_not_be_null")); //$NON-NLS-1$
        }
        
        if (providertypename == null) {
            throw new AdminProcessingException(AdminServerPlugin.Util.getString("ServerConfigAdminImpl.ProviderType_name_can_not_be_null")); //$NON-NLS-1$
        }        

            
        try {
			ConfigurationObjectEditor coe = getConfigurationServiceProxy().createEditor();
			
			ConfigurationModelContainer cmc = getConfigurationServiceProxy().getConfigurationModel(Configuration.NEXT_STARTUP);
			
			if (cmc.getConfiguration().getAuthenticationProvider(domainprovidername) != null)  {
			    throw new AdminProcessingException(AdminServerPlugin.Util.getString("ServerConfigAdminImpl.Provider_already_exist")); //$NON-NLS-1$
			    
			}
			ComponentType providertype = cmc.getComponentType(providertypename);
			
			if (providertype == null)  {
			    throw new AdminProcessingException(AdminServerPlugin.Util.getString("ServerConfigAdminImpl.ProviderType_does_not_exist")); //$NON-NLS-1$
			    
			}            
			AuthenticationProvider provider = coe.createAuthenticationProviderComponent(Configuration.NEXT_STARTUP_ID,
			                                                                (ComponentTypeID)providertype.getID(),
			                                                                domainprovidername);
			
			 
			 Properties props = providertype.getDefaultPropertyValues();
			 props.putAll(properties);
			 
			 provider = (AuthenticationProvider) coe.modifyProperties(provider, props, ConfigurationObjectEditor.SET);


			 getConfigurationServiceProxy().executeTransaction(coe.getDestination().popActions(), getUserName());
		} catch (InvalidConfigurationException e) {
			throw new AdminComponentException(e);
		} catch (ConfigurationException e) {
			throw new AdminComponentException(e);
		}
            
    }
    	
    
    /**
     * This method checks the passed in connector type's extension module is not already in the
     * system, if it there takes the appropriate action. Otherwise keeps tracks of all modules 
     * to add. 
     * @param type - connector type
     * @param extModules - Extension modules for the Coneector Type
     * @param options - Admin Options
     * @param ignorableModules - Modules which are already added, can be ignored for adding
     */
    void checkAddingConnectorType(ConnectorBindingType type, ExtensionModule[] extModules, AdminOptions options, HashSet ignorableModules) 
        throws MetaMatrixComponentException, MetaMatrixProcessingException, AdminException  {

        // Now check if the the extension modules are already there        
        for (int i = 0; i < extModules.length; i++) {
            boolean add = true;
            
            String moduleName = extModules[i].getFullName();
            
            // see if we can ignore this, because we may have just added this during import of
            // another connector type through this archive
            if (ignorableModules.contains(extModules[i])) {
                continue;
            }
            
            // we have not already added this this time around, now check if this available 
            // from configuration service
            if (getExtensionSourceManager().isSourceInUse(moduleName)) {
                if (options.containsOption(AdminOptions.OnConflict.EXCEPTION)) {
                    throwProcessingException("ServerConfigAdminImpl.Extension_module_already_exists", new Object[] {moduleName}); //$NON-NLS-1$
                }
                else if (options.containsOption(AdminOptions.OnConflict.IGNORE)) {
                    add = false;
                }
                else if (options.containsOption(AdminOptions.OnConflict.OVERWRITE)) {
                    // since we are overwrite, first delete and then add, there is no safe way to overwrite
                    deleteExtensionModule(moduleName);
                }                
            }
                            
            // Now keep track what extension modules to add; also to ignore in future
            // adds
            if (add) {                
                ignorableModules.add(extModules[i]);
            }
        }                    
    }    
    
    /**
     * @see com.metamatrix.admin.api.server.ServerConfigAdmin#addExtensionModule(java.lang.String, java.lang.String, byte[],
     *      java.lang.String, boolean)
     * @since 4.3
     */
    public void addExtensionModule(String type,
                                   String sourceName,
                                   byte[] source,
                                   String description) throws AdminException {
        Collection foundModules = parent.getExtensionModules(sourceName);
        if (foundModules.size() > 0) {
            throwProcessingException("ServerConfigAdminImpl.Extension_Module_duplicate", new Object[] {sourceName}); //$NON-NLS-1$
        }
        
        
        try {
			ExtensionModuleDescriptor desc = getExtensionSourceManager().addSource(getUserName(),
			                                                                       type,
			                                                                       sourceName,
			                                                                       source,
			                                                                       description,
			                                                                       true);
			if (desc == null) {
			    throwProcessingException("ServerConfigAdminImpl.Extension_Module_Descriptor_was_null", new Object[] {sourceName}); //$NON-NLS-1$
			}
		} catch (DuplicateExtensionModuleException e) {
			throw new AdminProcessingException(e);
		} catch (InvalidExtensionModuleTypeException e) {
			throw new AdminProcessingException(e);
		} catch (MetaMatrixComponentException e) {
			throw new AdminComponentException(e);
		}
    }

    /**
     * @see com.metamatrix.admin.api.server.ServerConfigAdmin#deleteExtensionModule(java.lang.String)
     * @since 4.3
     */
    public void deleteExtensionModule(String sourceName) throws AdminException {
    	try {
			getExtensionSourceManager().removeSource(getUserName(), sourceName);
		} catch (ExtensionModuleNotFoundException e) {
			throw new AdminProcessingException(e);
		} catch (MetaMatrixComponentException e) {
			throw new AdminComponentException(e);
		}
    }

    /**
     * @throws MetaMatrixComponentException
     * @throws MetaMatrixProcessingException
     * @see com.metamatrix.admin.api.server.ServerConfigAdmin#addHost(java.lang.String, java.util.Properties)
     * @since 4.3
     */
    public void addHost(String hostIdentifer,
                        Properties properties) throws AdminException {

        com.metamatrix.common.config.api.Host host = null;

        String hostName = getName(hostIdentifer);

        if (hostName == null) {
            throw new AdminProcessingException(AdminServerPlugin.Util.getString("ServerConfigAdminImpl.Host_name_can_not_be_null")); //$NON-NLS-1$
        }
        if (properties == null) {
            throw new AdminProcessingException(AdminServerPlugin.Util.getString("ServerConfigAdminImpl.Properties_can_not_be_null")); //$NON-NLS-1$
        }
        try {
			host = getConfigurationServiceProxy().addHost(hostName, getUserName(), properties);

			if (host == null) {
			    throwProcessingException("ServerConfigAdminImpl.Host_was_null", new Object[] {hostName}); //$NON-NLS-1$

			}
		} catch (ConfigurationException e) {
			throw new AdminComponentException(e);
		} catch (ServiceException e) {
			throw new AdminComponentException(e);
		}
    }

    /**
     * @throws MetaMatrixComponentException
     * @see com.metamatrix.admin.api.server.ServerConfigAdmin#addProcess(java.lang.String, java.util.Properties)
     * @since 4.3
     */
    public void addProcess(String processIdentifier,
                           Properties properties) throws AdminException {

        String processName = getName(processIdentifier);
        if (processName == null) {
            throw new AdminProcessingException(AdminServerPlugin.Util.getString("ServerConfigAdminImpl.Name_can_not_be_null")); //$NON-NLS-1$
        }
        String hostName = getParent(processIdentifier);
        
        if (hostName == null) {
            throw new AdminProcessingException(AdminServerPlugin.Util.getString("ServerConfigAdminImpl.Host_name_can_not_be_null")); //$NON-NLS-1$
        }
        Host theHost = this.getHostByName(hostName);

        if (properties == null) {
            throw new AdminProcessingException(AdminServerPlugin.Util.getString("ServerConfigAdminImpl.Properties_can_not_be_null")); //$NON-NLS-1$
        }


        try {
			com.metamatrix.common.config.api.VMComponentDefn processDefn = null;

			processDefn = getConfigurationServiceProxy().addProcess(processName, hostName, getUserName(), properties);

			if (processDefn != null) {
			    Collection svcs = this.getConfigurationModel().getConfiguration().getServiceComponentDefns();
			    if (svcs != null && ! svcs.isEmpty()) {
			        ServiceComponentDefn svc = null;
			        for (Iterator it=svcs.iterator(); it.hasNext();) {
			             svc =(ServiceComponentDefn) it.next();
			         // deploy essential services
			             if (svc.isEssential()) {
			                getConfigurationServiceProxy().deployService((VMComponentDefnID) processDefn.getID(), svc.getName(), getUserName());                          
			            } 
			        }
			    }

			} else {
			    final Object[] params = new Object[] {
			        processIdentifier, hostName
			    };
			    throwProcessingException("ServerConfigAdminImpl.Process_was_null", params); //$NON-NLS-1$
			}
		} catch (ConfigurationException e) {
			throw new AdminComponentException(e);
		} catch (ServiceException e) {
			throw new AdminComponentException(e);
		}
    }

    /**
     * @param name, is nullable, indicates the name to assign to the vdb
     * 		If name is null, then use the name defined in the vdbFile.
     * @see com.metamatrix.admin.api.server.ServerConfigAdmin#addVDB(java.lang.String, java.lang.String, byte[], char[])
     * @since 4.3
     */
    public VDB addVDB(String name, byte[] vdbFile, AdminOptions options) throws AdminException {
        VDBArchive vdb = null;
        try {
			vdb = new VDBArchive(new ByteArrayInputStream(vdbFile));
			if (name != null) {
				vdb.setName(name);
			}
		} catch (IOException e) {
			throw new AdminComponentException(e);
		}
        return addVDB(vdb, options);
    }

    /** 
     * @param options
     * @param vdbDefn
     * @return
     * @throws AdminComponentException
     * @since 4.3
     */
    private VDB addVDB(VDBArchive vdb, AdminOptions options) throws AdminException {
        
    	VDBDefn def = vdb.getConfigurationDef();
        List<ConnectorBinding> newBindings = new ArrayList(def.getConnectorBindings().values());

        AdminStatus status = checkDecryption(newBindings);
        if (status.getCode() == AdminStatus.CODE_DECRYPTION_FAILED
            && !options.containsOption(AdminOptions.BINDINGS_IGNORE_DECRYPT_ERROR)) {
            throw new AdminProcessingException(status.getCode(), status.getMessage());
        }

        Collection newBindingNames = getBindingNames(newBindings);
        Collection existingBindings = parent.getConnectorBindingsInVDB(vdb.getName());

        // AdminOptions checking here.
        getBindingNamesToUpdate(existingBindings, newBindingNames, options);
        
        // Update connector bindings only if OVERWRITE option given.  VDBDefnImport will take
        // care of adding any bindings that are new to the VDB.
        boolean updateBindings = (options.containsOption(AdminOptions.OnConflict.OVERWRITE));

        VirtualDatabase newVDB = null;
    	try {
    	
			newVDB = importVDBDefn(vdb, getUserName(), updateBindings, Collections.EMPTY_LIST);
		} catch (Exception e) {
			// TODO: remove the generalization of exception
			throw new AdminComponentException(e);
		}
        if (newVDB == null) {
            throwProcessingException("ServerConfigAdminImpl.VDB_created_was_null", new Object[] {def.getName()}); //$NON-NLS-1$
        }
        
        // if there are data roles, then import them
        if (vdb.getDataRoles() != null) {
            importDataRoles(vdb.getName(), vdb.getVersion(), vdb.getDataRoles(), options);
        }        
        
        return convertToAdminVDB(newVDB);
    }
    
    private VirtualDatabase importVDBDefn(VDBArchive vdb, String principal, boolean updateExistingBinding, List vms) throws Exception {
        VDBCreation vdbc = new VDBCreation();
        vdbc.setUpdateBindingProperties(updateExistingBinding);
        vdbc.setVMsToDeployBindings(vms);
        return vdbc.loadVDBDefn(vdb,principal);
    } 
    
    /**
     * @see com.metamatrix.admin.api.server.ServerConfigAdmin#generateMaterializationScripts(java.lang.String, java.lang.String, java.lang.String, java.lang.String, java.lang.String, java.lang.String)
     * @since 4.3
     */
    public ScriptsContainer generateMaterializationScripts(String vdbName, String vdbVersion, 
                                                           String metamatrixUserName, String metamatrixUserPwd, 
                                                           String materializationUserName, String materializationUserPwd) 
    throws AdminException {

        // Get ModelInfo and connector binding for the materialization model
        VDBArchive vdbArchive= null;
        ModelInfo materializationModel = null;
        try {
        	vdbArchive = VDBDefnFactory.createVDBArchive(vdbName, vdbVersion);
        	VDBDefn def = vdbArchive.getConfigurationDef();
        	Collection<ModelInfo> models = def.getModels();
        	
        	for(ModelInfo model:models) {
        		if (model.isMaterialization()) {
        			materializationModel = model;
        			break;
        		}
        	}
        } catch(Exception e){
        	// TODO: generalization of the exception should be removed
        	throw new AdminComponentException(e);
        } finally {
        	if (vdbArchive != null) {
        		vdbArchive.close();
        	}
        }
        
        List bindings = materializationModel.getConnectorBindingNames();
        
        ConnectorBinding materializationConnector = null;
        String materializationConnectorName = null;
        if ( bindings != null && bindings.size() > 0 ) {
            materializationConnectorName = (String) bindings.iterator().next();
        } else {
            Object[] params = new Object[] {materializationModel.getName()};
            throw new AdminProcessingException(AdminServerPlugin.Util.getString(AdminServerPlugin.Util.getString("ServerConfigAdminImpl.Unable_to_get_binding_name", params))); //$NON-NLS-1$
        }
        
        try {
            materializationConnector = getConnectorBindingByName(materializationConnectorName);
        } catch (ServiceException e) {
            throw new AdminComponentException(AdminServerPlugin.Util.getString("ServerConfigAdminImpl.Unable_to_get_binding_name", new Object[] {materializationModel.getName()}), e); //$NON-NLS-1$
        } catch(ConfigurationException e) {
        	throw new AdminComponentException(AdminServerPlugin.Util.getString("ServerConfigAdminImpl.Unable_to_get_binding_name", new Object[] {materializationModel.getName()}), e); //$NON-NLS-1$
        }
        
        // Scrape materialization info from materialization connector binding
        Properties materializationConnectorProps = materializationConnector.getProperties();
        String materializationURL = materializationConnectorProps.getProperty(CONNECTION_PROPERTY_URL);
        if ( materializationUserName == null || materializationUserName.length() == 0 ||
              materializationUserPwd == null || materializationUserPwd.length() == 0) {
            // materialization user and pwd default to same user and pwd being used
            // for materialization connector binding
            materializationUserName = materializationConnectorProps.getProperty(CONNECTION_PROPERTY_USER);
            materializationUserPwd = materializationConnectorProps.getProperty(CONNECTION_PROPERTY_PASSWORD);
        } else {
            // Encrypt materialization user pwd when specified in method args
            // Already done if getting from materialization connector
            try {
                materializationUserPwd = PropertiesUtils.saveConvert(CryptoUtil.stringEncrypt(materializationUserPwd), false);
            } catch (CryptoException e) {
                throw new AdminProcessingException(AdminServerPlugin.Util.getString("ServerConfigAdminImpl.Unable_to_encrypt_mat_db_user", new Object[] {materializationUserName}), e); //$NON-NLS-1$
            }
        }
        // Encrypt metamatrix user password - will allways need to be done
        try {
            metamatrixUserPwd = PropertiesUtils.saveConvert(CryptoUtil.stringEncrypt(metamatrixUserPwd), false);
        } catch (CryptoException e) {
            throw new AdminProcessingException(AdminServerPlugin.Util.getString("ServerConfigAdminImpl.Unable_to_encrypt_MM_user", new Object[] {metamatrixUserName}), e); //$NON-NLS-1$

        }
        
        String materializationDriver = materializationConnectorProps.getProperty(CONNECTION_PROPERTY_DRIVER);
        
        // Get host info for building MM URL from system information
        String mmHost = null;
        String mmPort = null;
        Collection hosts = parent.getHosts(AdminObject.WILDCARD);
        org.teiid.adminapi.Host aHost = (org.teiid.adminapi.Host) hosts.iterator().next();
        mmHost = aHost.getName();
        Collection hostProcesses = parent.getProcesses(aHost.getIdentifier() + AdminObject.DELIMITER + AdminObject.WILDCARD);
        ProcessObject hostProcess = (ProcessObject) hostProcesses.iterator().next();
        mmPort = hostProcess.getPropertyValue(ProcessObject.SERVER_PORT);
        
        //boolean useSSL = SSLConfiguration.isSSLEnabled();
        boolean useSSL = false;
            
        String mmDriver = "com.metamatrix.jdbc.MMDriver"; //$NON-NLS-1$
        
        // Generate connection props and insert into scripts.
        MaterializationLoadScriptsImpl binaryScripts = (MaterializationLoadScriptsImpl)
            RuntimeMetadataHelper.createMaterializedViewLoadPropertiesVersion(materializationModel, materializationURL, materializationDriver,
                                                                   materializationUserName, materializationUserPwd, mmHost, mmPort,
                                                                   mmDriver, useSSL, metamatrixUserName, metamatrixUserPwd, vdbName, vdbVersion);
        
        MMScriptsContainer scripts = new MMScriptsContainer();
        scripts.addFile(binaryScripts.getCreateScriptFileName(), binaryScripts.getCreateFileContents());
        scripts.addFile(binaryScripts.getConnectionPropsFileName(), binaryScripts.getConPropsFileContents());
        scripts.addFile(binaryScripts.getTruncateScriptFileName(), binaryScripts.getTruncateFileContents());
        scripts.addFile(binaryScripts.getLoadScriptFileName(), binaryScripts.getLoadFileContents());
        scripts.addFile(binaryScripts.getSwapScriptFileName(), binaryScripts.getSwapFileContents());
                
        return scripts;
    }
    

    /**
     * @see com.metamatrix.admin.api.server.ServerConfigAdmin#disableHost(java.lang.String)
     * @since 4.3
     */
    public void disableHost(String identifier) throws AdminException {

        try {
			Collection hosts = getConfigurationServiceProxy().getHosts();

			for (Iterator iter = hosts.iterator(); iter.hasNext();) {
			    Host hostObject = (Host)iter.next();
			    String hostName = hostObject.getName();
			    if (identifierMatches(identifier, new String[] {hostName})) {
			        Host updatedHost = updateHost(hostObject, false);
			        if (updatedHost == null) {
			            throwProcessingException("ServerConfigAdminImpl.Host_was_null", new Object[] {hostName}); //$NON-NLS-1$
			        }
			    }
			}
		} catch (ConfigurationException e) {
			throw new AdminComponentException(e);
		} catch (ServiceException e) {
			throw new AdminComponentException(e);
		} catch (ModificationException e) {
			throw new AdminComponentException(e);
		}
    }

    /**
     * @param hostObject
     * @return
     * @throws ConfigurationException
     * @throws ServiceException
     * @throws ModificationException
     * @since 4.3
     */
    private Host updateHost(Host hostObject, boolean enable) 
    	throws ConfigurationException, ModificationException {
    	
        Properties theProperties = hostObject.getProperties();
        theProperties.setProperty(HostType.HOST_ENABLED, Boolean.toString(enable)); 
        return (Host)getConfigurationServiceProxy().modify(hostObject, theProperties, getUserName());
    }

    /**
     * @see com.metamatrix.admin.api.server.ServerConfigAdmin#disableProcess(java.lang.String)
     * @since 4.3
     */
    public void disableProcess(String identifier) throws AdminException {

        Collection defns = new ArrayList();
        try {
            defns = getConfigurationServiceProxy().getCurrentConfiguration().getVMComponentDefns();
        } catch (ConfigurationException e) {
        	throw new AdminComponentException(e);
        } catch (ServiceException e) {
        	throw new AdminComponentException(e);
        } 
        
        try {
			for (Iterator iter = defns.iterator(); iter.hasNext();) {
			    VMComponentDefn defn = (VMComponentDefn)iter.next();
			    String processName = defn.getName();
			    String hostName = defn.getHostID().getName();

			    String[] identifierParts = new String[] {
			        hostName, processName
			    };
			    if (identifierMatches(identifier, identifierParts)) {
			        VMComponentDefn updatedProcess = updateProcess(defn, false);
			        if (updatedProcess == null) {
			            final Object[] params = new Object[] {
			                identifier, hostName
			            };
			            throwProcessingException("ServerConfigAdminImpl.Process_was_null", params); //$NON-NLS-1$
			        }
			    }
			}
		} catch (ConfigurationException e) {
			throw new AdminComponentException(e);
		} catch (ModificationException e) {
			throw new AdminComponentException(e);
		}

    }

    /**
     * @param defn
     * @return
     * @since 4.3
     */
    private VMComponentDefn updateProcess(VMComponentDefn defn, boolean enabled) 
    	throws ConfigurationException, ModificationException {
    	
        Properties processProperties = defn.getProperties();
        processProperties.setProperty(VMComponentDefnType.ENABLED_FLAG, Boolean.toString(enabled));
        return (VMComponentDefn)getConfigurationServiceProxy().modify(defn, processProperties, getUserName());
    }

    /**
     * @see com.metamatrix.admin.api.server.ServerConfigAdmin#enableHost(java.lang.String)
     * @since 4.3
     */
    public void enableHost(String identifier) throws AdminException {

        try {
			Collection hosts = getConfigurationServiceProxy().getHosts();

			for (Iterator iter = hosts.iterator(); iter.hasNext();) {
			    Host hostObject = (Host)iter.next();
			    String hostName = hostObject.getName();
			    if (identifierMatches(identifier, new String[] {hostName})) {
			        Host updatedHost = updateHost(hostObject, true);
			        if (updatedHost == null) {
			            throwProcessingException("ServerConfigAdminImpl.Host_was_null", new Object[] {hostName}); //$NON-NLS-1$
			        }
			    }
			}
		} catch (ConfigurationException e) {
			throw new AdminComponentException(e);
		} catch (ServiceException e) {
			throw new AdminComponentException(e);
		} catch (ModificationException e) {
			throw new AdminComponentException(e);
		}
    }

    /**
     * @see com.metamatrix.admin.api.server.ServerConfigAdmin#enableProcess(java.lang.String)
     * @since 4.3
     */
    public void enableProcess(String identifier) throws AdminException {

        try {
			Collection defns = getConfigurationServiceProxy().getCurrentConfiguration().getVMComponentDefns();

			for (Iterator iter = defns.iterator(); iter.hasNext();) {
			    VMComponentDefn defn = (VMComponentDefn)iter.next();
			    String processName = defn.getName();
			    String hostName = defn.getHostID().getName();

			    String[] identifierParts = new String[] {
			        hostName, processName
			    };
			    if (identifierMatches(identifier, identifierParts)) {
			        VMComponentDefn updatedProcess = updateProcess(defn, true);
			        if (updatedProcess == null) {
			            final Object[] params = new Object[] {
			                identifier, hostName
			            };
			            throwProcessingException("ServerConfigAdminImpl.Process_was_null", params); //$NON-NLS-1$
			        }
			    }
			}
		} catch (ConfigurationException e) {
			throw new AdminComponentException(e);
		} catch (ServiceException e) {
			throw new AdminComponentException(e);
		} catch (ModificationException e) {
			throw new AdminComponentException(e);
		}
    }

    /** 
     * @see org.teiid.adminapi.ConfigurationAdmin#exportConfiguration()
     * @since 4.3
     */
    public char[] exportConfiguration() throws AdminException {

        char[] results = null;

        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        OutputStream os = new BufferedOutputStream(baos);

        try {

            ConfigurationModelContainer container = getConfigurationServiceProxy().getConfigurationModel(Configuration.NEXT_STARTUP);
            ConfigurationModelContainerAdapter adapter = new ConfigurationModelContainerAdapter();
            adapter.writeConfigurationModel(os, container, getUserName());
            results = ObjectConverterUtil.bytesToChar(baos.toByteArray(), null);
        } catch (ServiceException e) {
        	throw new AdminComponentException(e);
        } catch(ConfigurationException e){
        	throw new AdminComponentException(e);
        } catch(IOException e) {
        	throw new AdminComponentException(e);
        } finally {
            if (os != null) {
                try {
                    os.close();
                } catch (IOException err) {
                }
            }
            if (baos != null) {
                try {
                    baos.close();
                } catch (IOException err) {
                }
            }
        }
        return results;
    }
    
    /** 
     * @see org.teiid.adminapi.ConfigurationAdmin#importConfiguration(char[])
     * @since 4.3
     */
    public void importConfiguration(char[] fileData) throws AdminException {
        InputStream is = null; 
        
        try {
            is = ObjectConverterUtil.convertToInputStream(fileData);
            
    
            //Import the configuration into a set of Objects
            ConfigurationObjectEditor readEditor = getConfigurationServiceProxy().createEditor();
            XMLConfigurationImportExportUtility utility = new XMLConfigurationImportExportUtility();
            Collection objects = utility.importConfigurationObjects(is, readEditor, Configuration.NEXT_STARTUP);  
                    
            //Delete the NEXT_STARTUP configuration.
            ConfigurationObjectEditor writeEditor = getConfigurationServiceProxy().createEditor();
            writeEditor.delete(Configuration.NEXT_STARTUP_ID);
    
            //Save the new configuration as the NEXT_STARTUP
            writeEditor.createConfiguration(Configuration.NEXT_STARTUP_ID, objects);            
            getConfigurationServiceProxy().executeTransaction(writeEditor.getDestination().getActions(), getUserName());
            
        } catch(ConfigObjectsNotResolvableException e) {
        	throw new AdminComponentException(e);
        } catch(InvalidConfigurationElementException e) {
        	throw new AdminComponentException(e);
        } catch (ConfigurationException e) {  
        	throw new AdminComponentException(e); 
        } catch (ServiceException e) {
        	throw new AdminComponentException(e);
        } catch(IOException e){
        	throw new AdminComponentException(e);
        } finally {
            try {
                is.close();
            } catch (IOException e) {                
            }
        }
        
        
    }
    
    

    /** 
     * @see org.teiid.adminapi.ConfigurationAdmin#exportConnectorBinding(java.lang.String)
     * @since 4.3
     */
    public char[] exportConnectorBinding(String connectorBindingIdentifier) throws AdminException {

        char[] results = null;

        List<ConnectorBinding> selectedBindings = new ArrayList<ConnectorBinding>();
        List<ComponentType> selectedTypes = new ArrayList<ComponentType>();

        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        OutputStream os = new BufferedOutputStream(baos);

        XMLConfigurationImportExportUtility util = new XMLConfigurationImportExportUtility();
        try {
            // get config data from ConfigurationService
             ConfigurationModelContainer config = getConfigurationServiceProxy().getConfigurationModel(Configuration.NEXT_STARTUP);

            Collection<ConnectorBinding> components = config.getConfiguration().getConnectorBindings();

            for (Iterator<ConnectorBinding> iter = components.iterator(); iter.hasNext();) {
                ConnectorBinding binding = iter.next();

                String bindingName = binding.getName();

                String[] identifierParts = new String[] {
                     bindingName
                };

                if (identifierMatches(connectorBindingIdentifier, identifierParts)) {
                    selectedBindings.add(binding);
                    ComponentType ct = config.getComponentType(binding.getComponentTypeID().getFullName());
                    selectedTypes.add(ct);
                }
            }
            // get the selected bindings
            int numSelected = selectedBindings.size();
            
            // If we didn't find any matching bindings, no need to continue
            if ( numSelected == 0 ) {
                return new char[] {};
            }
            
            ConnectorBinding[] bindingArray = new ConnectorBinding[numSelected];
            bindingArray = (ConnectorBinding[])selectedBindings.toArray(bindingArray);
            
            // convert the type
            int numSelected2 = selectedTypes.size();
            ComponentType[] typeArray = new ComponentType[numSelected2];
            typeArray = (ComponentType[])selectedTypes.toArray(typeArray);

            Properties properties = getPropertiesForExporting();
            util.exportConnectorBindings(os, bindingArray, typeArray, properties);
            results = ObjectConverterUtil.bytesToChar(baos.toByteArray(), null);
            
        } catch(ConfigObjectsNotResolvableException e) {
        	throw new AdminComponentException(e);
        } catch(ConfigurationException e) {
        	throw new AdminComponentException(e); 
        } catch (ServiceException e) {
        	throw new AdminComponentException(e);
        } catch(IOException e) {
        	throw new AdminComponentException(e); 
        } finally {
            if (os != null) {
                try {
                    os.close();
                } catch (IOException err) {
                }
            }
            if (baos != null) {
                try {
                    baos.close();
                } catch (IOException err) {
                }
            }
        }
        return results;
    }

    /**
     * @see org.teiid.adminapi.ConfigurationAdmin#exportConnectorType(java.lang.String)
     * @since 4.3
     */
    public char[] exportConnectorType(String connectorTypeIdentifier) throws AdminException {

        char[] results = null;

        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        OutputStream os = new BufferedOutputStream(baos);

        XMLConfigurationImportExportUtility util = new XMLConfigurationImportExportUtility();
        try {
            List selectedTypes = getComponentTypes(connectorTypeIdentifier);
            
            // get the selected types
            int numSelected = selectedTypes.size();

            ComponentType[] typeArray = new ComponentType[numSelected];
            typeArray = (ComponentType[])selectedTypes.toArray(typeArray);

            Properties properties = getPropertiesForExporting();
            util.exportComponentTypes(os, typeArray, properties);
            results = ObjectConverterUtil.bytesToChar(baos.toByteArray(), null);
            
        } catch(ConfigurationException e) {
        	throw new AdminComponentException(e);
        } catch (IOException e) {
        	throw new AdminComponentException(e);
        } finally {
            if (os != null) {
                try {
                    os.close();
                } catch (IOException err) {
                }
            }
            if (baos != null) {
                try {
                    baos.close();
                } catch (IOException err) {
                }
            }
        }
        return results;
    }

    private List getComponentTypes(String connectorTypeIdentifier) throws ConfigurationException {
        
        List selectedTypes = new ArrayList();
        
        // get types from ConfigurationService
        Collection types = getConfigurationServiceProxy().getAllComponentTypes(false);

        for (Iterator iter = types.iterator(); iter.hasNext();) {
            ComponentType componentType = (ComponentType)iter.next();
            if (componentType.getComponentTypeCode() == ComponentType.CONNECTOR_COMPONENT_TYPE_CODE) {

                String name = componentType.getName();
                String[] identifierParts = new String[] {
                    name
                };
                if (identifierMatches(connectorTypeIdentifier, identifierParts)) {
                    selectedTypes.add(componentType);
                }
            }
        }
        return selectedTypes;
    }

    
    /** 
     * @see org.teiid.adminapi.ConfigurationAdmin#exportConnectorArchive(java.lang.String)
     * @since 4.3
     */
    public byte[] exportConnectorArchive(String connectorTypeIdentifier) throws AdminException {
       
        BasicConnectorArchive archive = new BasicConnectorArchive();
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        try {
            List selectedTypes = getComponentTypes(connectorTypeIdentifier);
            
            // get the selected types
            int numSelected = selectedTypes.size();
            
            // Get the Connector type first
            if (numSelected == 0) {
                throwProcessingException("ServerConfigAdminImpl.Connector_Type_not_found_in_Configuration", new Object[] {connectorTypeIdentifier}); //$NON-NLS-1$
            }
                    
            for (Iterator i = selectedTypes.iterator(); i.hasNext();) {
                ConnectorBindingType type = (ConnectorBindingType)i.next();
                archive.addConnectorType(type);
                
                String[] modules = type.getExtensionModules();
                for (int m = 0; m < modules.length; m++) {
                    String extModuleName = modules[m];
                    // ignore the patch jar..
                    if (!"connector_patch.jar".equals(extModuleName)) { // //$NON-NLS-1$                           
                        ExtensionModuleDescriptor emd = getExtensionSourceManager().getSourceDescriptor(extModuleName);
                        byte[] source = getExtensionSourceManager().getSource(extModuleName);                
                        ExtensionModule extModule = new BasicExtensionModule(extModuleName, emd.getType(), emd.getDescription(), source);
                        archive.addExtensionModule(type, extModule);
                    }
                }
            }
                    
            // now use the export utility to bundle up the componenets to to zip file
            XMLConfigurationImportExportUtility util = new XMLConfigurationImportExportUtility();                        
            util.exportConnectorArchive(baos, archive, getPropertiesForExporting());
            return baos.toByteArray();
            
        } catch(ConfigObjectsNotResolvableException e) {
        	throw new AdminComponentException(e);
        } catch(ExtensionModuleNotFoundException e) {
        	throw new AdminComponentException(e);
        } catch(MetaMatrixComponentException e) {
        	throw new AdminComponentException(e);
        } catch (IOException e) {
        	throw new AdminComponentException(e);
        } finally {
            try {baos.close();} catch (IOException e) {}            
        }         
    }
    
    
    /** 
     * @see org.teiid.adminapi.ConfigurationAdmin#exportExtensionModule(java.lang.String)
     * @since 4.3
     */
    public byte[] exportExtensionModule(String identifier) throws AdminException {
        byte[] data = null;
                
    	try {
			//get modules from ExtensionSourceManager
			Collection modules = getExtensionSourceManager().getSourceDescriptors();
   
			for (Iterator iter = modules.iterator(); iter.hasNext();) {
			    ExtensionModuleDescriptor descriptor = (ExtensionModuleDescriptor) iter.next();
			    String sourceName = descriptor.getName();

			    String[] identifierParts = new String[] {sourceName};                
			    if (identifierMatches(identifier, identifierParts)) {
			        data = getExtensionSourceManager().getSource(sourceName);
			        break;
			    }
			}
		} catch (ExtensionModuleNotFoundException e) {
			throw new AdminComponentException(e);
		} catch (MetaMatrixComponentException e) {
			throw new AdminComponentException(e);
		}
        return data;
    }

    /**  
     * @see org.teiid.adminapi.ConfigurationAdmin#exportVDB(java.lang.String, java.lang.String)
     * @since 4.3
     */
    public byte[] exportVDB(String name, String version) throws AdminException {
        VDBArchive archive = null;
        try {
            archive = VDBDefnFactory.createVDBArchive(name, version);
            archive.updateRoles(exportDataRoles(name, version));
            return VDBArchive.writeToByteArray(archive);
            
        } catch (Exception e) {
        	//TODO: remove the generalization of Exception
        	throw new AdminComponentException(e);
        } finally {
        	if (archive != null) {
        		archive.close();
        	}
        }
    }

      
    /**
	 * @return
	 * @since 4.3
	 */
    private Properties getPropertiesForExporting() {
        Properties properties = new Properties();
        properties.put(ConfigurationPropertyNames.APPLICATION_CREATED_BY, "ServerAdmin"); //$NON-NLS-1$
        properties.put(ConfigurationPropertyNames.APPLICATION_VERSION_CREATED_BY, "4.3"); //$NON-NLS-1$
        properties.put(ConfigurationPropertyNames.USER_CREATED_BY, getUserName());
        return properties;
    }
    

    /**
     * @see com.metamatrix.admin.api.server.ServerConfigAdmin#getLogConfiguration()
     * @since 4.3
     */
    public LogConfiguration getLogConfiguration() throws AdminException {
//        com.metamatrix.common.log.LogConfiguration logConfig = null;
//        try {
//            logConfig = getConfigurationServiceProxy().getNextStartupConfiguration().getLogConfiguration();
//        } catch (ConfigurationException e) {
//        	throw new AdminComponentException(e);
//        } catch (ServiceException e) {
//        	throw new AdminComponentException(e);
//        }
//        
//        MMLogConfiguration result = new MMLogConfiguration();
//        if (logConfig != null) {
//            result.setLogLevel(logConfig.getMessageLevel());
//            result.setDiscardedContexts(logConfig.getDiscardedContexts());
//
//            // get the Set of all contexts, remove the ones which are
//            // currently "discarded"
//            Set contextsSet = new HashSet(LogContextsUtil.ALL_CONTEXTS);
//            if (logConfig.getDiscardedContexts() != null) {
//                contextsSet.removeAll(logConfig.getDiscardedContexts());
//            }
//            result.setIncludedContexts(contextsSet);
//        } // if
//        return result;
    	  return null;
    }

    /**
     * @see com.metamatrix.admin.api.server.ServerConfigAdmin#setLogConfiguration(org.teiid.adminapi.LogConfiguration)
     * @since 4.3
     */
    public void setLogConfiguration(LogConfiguration adminLogConfig) throws AdminException {
//        Configuration config = null;
//        try {
//            config = getConfigurationServiceProxy().getNextStartupConfiguration();
//        } catch (ConfigurationException e) {
//        	throw new AdminComponentException(e);
//        } catch (ServiceException e) {
//        	throw new AdminComponentException(e);
//        }
//        
//        if (config != null) {
//            Set discardedCtx = adminLogConfig.getDiscardedContexts();
//            Set includedCtx = adminLogConfig.getIncludedContexts();
//            
//            // if both include CTX_ALL, do nothing
//            if ( discardedCtx.contains(LogConfiguration.CTX_ALL) && 
//                 includedCtx.contains(LogConfiguration.CTX_ALL) ) {
//                return;
//            }
//            // if CTX_ALL flag is contained, all other contexts are ignored
//            if ( discardedCtx.contains(LogConfiguration.CTX_ALL) ) {
//                discardedCtx = new HashSet(LogContextsUtil.ALL_CONTEXTS);
//                includedCtx = Collections.EMPTY_SET;
//            } else if ( includedCtx.contains(LogConfiguration.CTX_ALL) ) {
//                includedCtx = new HashSet(LogContextsUtil.ALL_CONTEXTS);
//                discardedCtx = Collections.EMPTY_SET;
//            }
//            
//            com.metamatrix.common.log.LogConfiguration logConfig = config.getLogConfiguration();
//            
//            logConfig.setMessageLevel(adminLogConfig.getLogLevel());
//            logConfig.recordContexts(includedCtx);
//            logConfig.discardContexts(discardedCtx);
//            LogManager.setLogConfiguration(logConfig);
//
//            ConfigurationObjectEditor coe = null;
//            try {
//				coe = getConfigurationServiceProxy().createEditor();
//				coe.setLogConfiguration(config, logConfig);
//				ModificationActionQueue maq = coe.getDestination();
//				java.util.List actions = maq.popActions();
//				getRuntimeStateAdminAPIHelper().setLogConfiguration(config, logConfig, actions, getUserName());
//			} catch (ConfigurationException e) {
//				throw new AdminComponentException(e);
//			} catch (ServiceException e) {
//				throw new AdminComponentException(e);
//			} catch (MetaMatrixComponentException e) {
//				throw new AdminComponentException(e);
//			}
//        } // if

    }

    /**
     * @see com.metamatrix.admin.api.server.ServerConfigAdmin#setSystemProperty(java.lang.String, java.lang.String)
     * @since 4.3
     */
    public void setSystemProperty(String propertyName,
                                  String propertyValue) throws AdminException {
        if (propertyName == null) {
            throw new IllegalArgumentException(AdminPlugin.Util.getString("ServerConfigAdminImpl.Property_name_can_not_be_null")); //$NON-NLS-1$
        }
        try {
			getConfigurationServiceProxy().setSystemPropertyValue(propertyName, propertyValue, getUserName());
		} catch (ConfigurationException e) {
			throw new AdminComponentException(e);
		} catch (ServiceException e) {
			throw new AdminComponentException(e);
		}
    }
    
    /** 
     * @see org.teiid.adminapi.ConfigurationAdmin#updateSystemProperties(java.util.Properties)
     * @since 4.3
     */
    public void updateSystemProperties(Properties properties) throws AdminException {
    	try {
			getConfigurationServiceProxy().updateSystemPropertyValues(properties, getUserName());
		} catch (ConfigurationException e) {
			throw new AdminComponentException(e);
		} catch (ServiceException e) {
			throw new AdminComponentException(e);
		}
    }
    

    /**
     * Supported classes are  {@link org.teiid.adminapi.Host}, {@link org.teiid.adminapi.ConnectorBinding}, 
     * {@link SystemObject}, {@link ProcessObject}
     * @see com.metamatrix.admin.api.server.ServerConfigAdmin#setProperty(java.lang.String, java.lang.String, java.lang.String)
     * @since 4.3
     */
    public void setProperty(String identifier,
                            String className,
                            String propertyName,
                            String propertyValue) throws AdminException {

        Properties properties = new Properties();
        properties.setProperty(propertyName, propertyValue);
        
        updateProperties(identifier, className, properties);
    }

    
    
    /** 
     * Supported classes are {@link org.teiid.adminapi.ConnectorBinding}, {@link org.teiid.adminapi.Service}, 
     * {@link SystemObject}, {@link ProcessObject}
     * @see org.teiid.adminapi.ConfigurationAdmin#updateProperties(java.lang.String, java.lang.String, java.util.Properties)
     * @since 4.3
     */
    public void updateProperties(String identifier,
                                 String className,
                                 Properties properties) throws AdminException {
    
    	int nodeCount = getNodeCount(identifier); 
    	int type = MMAdminObject.getObjectType(className);
    	
    	AdminObject adminObject = null;
    	String hostName;
        
        
        switch (type) {
            
            case MMAdminObject.OBJECT_TYPE_SYSTEM_OBJECT:
                this.updateSystemProperties(properties);
                break;
                
            case MMAdminObject.OBJECT_TYPE_PROCESS_OBJECT:
            	adminObject = getAdminObject(identifier, className);
                ProcessObject process = (ProcessObject)adminObject;
                String processName = adminObject.getName();
                hostName = process.getHostIdentifier();
                try {
					VMComponentDefn vmDefn = getVMByName(hostName, processName);
					Properties processProperties = vmDefn.getProperties();
					processProperties.putAll(properties);
					
					VMComponentDefn updatedProcess = (VMComponentDefn)getConfigurationServiceProxy().modify(vmDefn,
					                                                                                        processProperties,
					                                                                                        getUserName());
					if (updatedProcess == null) {
					    throwProcessingException("ServerConfigAdminImpl.Process_was_null_when_updating_properties", new Object[] {processName}); //$NON-NLS-1$
					}
				} catch (ConfigurationException e) {
					throw new AdminComponentException(e);
				} catch (ServiceException e) {
					throw new AdminComponentException(e);
				} catch (MetaMatrixProcessingException e) {
					throw new AdminProcessingException(e);
				}
                    
                break;
                
            case MMAdminObject.OBJECT_TYPE_CONNECTOR_BINDING:
            	String connectorBindingName;
            	if (nodeCount > 1) {
            		adminObject = getAdminObject(identifier, className);
            		connectorBindingName = adminObject.getName();
            	}else{
            		connectorBindingName = identifier;
            	}
             	
            	try {
					ConnectorBinding connectorBinding = this.getConnectorBindingByName(connectorBindingName);
						
					//If the node count in the identifier is greater than 1, then this connector binding is deployed.
					if (nodeCount>1){
						DeployedComponent updatedConnectorBinding = updateDeployedComponentProperties(
								properties, identifier);
						
						if (updatedConnectorBinding == null) {
						    throwProcessingException("ServerConfigAdminImpl.Connector_Binding_was_null_when_updating_properties", new Object[] {connectorBindingName}); //$NON-NLS-1$
						}
					}else{
					
						Properties bindingProperties = connectorBinding.getProperties();
						bindingProperties.putAll(properties);
						
						ConnectorBinding updatedConnectorBinding = 
						    (ConnectorBinding)getConfigurationServiceProxy().modify(connectorBinding,
						                                                            bindingProperties,
						                                                            getUserName());
						
						if (updatedConnectorBinding == null) {
						    throwProcessingException("ServerConfigAdminImpl.Connector_Binding_was_null_when_updating_properties", new Object[] {connectorBindingName}); //$NON-NLS-1$
						}
					}
				} catch (ConfigurationException e) {
					throw new AdminComponentException(e);
				} catch (ServiceException e) {
					throw new AdminComponentException(e);
				}
                break;
                
            case MMAdminObject.OBJECT_TYPE_SERVICE:
            	String serviceName;
            	if (nodeCount > 1) {
            		adminObject = getAdminObject(identifier, className);
            		serviceName = adminObject.getName();
            	}else{
            		serviceName = identifier;
            	}
            	
                try {
					ServiceComponentDefn serviceDefn = this.getServiceByName(serviceName);
					
					ComponentDefnID componentDefnID = (ComponentDefnID)serviceDefn.getID();

					//If the node count in the identifier is greater than 1, then this service is deployed.
					if (nodeCount>1){
						DeployedComponent updatedServiceDefn = updateDeployedComponentProperties(
								properties, identifier);
						
						if (updatedServiceDefn == null) {
						    throwProcessingException("ServerConfigAdminImpl.Service_was_null_when_updating_properties", new Object[] {serviceName}); //$NON-NLS-1$
						}
					}else{
					
						Properties svcProperties = serviceDefn.getProperties();
						svcProperties.putAll(properties);
						
						ServiceComponentDefn updatedServiceDefn = 
						    (ServiceComponentDefn)getConfigurationServiceProxy().modify(serviceDefn,
						                                                            svcProperties,
						                                                            getUserName());
						
						if (updatedServiceDefn == null) {
						    throwProcessingException("ServerConfigAdminImpl.Service_was_null_when_updating_properties", new Object[] {serviceName}); //$NON-NLS-1$
						}
					}
				} catch (ConfigurationException e) {
					throw new AdminComponentException(e);
				} catch (ServiceException e) {
					throw new AdminComponentException(e);
				} catch (InvalidSessionException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				} catch (AuthorizationException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				} catch (MetaMatrixComponentException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
                break;
                
                
            default:
                throwProcessingException("ServerConfigAdminImpl.Unsupported_Admin_Object", new Object[] {className}); //$NON-NLS-1$
        }

    }

	/**
	 * @param properties
	 * @param componentDefnID
	 * @return
	 * @throws ConfigurationException
	 */
	private DeployedComponent updateDeployedComponentProperties(
			Properties properties, String identifier)
			throws ConfigurationException {
		DeployedComponent dc = getDeployedComponent(identifier);

		Properties bindingProperties = dc.getProperties();
		bindingProperties.putAll(properties);
		
		DeployedComponent updatedConnectorBinding = 
		    (DeployedComponent)getConfigurationServiceProxy().modify(dc,
		                                                            bindingProperties,
		                                                            getUserName());
		return updatedConnectorBinding;
	}

	private AdminObject getAdminObject(String identifier, String className)
			throws AdminException {
		Collection adminObjects = getAdminObjects(identifier, className);        
        if (adminObjects == null || adminObjects.size() == 0) {
            throwProcessingException("ServerConfigAdminImpl.No_Objects_Found", new Object[] {identifier, className}); //$NON-NLS-1$
        }
        if (adminObjects.size() > 1) {
            throwProcessingException("ServerConfigAdminImpl.Multiple_Objects_Found", new Object[] {identifier, className}); //$NON-NLS-1$
        }
        AdminObject adminObject = (AdminObject) adminObjects.iterator().next();
		return adminObject;
	}

    private ConnectorBinding getConnectorBindingByName(String name) throws ConfigurationException, ServiceException, AdminProcessingException {
        Configuration nextStartupConfig = getConfigurationServiceProxy().getNextStartupConfiguration();
        ConnectorBinding cb = nextStartupConfig.getConnectorBinding(name);
        if (cb == null) {
        	throw new AdminProcessingException(AdminServerPlugin.Util.getString("ServerConfigAdminImpl.Connector_Binding_not_found_in_Configuration", name)); //$NON-NLS-1$
        }
        return cb;
    }
    
    protected List<ConnectorBinding> getConnectorBindingsByName(String[] bindingNames) throws ConfigurationException, ServiceException, AdminProcessingException {
    	List<ConnectorBinding> bindingList = new ArrayList<ConnectorBinding>(bindingNames.length);
    	
        for(int i=0; i<bindingNames.length; i++) {
        	bindingList.add(getConnectorBindingByName(bindingNames[i]));
        }
        return bindingList;
    }
    
    
    /** 
     * @see org.teiid.adminapi.ConfigurationAdmin#assignBindingToModel(java.lang.String, java.lang.String, java.lang.String, java.lang.String)
     * @since 4.3
     */
    public void assignBindingToModel(String connectorBindingName,
                                     String vdbName,
                                     String vdbVersion,
                                     String modelName) throws AdminException {
    	String[] connectorBindingNames = new String[1];
    	connectorBindingNames[0] = connectorBindingName;
    	assignBindingsToModel(connectorBindingNames,vdbName,vdbVersion,modelName);
    }
    
    /** 
     * @see org.teiid.adminapi.ConfigurationAdmin#deassignBindingFromModel(java.lang.String, java.lang.String, java.lang.String, java.lang.String)
     * @since 5.0
     */
    public void deassignBindingFromModel(String connectorBindingName,
                                          String vdbName,
                                          String vdbVersion,
                                          String modelName) throws AdminException {
    	String[] connectorBindingNames = new String[1];
    	connectorBindingNames[0] = connectorBindingName;
    	deassignBindingsFromModel(connectorBindingNames,vdbName,vdbVersion,modelName);
    }

    /** 
     * @see org.teiid.adminapi.ConfigurationAdmin#assignBindingsToModel(String[], java.lang.String, java.lang.String, java.lang.String)
     * @since 5.0
     */
    public void assignBindingsToModel(String[] connectorBindingNames,
                                       String vdbName,
                                       String vdbVersion,
                                       String modelName) throws AdminException {
        try {

            List newBindingList = getConnectorBindingsByName(connectorBindingNames);
            if (!newBindingList.isEmpty()) {

                Collection colVdbs = getVirtualDatabases();
                if (colVdbs != null) {
                    for (Iterator iter = colVdbs.iterator(); iter.hasNext();) {
                        VirtualDatabase vdb = (VirtualDatabase)iter.next();

                        if (vdb.getName().equals(vdbName) && vdb.getVirtualDatabaseID().getVersion().equals(vdbVersion)) {
                            VirtualDatabaseID vdbId = (VirtualDatabaseID)vdb.getID();
                            Collection models = getModels(vdbId);

                            if (models != null) {
                                HashMap map = new HashMap(models.size());
                                Iterator modelIter = models.iterator();
                                while (modelIter.hasNext()) {
                                    Model model = (Model)modelIter.next();
            
                                    if (model.getName().equals(modelName)) {
                                   	 
                                        List bindings = new ArrayList(newBindingList.size());
    			                        // if the model is enabled for multi-source bindings, add the new binding 
                                    	// to the existing list rather than overwriting the existing list.
                                        
                                        if (model.isMultiSourceBindingEnabled()) {
                                            // use the mutlibindings set to ensure unique binding names
                                            Set multibindings = new HashSet();
                                            if (!(model.getConnectorBindingNames().isEmpty())) {
                                                multibindings.addAll(model.getConnectorBindingNames());
                                            }
                                            
                                            Iterator bindingsIter = newBindingList.iterator();
                                            while(bindingsIter.hasNext()) {
                                                ConnectorBinding newBinding = (ConnectorBinding)bindingsIter.next();
                                                multibindings.add(newBinding.getRoutingUUID());                                               
                                            }
    		                                // convert set to list for the map
                                            bindings.addAll(multibindings);

                                        } else {
                                            bindings.add( ((ConnectorBinding)newBindingList.get(0)).getRoutingUUID());

                                        }                                                                              
                                        
                                        map.put(model.getName(), bindings);
                                    } else {
                                        //put the original name in the map, because the user is not changing this
                                        map.put(model.getName(), model.getConnectorBindingNames());
                                    }
                                }
                                
                                setConnectorBindingNames(vdbId, map);

                            }
                            setVDBState(vdbId, VDB.ACTIVE);
                            // done
                            break;
                        }
                    }
                }
            } else {
                throw new AdminProcessingException(AdminServerPlugin.Util.getString("ServerConfigAdminImpl.Connector_Binding_not_found_in_Configuration")); //$NON-NLS-1$
            }
        } catch (VirtualDatabaseException e) {
        	throw new AdminComponentException(e);
        } catch(ConfigurationException e) {
        	throw new AdminComponentException(e);
        }
    }
    
    /** 
     * @see org.teiid.adminapi.ConfigurationAdmin#deassignBindingFromModel(String[], java.lang.String, java.lang.String, java.lang.String)
     * @since 5.0
     */
    public void deassignBindingsFromModel(String[] connectorBindingNames,
                                           String vdbName,
                                           String vdbVersion,
                                           String modelName) throws AdminException {
        try {

            List<ConnectorBinding> connectorList = getConnectorBindingsByName(connectorBindingNames);

            Collection colVdbs = getVirtualDatabases();
            if (colVdbs == null) {
            	return;
            }
            for (Iterator iter = colVdbs.iterator(); iter.hasNext();) {
                VirtualDatabase vdb = (VirtualDatabase)iter.next();

                if (vdb.getName().equals(vdbName) && vdb.getVirtualDatabaseID().getVersion().equals(vdbVersion)) {
                    VirtualDatabaseID vdbId = (VirtualDatabaseID)vdb.getID();
                    Collection models = getModels(vdbId);

                    if (models != null) {
                        HashMap map = new HashMap(models.size());
                        Iterator modelIter = models.iterator();
                        while (modelIter.hasNext()) {
                            Model model = (Model)modelIter.next();
    
                            if (model.getName().equals(modelName)) {                                       
                                if (model.getConnectorBindingNames().size() > 0) {
                                     
                                     // load all the existing bindings
                                     Set bindingSet = new HashSet(model.getConnectorBindingNames().size());
                                     bindingSet.addAll(model.getConnectorBindingNames());
                                     
                                     // remove the bindings passed in to be removed
                                     for (ConnectorBinding binding : connectorList) {
                                        	bindingSet.remove(binding.getRoutingUUID());                                               
                                     }
                                     
                                     // convert the set to a list for the map
	                                 List bindings = new ArrayList(model.getConnectorBindingNames().size());
                                     bindings.addAll(bindingSet);
                                     map.put(model.getName(), bindings);
                                 }
                            } else {
                                //put the original name in the map, because the user is not changing this
                                map.put(model.getName(), model.getConnectorBindingNames());
                            }
                        }
                        
                        setConnectorBindingNames(vdbId, map);

                    }
                    setVDBState(vdbId, VDB.ACTIVE);
                    // done
                    break;
                }
            }
        } catch (VirtualDatabaseException e) {
        	throw new AdminComponentException(e);
        } catch(ConfigurationException e) {
        	throw new AdminComponentException(e);
        }
        
        
    }

    protected Collection getVirtualDatabases( ) throws VirtualDatabaseException {
        return RuntimeMetadataCatalog.getInstance().getVirtualDatabases();
    }

    protected Collection getModels(VirtualDatabaseID vdbId) throws VirtualDatabaseException {
        return RuntimeMetadataCatalog.getInstance().getModels(vdbId);
    }
    
    protected void setConnectorBindingNames(VirtualDatabaseID vdbId,
                                          Map mapModelsToConnBinds) throws VirtualDatabaseException  {
        RuntimeMetadataCatalog.getInstance().setConnectorBindingNames(vdbId, mapModelsToConnBinds, getUserName());
    }
    
    protected void setVDBState(VirtualDatabaseID vdbID,
                             int siState) throws VirtualDatabaseException {
        RuntimeMetadataCatalog.getInstance().setVDBStatus(vdbID, (short)siState, getUserName());
    }

    /**
     * @see com.metamatrix.admin.api.server.ServerConfigAdmin#deleteConnectorBinding(java.lang.String)
     * @since 4.3
     */
    public void deleteConnectorBinding(String connectorBindingIdentifier) throws AdminException {
        // First, determine if the binding is still mapped to another VDB
        // Can't muck with bindings that are mapped
        // TODO: How do you determine whether a connector binding is mapped (in use) by some VDB?
        
        // Second, stop connector binding if running
        Collection binding = parent.getConnectorBindings(connectorBindingIdentifier);
        if ( binding != null && binding.size() > 0 ) {
        org.teiid.adminapi.ConnectorBinding theBinding = (org.teiid.adminapi.ConnectorBinding)binding.iterator().next();
            if ( theBinding != null && theBinding.getState() == org.teiid.adminapi.ConnectorBinding.STATE_OPEN ) {
                try {
                    shutDownConnectorBinding((MMConnectorBinding)theBinding, true);
                } catch (final Exception err) {
                    // ignore - not running
                }
            }
        }

        String connectorBindingName = getName(connectorBindingIdentifier);
        Configuration nextStartupConfig = null;
        try {
            nextStartupConfig = getConfigurationServiceProxy().getNextStartupConfiguration();
        } catch (ConfigurationException e) {
        	throw new AdminComponentException(e);
        } catch (ServiceException e) {
        	throw new AdminComponentException(e);
        }
        
        if ( nextStartupConfig == null ) {
            return;
        }

        ServiceComponentDefn service = null;
        try {
            service = this.getServiceByName(connectorBindingName);
        } catch (InvalidSessionException e) {
        	throw new AdminComponentException(e);
        } catch (AuthorizationException e) {
        	throw new AdminComponentException(e);
        } catch (ConfigurationException e) {
        	throw new AdminComponentException(e);
        } catch (MetaMatrixComponentException e) {
        	throw new AdminComponentException(e);
        }

        if (service == null) {
            //Some Bindings may not have a Service 
            //throw new AdminProcessingException(AdminServerPlugin.Util.getString("ServerConfigAdminImpl.Connector_Binding_not_found_in_Configuration",new Object[] {connectorBindingName}); //$NON-NLS-1$
        } else {
        	try {
				getConfigurationServiceProxy().delete(service, false, getUserName());
			} catch (ConfigurationException e) {
				throw new AdminComponentException(e);
			} catch (ServiceException e) {
				throw new AdminComponentException(e);
			}
        }

        ConnectorBinding cb = nextStartupConfig.getConnectorBinding(connectorBindingName);

        if (cb == null) {
            throwProcessingException("ServerConfigAdminImpl.Connector_Binding_not_found_in_Configuration", new Object[] {connectorBindingName}); //$NON-NLS-1$
        } else {
        	try {
				getConfigurationServiceProxy().delete(cb, false, getUserName());
			} catch (ConfigurationException e) {
				throw new AdminComponentException(e);
			} catch (ServiceException e) {
				throw new AdminComponentException(e);
			}
        }
    }

    
    /**
     * @see com.metamatrix.admin.api.server.ServerConfigAdmin#deleteConnectorType(java.lang.String)
     * @since 4.3
     */
    public void deleteConnectorType(String name) throws AdminException {
        
        ComponentType ct = this.getComponentType(name);
        if (ct == null) {
            throwProcessingException("ServerConfigAdminImpl.Connector_Type_not_found_in_Configuration", new Object[] {name}); //$NON-NLS-1$
        } else {
	        try {
				getConfigurationServiceProxy().delete(ct, getUserName());
			} catch (ConfigurationException e) {
				throw new AdminComponentException(e);
			} catch (ServiceException e) {
				throw new AdminComponentException(e);
			}
        }

    }

    private ComponentType getComponentType(String name) throws AdminException {

        Collection types = null;
        try {
            types = getConfigurationServiceProxy().getAllComponentTypes(true);
        } catch (ConfigurationException e) {
        	throw new AdminComponentException(e);
        } catch (ServiceException e) {
        	throw new AdminComponentException(e);
        }
        
        ComponentType result = null;
        Iterator compTypes = types.iterator();
        while (compTypes.hasNext()) {
            ComponentType type = (ComponentType)compTypes.next();
            if (type.getName().equals(name)) {
                result = type;
                break;
            }
        }
        return result;
    }

    /**
     * @see com.metamatrix.admin.api.server.ServerConfigAdmin#deleteHost(java.lang.String)
     * @since 4.3
     */
    public void deleteHost(String identifier) throws AdminException {

        String hostName = getName(identifier);

        try {
			Host host = getHostByName(hostName);
			if (host != null) {
			    getConfigurationServiceProxy().delete(host, false, getUserName());
			}
		} catch (ConfigurationException e) {
			throw new AdminComponentException(e);
		} catch (ServiceException e) {
			throw new AdminComponentException(e);
		}

    }

    /*
     * This method will get the Host by Name from the Next COnfiguration
     */
    private Host getHostByName(String hostName) throws AdminException {
        Host theHost = null;
        try {
            theHost = getConfigurationServiceProxy().getHost(new HostID(hostName));
        } catch (ConfigurationException e) {
        	throw new AdminComponentException(e);
        } catch (ServiceException e) {
        	throw new AdminComponentException(e);
        } 
        if (theHost == null) {
            throwProcessingException("ServerConfigAdminImpl.Host_not_found_in_Configuration", new Object[] {hostName}); //$NON-NLS-1$
        }
        return theHost;
    }

    /*
     * Look up the VM in Configuration
     */
    private VMComponentDefn getVMByName(String hostName,
                                        String processName) throws ConfigurationException,
                                                           ServiceException,
                                                           MetaMatrixProcessingException, AdminException {
        VMComponentDefn result = null;
        Collection defns = getConfigurationServiceProxy().getCurrentConfiguration().getVMComponentDefns();

        // convert config data to MMProcess objects, merge with runtime data
        for (Iterator iter = defns.iterator(); iter.hasNext();) {
            VMComponentDefn defn = (VMComponentDefn)iter.next();
            if (defn.getName().equalsIgnoreCase(processName) && defn.getHostID().getName().equalsIgnoreCase(hostName)) {
                result = defn;
                break;
            }
        }
        if (result == null) {

            final Object[] params = new Object[] {
                processName, hostName
            };
            throwProcessingException("ServerConfigAdminImpl.Process_not_found_in_Configuration", params); //$NON-NLS-1$
        }
        return result;
    }

    /**
     * @see com.metamatrix.admin.api.server.ServerConfigAdmin#deleteProcess(java.lang.String)
     * @since 4.3
     */
    public void deleteProcess(String processIdentifier) throws AdminException {
        String processName = getName(processIdentifier);
        if (processName == null) {
            throw new AdminProcessingException(AdminPlugin.Util.getString("ServerConfigAdminImpl.Name_can_not_be_null")); //$NON-NLS-1$
        }
        String hostName = getParent(processIdentifier);
        if (hostName == null) {
            throw new AdminProcessingException(AdminPlugin.Util.getString("ServerConfigAdminImpl.Host_name_can_not_be_null")); //$NON-NLS-1$
        }
        try {
			VMComponentDefn vmDefn = getVMByName(hostName, processName);
			if (vmDefn != null) {
			    getConfigurationServiceProxy().delete(vmDefn, false, getUserName());
			}
		} catch (ConfigurationException e) {
			throw new AdminComponentException(e);
		} catch (ServiceException e) {
			throw new AdminComponentException(e);
		} catch (MetaMatrixProcessingException e) {
			throw new AdminComponentException(e);
		}
    }

    protected ServiceComponentDefn getServiceByName(String serviceName) throws ConfigurationException,
                                                                       InvalidSessionException,
                                                                       AuthorizationException,
                                                                       MetaMatrixComponentException {
        ServiceComponentDefnID serviceID = new ServiceComponentDefnID(Configuration.NEXT_STARTUP_ID, serviceName);
        return getServiceByID(serviceID);
    }

    protected ServiceComponentDefn getServiceByID(ServiceComponentDefnID serviceID) throws InvalidSessionException,
                                                                                   AuthorizationException,
                                                                                   ConfigurationException,
                                                                                   MetaMatrixComponentException {
    	
         return (ServiceComponentDefn)getConfigurationServiceProxy().getComponentDefn(Configuration.NEXT_STARTUP_ID,serviceID);
    }
    
   
    /** 
     * @param properties
     * @param connectorBindingName
     * @param connectorTypeIdentifier the component type ID for the config object.  Used to find
     * "isMasked" properties for which to attempt decryption.
     * @return
     */
    private AdminStatus checkDecryption(Properties properties, String connectorBindingName, String connectorTypeIdentifier) 
     {
        boolean decryptable = true;
        
        if (decryptable) {
            return new MMAdminStatus(AdminStatus.CODE_SUCCESS, "AdminStatus.CODE_SUCCESS"); //$NON-NLS-1$
        } 
        
        return new MMAdminStatus(AdminStatus.CODE_DECRYPTION_FAILED, "AdminStatus.CODE_DECRYPTION_FAILED", connectorBindingName); //$NON-NLS-1$
    }

    /**
     * Check that the properties of the specified ConnectorBinding can be decrypted.
     * @param 
     * @return
     * @throws AdminException 
     * @since 4.3
     */
    private AdminStatus checkDecryption(ConnectorBinding binding) throws AdminException {
        
        List bindings = new ArrayList();
        bindings.add(binding);
        List decryptables = new ArrayList();
        try {
            decryptables = getConfigurationServiceProxy().checkPropertiesDecryptable(bindings);
        } catch (ConfigurationException e) {
        	throw new AdminComponentException(e);
        } catch (ServiceException e) {
        	throw new AdminComponentException(e);
        }
        
        boolean decryptable = ((Boolean) decryptables.get(0)).booleanValue();
        
        if (decryptable) {
            return new MMAdminStatus(AdminStatus.CODE_SUCCESS, "AdminStatus.CODE_SUCCESS"); //$NON-NLS-1$
        } 
        
        return new MMAdminStatus(AdminStatus.CODE_DECRYPTION_FAILED, "AdminStatus.CODE_DECRYPTION_FAILED", binding.getName()); //$NON-NLS-1$
    }
    
    /**
     * Check that the properties of the specified ConnectorBindings can be decrypted. 
     * @param bindings List<ConnectorBinding> 
     * @return
     * @throws AdminException 
     * @since 4.3
     */
    private AdminStatus checkDecryption(List bindings) throws AdminException {
        
        List decryptables = new ArrayList();
        try {
            decryptables = getConfigurationServiceProxy().checkPropertiesDecryptable(bindings);
        } catch (ConfigurationException e) {
        	throw new AdminComponentException(e);
        } catch (ServiceException e) {
        	throw new AdminComponentException(e);
        }
        
        List nonDecryptableBindings = new ArrayList();
        
        Iterator iter1 = bindings.iterator();
        Iterator iter2 = decryptables.iterator();
        while (iter1.hasNext() && iter2.hasNext()) {
            ConnectorBinding binding = (ConnectorBinding) iter1.next();
            boolean decryptable = ((Boolean) iter2.next()).booleanValue();
         
            if (! decryptable) {
                nonDecryptableBindings.add(binding);
            }
        }
                
        if (nonDecryptableBindings.size() == 0) {
            return new MMAdminStatus(AdminStatus.CODE_SUCCESS, "AdminStatus.CODE_SUCCESS"); //$NON-NLS-1$
        } 
        
        return new MMAdminStatus(AdminStatus.CODE_DECRYPTION_FAILED, "AdminStatus.CODE_DECRYPTION_FAILED", //$NON-NLS-1$ 
                                 prettyPrintBindingNames(nonDecryptableBindings)); 
    }
    
    /**
     * Check to see if any new connector bindings being added collide with existing bindings.
     * Binding collision is determined by connector binding name.  Collision resolution is
     * determined by examining the AdminOptions the user passed in.
     * 
     * @param existingBindings Collection of ConnectorBinding that already exist in the
     * system.  Their names are used to determine collision with new binding names.
     * @param newBindingNames
     * @param options One of the {@link AdminOptions.OnConflict} options.
     * @return The collection of total bindings to add or update.
     * @throws AdminException
     * @since 4.3
     */
    protected Collection getBindingNamesToUpdate(Collection existingBindings, Collection newBindingNames, AdminOptions options) throws AdminException {
        if ( options == null ) {
            // Default is to NOT update any existing connector bindingds.
            options = new AdminOptions(AdminOptions.OnConflict.IGNORE);
        }
        Collection addBindings = new ArrayList(newBindingNames.size());
        if (existingBindings == null || existingBindings.size() == 0) {
            // If no bindings exist, no need to check options
            addBindings.addAll(newBindingNames);
        } else {
            // Get the names of the existing bindings
            Collection existingBindingNames = getBindingNames(existingBindings);

            // Build lists of colliding and non-colliding binding names
            Collection collidingBindingNames = new ArrayList(existingBindingNames.size());
            Collection nonCollidingBindingNames = new ArrayList(existingBindingNames.size());
            for (Iterator newBindingItr = newBindingNames.iterator(); newBindingItr.hasNext();) {
                String newBindingName = (String) newBindingItr.next();
                if (existingBindingNames.contains(newBindingName)) {
                    collidingBindingNames.add(newBindingName);
                } else {
                    nonCollidingBindingNames.add(newBindingName);
                }
            }

            if (collidingBindingNames.size() > 0) {
                // At least some binding names collide, check options
                if (options.containsOption(AdminOptions.OnConflict.OVERWRITE)) {
                    // User specified overwrite - update all
                    addBindings.addAll(newBindingNames);
                } else if (options.containsOption(AdminOptions.OnConflict.IGNORE)) {
                    // User specified not to overwrite - add only non-colliding, if any
                    addBindings.addAll(nonCollidingBindingNames);
                } else if (options.containsOption(AdminOptions.OnConflict.EXCEPTION)) {
                    // User specified to throw exception if bindings already exist
                    String msg = (collidingBindingNames.size() == 1 ? 
                                    AdminServerPlugin.Util.getString("ServerConfigAdminImpl.binding_exists", collidingBindingNames.toArray()) :  //$NON-NLS-1$
                                        AdminServerPlugin.Util.getString("ServerConfigAdminImpl.bindings_exist", collidingBindingNames.toArray())); //$NON-NLS-1$
                    throw new AdminProcessingException(msg);
                } else {
                    // Unknown AdminOption - throw exception
                    throw new AdminProcessingException(AdminServerPlugin.Util.getString("ServerConfigAdminImpl.Unknown_admin_options", new Object[] {options.toString()})); //$NON-NLS-1$
                }
            } else {
                // No existing bindings collide with new bindings - add new bindings
                addBindings.addAll(nonCollidingBindingNames);
            }
        }
       
        return addBindings;
    }
    
    /**
     * Gets and returns ConnectorBinding names from the given Collection of connector bindings.
     * Handles "both" types of ConnectorBindings - common.config.api.ConnectorBinding and
     * admin.api.objects.ConnectorBinding.
     * 
     * @param connectorBindings A Collection of ConnectorBinding objects of either type
     * common.config.api.ConnectorBinding or admin.api.objects.ConnectorBinding.
     * @return The Collection of String connector binding names from the given collection.
     * @since 4.3
     */
    private Collection getBindingNames(Collection connectorBindings) {
        Collection bindingNames = new ArrayList(connectorBindings.size());
        for (Iterator bindingItr = connectorBindings.iterator(); bindingItr.hasNext();) {
            Object aBindingObj = bindingItr.next();
            String aBindingName = null;
            // Handle connector bindings of both dialects
            if (aBindingObj instanceof ConnectorBinding) {
                aBindingName = ((ConnectorBinding)aBindingObj).getName();
            } else {
                // instance of com.metamatrix.admin.api.objects.ConnectorBinding - or exception
                aBindingName = ((org.teiid.adminapi.ConnectorBinding)aBindingObj).getName();
            }
            bindingNames.add(aBindingName);
        }
        return bindingNames;
    }

    private String prettyPrintBindingNames(List bindings) {
        StringBuffer buffer = new StringBuffer();
        for (Iterator iter = bindings.iterator(); iter.hasNext();) {
            ConnectorBinding binding = (ConnectorBinding) iter.next();
            buffer.append(binding.getName());
            if (iter.hasNext()) {
                buffer.append(", "); //$NON-NLS-1$
            }
        }
        
        return buffer.toString();
    }
    
    /**
     * Will return the number of nodes in an identifier based on the
     * delimiter {@link AdminObject.DELIMITER}
     * @param identifier
     * @return the number of nodes
     */
    protected int getNodeCount(String identifier) {
    	int count = 0;
    	
    	count = identifier.split("\\"+AdminObject.DELIMITER).length;
        
        return count;
    }

    /** 
     * @see org.teiid.adminapi.ConfigurationAdmin#addUDF(byte[], java.lang.String)
     */
    public void addUDF(byte[] modelFileContents, String classpath) throws AdminException {
    	classpath = classpath.trim();
        try {
            deleteExtensionModule(FUNCTION_DEFINITIONS_MODEL);
        } catch (AdminException e) {
            // ok to not to find the file
        }
        
        // add the function definitions as extension modules
        addExtensionModule(ExtensionModule.FUNCTION_DEFINITION_TYPE, FUNCTION_DEFINITIONS_MODEL, modelFileContents, "User Defined Functions File"); //$NON-NLS-1$
        String commonpath = CurrentConfiguration.getInstance().getProperties().getProperty(ServerPropertyNames.COMMON_EXTENSION_CLASPATH, ""); //$NON-NLS-1$
        
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
        setSystemProperty(ServerPropertyNames.COMMON_EXTENSION_CLASPATH, sb.toString()+commonpath);
    }

    /** 
     * @see org.teiid.adminapi.ConfigurationAdmin#deleteUDF()
     */
    public void deleteUDF() throws AdminException {
        deleteExtensionModule(FUNCTION_DEFINITIONS_MODEL);
    }

	@Override
	public Properties getBootstrapProperties() throws AdminException {
		Properties p = new Properties();
		p.putAll(CurrentConfiguration.getInstance().getBootStrapProperties());
		return p;
	}

	@Override
	public byte[] getClusterKey() throws AdminException {
		Cryptor cryptor;
		try {
			cryptor = CryptoUtil.getCryptor();
		} catch (CryptoException e) {
			throw new AdminComponentException(e);
		}
		if (cryptor instanceof SymmetricCryptor) {
			return ((SymmetricCryptor)cryptor).getEncodedKey(); 
		}
		return null;
	}

	@Override
	public void extensionModuleModified(String name) throws AdminException {
		
	}
    
}