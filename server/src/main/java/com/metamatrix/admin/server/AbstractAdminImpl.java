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

import java.io.IOException;
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

import javax.xml.parsers.ParserConfigurationException;

import org.teiid.adminapi.AdminComponentException;
import org.teiid.adminapi.AdminException;
import org.teiid.adminapi.AdminObject;
import org.teiid.adminapi.AdminOptions;
import org.teiid.adminapi.AdminProcessingException;
import org.teiid.adminapi.VDB;
import org.teiid.dqp.internal.process.DQPWorkContext;
import org.xml.sax.SAXException;

import com.metamatrix.admin.api.exception.security.InvalidSessionException;
import com.metamatrix.admin.objects.MMAdminObject;
import com.metamatrix.admin.objects.MMConnectorBinding;
import com.metamatrix.admin.objects.MMModel;
import com.metamatrix.admin.objects.MMPropertyDefinition;
import com.metamatrix.admin.objects.MMVDB;
import com.metamatrix.api.exception.MetaMatrixComponentException;
import com.metamatrix.api.exception.security.AuthorizationException;
import com.metamatrix.api.exception.security.AuthorizationMgmtException;
import com.metamatrix.common.config.api.ComponentObject;
import com.metamatrix.common.config.api.ComponentType;
import com.metamatrix.common.config.api.ComponentTypeDefn;
import com.metamatrix.common.config.api.Configuration;
import com.metamatrix.common.config.api.ConfigurationModelContainer;
import com.metamatrix.common.config.api.ConnectorBinding;
import com.metamatrix.common.config.api.DeployedComponent;
import com.metamatrix.common.config.api.exceptions.ConfigurationException;
import com.metamatrix.common.config.model.BasicDeployedComponent;
import com.metamatrix.common.extensionmodule.ExtensionModuleManager;
import com.metamatrix.common.log.LogManager;
import com.metamatrix.common.object.PropertyDefinition;
import com.metamatrix.common.util.LogContextsUtil.PlatformAdminConstants;
import com.metamatrix.metadata.runtime.RuntimeMetadataCatalog;
import com.metamatrix.metadata.runtime.api.Model;
import com.metamatrix.metadata.runtime.api.VirtualDatabase;
import com.metamatrix.metadata.runtime.api.VirtualDatabaseException;
import com.metamatrix.metadata.runtime.model.BasicVirtualDatabaseID;
import com.metamatrix.platform.admin.api.EntitlementMigrationReport;
import com.metamatrix.platform.admin.apiimpl.RuntimeStateAdminAPIHelper;
import com.metamatrix.platform.config.api.service.ConfigurationServiceInterface;
import com.metamatrix.platform.registry.ClusteredRegistryState;
import com.metamatrix.platform.security.api.AuthorizationPolicyFactory;
import com.metamatrix.platform.security.api.AuthorizationRealm;
import com.metamatrix.platform.security.api.MetaMatrixSessionID;
import com.metamatrix.platform.security.api.SessionToken;
import com.metamatrix.platform.security.api.service.AuthorizationServiceInterface;
import com.metamatrix.platform.security.api.service.MembershipServiceInterface;
import com.metamatrix.platform.security.api.service.SessionServiceInterface;
import com.metamatrix.platform.service.api.ServiceID;
import com.metamatrix.platform.service.api.exception.ServiceException;
import com.metamatrix.platform.vm.api.controller.ProcessManagement;
import com.metamatrix.server.admin.apiimpl.RuntimeMetadataHelper;
import com.metamatrix.server.query.service.QueryServiceInterface;

/**
 * @since 4.3
 */
public class AbstractAdminImpl {

    private static final String DOUBLE_ESCAPED_DELIMITER = "\\" + AdminObject.ESCAPED_DELIMITER; //$NON-NLS-1$

    protected ServerAdminImpl parent = null;

    private static String regexpAnyCharZeroOrMore = ".*"; //$NON-NLS-1$
    
    protected ClusteredRegistryState registry;
    
    /**
     * @since 4.3
     */
    public AbstractAdminImpl(ServerAdminImpl parent, ClusteredRegistryState registry) {
        super();

        this.parent = parent;
        this.registry = registry;
    }

    /**
     * Get the Parent Name for this Identifier
     * 
     * @param identifier
     * @return
     * @since 4.3
     */
    protected String getParent(String identifier) {
        return MMAdminObject.getParentName(identifier);
    }

    /**
     * Get the Node Name for this Identifier
     * 
     * @param identifier
     * @return
     * @since 4.3
     */
    protected String getName(String identifier) {
        return MMAdminObject.getNameFromIdentifier(identifier);
    }
    
    /**
     * Get The User Name for this Connection
     * 
     * @return String User Name for this Session to MetaMatrix
     * @since 4.3
     */
    protected String getUserName() {
        return DQPWorkContext.getWorkContext().getSessionToken().getUsername();
    }

    /**
     * Get The <code>MetaMatrixSessionID</code> for this Connection
     * 
     * @return this Session ID
     * @since 4.3
     */
    protected MetaMatrixSessionID getSessionID() {
        return DQPWorkContext.getWorkContext().getSessionToken().getSessionID();
    }
    
    protected SessionToken validateSession() {
        return DQPWorkContext.getWorkContext().getSessionToken();
    }


    /**
     * Throw a processing exception with a localized message.
     * 
     * @param key
     *            Key of message in i18n.properties file.
     * @param objects
     *            Objects to substitute into message.
     * @since 4.3
     */
    protected void throwProcessingException(String key, Object[] objects) throws AdminException {
        throw new AdminProcessingException(AdminServerPlugin.Util.getString(key, objects));
    }

    /**
     * Log a localized message.
     * 
     * @param key
     *            Key of message in i18n.properties file.
     * @param params
     *            Objects to substitute into message.
     * @since 4.3
     */
    protected void logDetail(String key,
                             Object[] params) {
        final String msg = AdminServerPlugin.Util.getString(key, params);
        LogManager.logDetail(PlatformAdminConstants.CTX_ADMIN, msg);
    }

    protected synchronized SessionServiceInterface getSessionServiceProxy() throws ServiceException {
        return parent.getSessionServiceProxy();
    }

    protected synchronized MembershipServiceInterface getMembershipServiceProxy() throws ServiceException {
        return parent.getMembershipServiceProxy();
    }

    protected synchronized AuthorizationServiceInterface getAuthorizationServiceProxy() throws ServiceException {
        return parent.getAuthorizationServiceProxy();
    }

    protected synchronized ConfigurationServiceInterface getConfigurationServiceProxy() throws ServiceException {
        return parent.getConfigurationServiceProxy();
    }

    protected synchronized QueryServiceInterface getQueryServiceProxy() throws ServiceException {
        return parent.getQueryServiceProxy();
    }

    protected ExtensionModuleManager getExtensionSourceManager() {
        return parent.getExtensionSourceManager();
    }
    
    protected RuntimeStateAdminAPIHelper getRuntimeStateAdminAPIHelper() throws ServiceException {
        return parent.getRuntimeStateAdminAPIHelper();
    }
    
    /**
     * Returns true if <code>identifierPartsArray</code> contains all the parts of <code>query</code>, in order.
     * 
     * @param query
     * @param identifierPartsArray
     * @return
     * @since 4.3
     */
    public static boolean identifierMatches(String query,
                                            String[] identifierPartsArray) {
        String identifier = MMAdminObject.buildIdentifier(identifierPartsArray);
        return identifierMatches(query, identifier);
    }

    /**
     * Returns true if <code>identifierPartsArray</code> contains all the parts of <code>query</code>, in order.
     * 
     * @param query
     * @param identifier
     * @return
     * @since 4.3
     */
    protected static boolean identifierMatches(String query,
                                            String identifier) {
        // Query was WILDCARD - matches everything
        if (AdminObject.WILDCARD.equals(query)) {
            return true;
        }

        // Canonicalize both search strings
        query = query.toUpperCase();
        identifier = identifier.toUpperCase();
        
        // Check for WILDCARDs in query
        int firstWildcardIndex = query.indexOf(AdminObject.WILDCARD);
        boolean matches = false;
        if ( firstWildcardIndex >= 0 ) {
            // At least one WILDCARD present in query
            // Escape any reg exp chars in the query - '|' char replaced with "\|"
            String regExQuery = query.replaceAll(AdminObject.ESCAPED_DELIMITER, DOUBLE_ESCAPED_DELIMITER);
            // Replace each WILDCARD with appropriate regexp - ".*" - 0 or more of any char.
            regExQuery = regExQuery.replaceAll(AdminObject.ESCAPED_WILDCARD, regexpAnyCharZeroOrMore);
            // Check for regexp match
            matches = identifier.matches(regExQuery);
        } else {
            // No WILDCARD in query - compare directly.
            matches = identifier.equals(query);
        }
        return matches;
    }

    /** 
     * Utility method the converts a VirtualDatabase into an admin VDB object.
     * @param newVDB
     * @return the converted VDB
     * @throws AdminException 
     * @since 4.3
     */
    protected VDB convertToAdminVDB(VirtualDatabase virtualDatabase) throws AdminException {
        MMVDB vdb = null;
        String vdbName = virtualDatabase.getName();
        String vdbVersion = virtualDatabase.getVirtualDatabaseID().getVersion();
        String[] identifierParts = new String[] {vdbName, vdbVersion};
            
        try {
			ConfigurationModelContainer cmc = getConfigurationModel();
			vdb = new MMVDB(identifierParts);
			vdb.setCreated(virtualDatabase.getCreationDate());
			vdb.setCreatedBy(virtualDatabase.getCreatedBy());
			vdb.setLastUpdated(virtualDatabase.getUpdateDate());
			vdb.setLastUpdatedBy(virtualDatabase.getUpdatedBy());
			vdb.setProperties(virtualDatabase.getProperties());
			vdb.setStatus(virtualDatabase.getStatus());
			vdb.setUID(((BasicVirtualDatabaseID)virtualDatabase.getVirtualDatabaseID()).getUID());

			vdb.setVersionedBy(virtualDatabase.getVersionBy());
			vdb.setVersionedDate(virtualDatabase.getVersionDate());
			vdb.setHasWSDL(virtualDatabase.hasWSDLDefined());

			//get the models and convert to MMModel objects
			Collection modelObjects = RuntimeMetadataCatalog.getInstance().getModels(virtualDatabase.getVirtualDatabaseID());
			for (Iterator iter2 = modelObjects.iterator(); iter2.hasNext();) {
			    Model modelObject = (Model)iter2.next();

			    String modelName = modelObject.getName();
			    String[] modelIdentifierParts = new String[] {
			        vdbName, modelName
			    };
			    MMModel model = new MMModel(modelIdentifierParts);
			    model.setConnectorBindingNames(getConnectorBindingNamesFromUUIDs(modelObject.getConnectorBindingNames(), cmc));
			    model.setMaterialization(modelObject.isMaterialization());
			    if (modelObject.isMaterialization()) {
			        vdb.setMaterializedViews(true);
			    }
			    model.setModelType(modelObject.getModelTypeName());
			    model.setModelURI(modelObject.getModelURI());
			    model.setPhysical(modelObject.isPhysical());
			    model.setProperties(modelObject.getProperties());
			    model.setSupportsMultiSourceBindings(modelObject.supportsMultiSourceBindings());
			    model.setVisible(modelObject.isVisible());

			    vdb.addModel(model);
			}
		} catch (VirtualDatabaseException e) {
			throw new AdminProcessingException(e);
		} catch (ConfigurationException e) {
			throw new AdminComponentException(e);
		}
        return vdb;
    }

    /**
     * Utility method to find VDBs in virtualDatabases collection matching identifier
     * and create VDB admin objects to return in sorted collection (VDBs and versions
     * will be together). 
     * @param identifier
     * @param virtualDatabases
     * @return Collection of {@link VDB}s matching identifier or empty collection.
     * @throws AdminException 
     * @since 4.3
     */
    protected List getVDBs(String identifier,
                         Collection virtualDatabases) throws AdminException {
        ArrayList results = new ArrayList(virtualDatabases.size());
        if ( identifier.indexOf(AdminObject.DELIMITER_CHAR) < 0 && identifier.indexOf(AdminObject.WILDCARD) < 0 ) {
            // If no WILDCARD specified for a VDB, apply a WILDCARD to
            // identifier so that, if only full VDB name was given,
            // all VDB versions will be returned.
            identifier = identifier.concat(AdminObject.DELIMITER_CHAR + AdminObject.WILDCARD); 
        }
        for (Iterator iter = virtualDatabases.iterator(); iter.hasNext();) {
            VirtualDatabase virtualDatabase = (VirtualDatabase) iter.next();
            String vdbName = virtualDatabase.getName();
            String vdbVersion = virtualDatabase.getVirtualDatabaseID().getVersion();
            String[] identifierParts = new String[] {vdbName, vdbVersion};
            
            if (identifierMatches(identifier, identifierParts)) {
                VDB vdb = convertToAdminVDB(virtualDatabase);
                results.add(vdb);
            }
        }
        // sort so that [VDB vdbVersion] are sorted
        // this means that, for a given VDB, latest version will be last
        Collections.sort(results);
        return results;
    }
    
//    protected String getConnectorBindingNameFromUUID(String uuid) throws ConfigurationException {
//        Configuration config;
//        config = getConfigurationServiceProxy().getCurrentConfiguration();
//        ConnectorBinding cb = config.getConnectorBindingByRoutingID(uuid);
//        if (cb != null) {
//            return cb.getName();
//        } 
//        
//        return null;
//    }
    
    protected List getConnectorBindingNamesFromUUIDs(List uuids, ConfigurationModelContainer configModel) {
        List results = new ArrayList(uuids.size());
        for (Iterator iter = uuids.iterator(); iter.hasNext();) {
            String uuid = (String) iter.next();

            ConnectorBinding cb = configModel.getConfiguration().getConnectorBindingByRoutingID(uuid);
            if (cb != null) {
                results.add(cb.getName());
            }
        }
        
        return results;        
    }    
    
    
//    protected List getConnectorBindingNamesFromUUIDs(List uuids) throws ConfigurationException, ServiceException {
//        ConfigurationModelContainer cmc = getConfigurationModel();
//        
//        return getConnectorBindingNamesFromUUIDs(uuids, cmc);
//    }
    
    protected Map getConnectorBindingNamesMapFromUUIDs(Collection uuids) throws ConfigurationException {
        Configuration config;
        config = getConfigurationServiceProxy().getCurrentConfiguration();

        Map uuidMap = new HashMap(uuids.size());
        for (Iterator iter = uuids.iterator(); iter.hasNext();) {
            String uuid = (String) iter.next();
            
            ConnectorBinding cb = config.getConnectorBindingByRoutingID(uuid);
            if (cb != null) {
                uuidMap.put(uuid, cb.getName());
            }
        }
        
        return uuidMap;        
    }    
    
    protected void shutDownConnectorBinding(MMConnectorBinding binding, boolean stopNow) throws AdminException {
            
        ServiceID serviceID = new ServiceID(binding.getServiceID(), binding.getHostName(), binding.getProcessName());
        
        try {
			ProcessManagement vmController = getProcessController(serviceID.getHostName(), serviceID.getProcessName());
		    vmController.stopService(serviceID, stopNow, true);
		} catch (MetaMatrixComponentException e) {
			throw new AdminComponentException(e);
		}
    }

    private ProcessManagement getProcessController(String hostName, String processName) throws MetaMatrixComponentException {
    	return this.registry.getProcessBinding(hostName, processName).getProcessController();
    } 

    /**
     * Convert a ComponentObject into a Collection of com.metamatrix.admin.api.objects.PropertyDefinition objects
     * @param component
     * @return
     * @since 4.3
     */
    protected Collection convertPropertyDefinitions(ComponentObject component) throws ConfigurationException {
        ComponentType ctype = getConfigurationServiceProxy().getComponentType(component.getComponentTypeID());
        Properties properties = component.getProperties();
        
        return convertPropertyDefinitions(ctype, properties);
    }
    
    protected Collection convertPropertyDefinitions(ComponentObject component, Properties properties) throws ConfigurationException {
        ComponentType ctype = getConfigurationServiceProxy().getComponentType(component.getComponentTypeID());
        
        return convertPropertyDefinitions(ctype, properties);
    }
    
    protected Collection convertPropertyDefinitions(ComponentType ctype, Properties properties) {
        ArrayList results = new ArrayList();
        
        for (Iterator iter = ctype.getComponentTypeDefinitions().iterator(); iter.hasNext(); ) {
            ComponentTypeDefn cdefn = (ComponentTypeDefn) iter.next();
            PropertyDefinition pdefn = cdefn.getPropertyDefinition();
                        
            MMPropertyDefinition result = new MMPropertyDefinition(new String[] {pdefn.getName()});
            result.setAllowedValues(pdefn.getAllowedValues());
            result.setDefaultValue(pdefn.getDefaultValue());
            result.setDescription(pdefn.getShortDescription());
            result.setDisplayName(pdefn.getDisplayName());
            result.setExpert(pdefn.isExpert());
            result.setMasked(pdefn.isMasked());
            result.setModifiable(pdefn.isModifiable());
            result.setPropertyType(pdefn.getPropertyType().getDisplayName());
            result.setPropertyTypeClassName(pdefn.getPropertyType().getClassName());
            result.setRequired(pdefn.isRequired());
            result.setRequiresRestart(pdefn.getRequiresRestart());
            
            String value = properties.getProperty(pdefn.getName());
            result.setValue(value);
            
            results.add(result);
        }
        
        
        return results;
    }
    
    protected DeployedComponent getDeployedComponent(String identifier) throws ConfigurationException {
        Configuration config = getConfigurationServiceProxy().getCurrentConfiguration();
        Collection components = config.getDeployedComponents();
        for (Iterator iter = components.iterator(); iter.hasNext(); ) {
            BasicDeployedComponent bdc = (BasicDeployedComponent)iter.next();
            String[] identifierParts = new String[] {
                bdc.getHostID().getName(), bdc.getVMComponentDefnID().getName(), bdc.getName()
            };
            if (identifierMatches(identifier, identifierParts)) {
                return bdc;
            }

        }
        return null;
    }
    
    /**
     * Get admin objects of the specified className that match the specified identifier. 
     * @param identifier
     * @param className
     * @return
     * @since 4.3
     */
    protected Collection getAdminObjects(String identifier, String className) throws AdminException {
        
        int code = MMAdminObject.getObjectType(className);
        
        ArrayList list = null;
        switch(code) {
            case MMAdminObject.OBJECT_TYPE_CACHE:
                return parent.getCaches(identifier);
            case MMAdminObject.OBJECT_TYPE_CONNECTOR_BINDING:
                return parent.getConnectorBindings(identifier);
            case MMAdminObject.OBJECT_TYPE_CONNECTOR_TYPE:
                return parent.getConnectorTypes(identifier);
            case MMAdminObject.OBJECT_TYPE_DQP:
                return parent.getDQPs(identifier);
            case MMAdminObject.OBJECT_TYPE_EXTENSION_MODULE:
                return parent.getExtensionModules(identifier);
            case MMAdminObject.OBJECT_TYPE_GROUP:
                return parent.getGroups(identifier);
            case MMAdminObject.OBJECT_TYPE_HOST:
                return parent.getHosts(identifier);
            case MMAdminObject.OBJECT_TYPE_LOG_CONFIGURATION:
                list = new ArrayList();
                list.add(parent.getLogConfiguration());
                return list;
            case MMAdminObject.OBJECT_TYPE_PROCESS_OBJECT:
                return parent.getProcesses(identifier);
            case MMAdminObject.OBJECT_TYPE_QUEUE_WORKER_POOL:
                return parent.getQueueWorkerPools(identifier);
            case MMAdminObject.OBJECT_TYPE_REQUEST:
                return parent.getRequests(identifier);
            case MMAdminObject.OBJECT_TYPE_SERVICE:
                return parent.getServices(identifier);
            case MMAdminObject.OBJECT_TYPE_RESOURCE:
                return parent.getResources(identifier);
            case MMAdminObject.OBJECT_TYPE_SESSION:
                return parent.getSessions(identifier);
            case MMAdminObject.OBJECT_TYPE_SYSTEM_OBJECT:
                list = new ArrayList();
                list.add(parent.getSystem());
                return list;
            case MMAdminObject.OBJECT_TYPE_VDB:
                return parent.getVDBs(identifier);
            case MMAdminObject.OBJECT_TYPE_TRANSACTION:
                return parent.getTransactions();
            case MMAdminObject.OBJECT_TYPE_ENTITLEMENT:                
            case MMAdminObject.OBJECT_TYPE_MODEL:
            case MMAdminObject.OBJECT_TYPE_PROPERTY_DEFINITION:
            case MMAdminObject.OBJECT_TYPE_ROLE:         
            default:
                throwProcessingException("AbstractAdminImpl.Unsupported_Admin_Object", new Object[] {className}); //$NON-NLS-1$
                
        }
        
        
        return Collections.EMPTY_LIST;
    }
    
    
    protected ConfigurationModelContainer getConfigurationModel() throws ConfigurationException {
        return getConfigurationServiceProxy().getConfigurationModel(Configuration.NEXT_STARTUP);
    }

    String importDataRoles(String vdbName, String vdbVersion, char[] xmlContents, AdminOptions options) 
        throws AdminException {
        try {
            Collection roles = AuthorizationPolicyFactory.buildPolicies(vdbName, vdbVersion, xmlContents);
            SessionToken session = validateSession();

            EntitlementMigrationReport rpt = new EntitlementMigrationReport("from file", vdbName + " " + vdbVersion); //$NON-NLS-1$ //$NON-NLS-2$

            Set allPaths = new HashSet(RuntimeMetadataHelper.getAllDataNodeNames(vdbName, vdbVersion, new HashMap()));

            getAuthorizationServiceProxy().migratePolicies(session, rpt, vdbName, vdbVersion, allPaths, roles, options);

            return rpt.toString();
        } catch (InvalidSessionException e) {
        	throw new AdminProcessingException(e);
		} catch (AuthorizationException e) {
			throw new AdminProcessingException(e);
		} catch (MetaMatrixComponentException e) {
			throw new AdminComponentException(e);
		} catch (SAXException e) {
			throw new AdminComponentException(e);
		} catch (IOException e) {
			throw new AdminComponentException(e);
		} catch (ParserConfigurationException e) {
			throw new AdminComponentException(e);
        }
    }    
    
    char[] exportDataRoles(String vdbName, String vdbVersion) throws AdminException {
        Collection roles = null;
        try {
			roles = getAuthorizationServiceProxy().getPoliciesInRealm(validateSession(), new AuthorizationRealm(vdbName, vdbVersion));
			if (roles != null && !roles.isEmpty()) {
			    return AuthorizationPolicyFactory.exportPolicies(roles);
			}
			return null;
		} catch (AuthorizationMgmtException e) {
			throw new AdminProcessingException(e);
		} catch (AuthorizationException e) {
			throw new AdminProcessingException(e);
		} catch (ServiceException e) {
			throw new AdminComponentException(e);
		} catch (IOException e) {
			throw new AdminComponentException(e);
		}
    }    
}
