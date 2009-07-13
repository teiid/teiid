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
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.teiid.adminapi.AdminComponentException;
import org.teiid.adminapi.AdminException;
import org.teiid.adminapi.AdminObject;
import org.teiid.adminapi.AdminProcessingException;
import org.teiid.adminapi.Cache;
import org.teiid.adminapi.ExtensionModule;
import org.teiid.adminapi.Session;
import org.teiid.adminapi.SystemObject;
import org.teiid.dqp.internal.process.DQPWorkContext;
import org.teiid.dqp.internal.process.Util;

import com.metamatrix.admin.objects.MMAdminObject;
import com.metamatrix.admin.objects.MMConnectorBinding;
import com.metamatrix.admin.objects.MMConnectorType;
import com.metamatrix.admin.objects.MMExtensionModule;
import com.metamatrix.admin.objects.MMLogConfiguration;
import com.metamatrix.admin.objects.MMModel;
import com.metamatrix.admin.objects.MMPropertyDefinition;
import com.metamatrix.admin.objects.MMRequest;
import com.metamatrix.admin.objects.MMSession;
import com.metamatrix.admin.objects.MMSystem;
import com.metamatrix.admin.objects.MMVDB;
import com.metamatrix.api.exception.MetaMatrixComponentException;
import com.metamatrix.api.exception.security.SessionServiceException;
import com.metamatrix.common.config.api.ComponentType;
import com.metamatrix.common.config.api.ComponentTypeDefn;
import com.metamatrix.common.config.api.ConnectorBinding;
import com.metamatrix.common.log.LogConfigurationImpl;
import com.metamatrix.common.object.PropertyDefinition;
import com.metamatrix.common.util.crypto.CryptoException;
import com.metamatrix.common.util.crypto.CryptoUtil;
import com.metamatrix.common.vdb.api.VDBArchive;
import com.metamatrix.dqp.embedded.DQPEmbeddedPlugin;
import com.metamatrix.dqp.service.AuthorizationService;
import com.metamatrix.dqp.service.ConfigurationService;
import com.metamatrix.dqp.service.ConnectorStatus;
import com.metamatrix.dqp.service.DQPServiceNames;
import com.metamatrix.dqp.service.DataService;
import com.metamatrix.dqp.service.TransactionService;
import com.metamatrix.dqp.service.VDBService;
import com.metamatrix.jdbc.EmbeddedConnectionFactoryImpl;
import com.metamatrix.platform.security.api.MetaMatrixSessionInfo;
import com.metamatrix.platform.security.api.SessionToken;
import com.metamatrix.platform.security.api.service.MembershipServiceInterface;
import com.metamatrix.platform.security.api.service.SessionServiceInterface;
import com.metamatrix.platform.util.ProductInfoConstants;
import com.metamatrix.server.serverapi.RequestInfo;


/** 
 * @since 4.3
 */
abstract class BaseAdmin {
    static final String DOT = "."; //$NON-NLS-1$
    static final String STAR = "*"; //$NON-NLS-1$
    static final String FILE_NAME_REGEX="\\w+\\.\\w+"; //$NON-NLS-1$
    static final String MULTIPLE_WORDS_REGEX = "\\w+([\\s|-]*\\w)*"; //$NON-NLS-1$
    static final String SINGLE_WORD_REGEX = "\\w+"; //$NON-NLS-1$    
    static final String MULTIPLE_WORD_WILDCARD_REGEX = "\\w*((\\.)?\\s*\\w)*(\\*)?"; //$NON-NLS-1$
    static final String SINGLE_WORD_WILDCARD_REGEX = "\\w*(\\*)?"; //$NON-NLS-1$
    // This should find word.word.* or word.* kind of patterns (ugly, you can rewrite) 
    static final String WORD_AND_DOT_WILDCARD_REGEX = "\\w+((\\.\\*)|(\\.\\w+)|(\\.\\w*\\*))*|\\w*(\\*){1}"; //$NON-NLS-1$
    
    static final String VDB_REGEX = "\\w*(\\*)?(\\.\\d+)?"; //$NON-NLS-1$
    static final String NUMBER_DOT_REGEX = "\\d+((\\.\\*)|(\\.\\d+)|(\\.\\d*\\*))*|\\d*(\\*){1}"; //$NON-NLS-1$
    static final String NUMBER_REGEX = "\\d*(\\*)?"; //$NON-NLS-1$
        
    static final String[] cacheTypes = {Cache.CODE_TABLE_CACHE, 
        Cache.CONNECTOR_RESULT_SET_CACHE,
        Cache.PREPARED_PLAN_CACHE,
        Cache.QUERY_SERVICE_RESULT_SET_CACHE
        };    
    
    EmbeddedConnectionFactoryImpl manager = null; 
    
    BaseAdmin(EmbeddedConnectionFactoryImpl manager){
        this.manager = manager;       
    }
            
    protected AdminException accumulateSystemException(AdminException parent, Exception e) {
        if (parent == null) {
            return new AdminComponentException(e); 
        }
        parent.addChild(new AdminComponentException(e));
        return parent;
    }
    
    protected AdminException accumulateProcessingException(AdminException parent, Exception e) {
        if (parent == null) {
            return new AdminProcessingException(e); 
        }
        parent.addChild(new AdminProcessingException(e));
        return parent;
    }
    
    protected String prettyPrintBindingNames(List bindings) {
        StringBuffer buffer = new StringBuffer();
        for (Iterator iter = bindings.iterator(); iter.hasNext();) {
            ConnectorBinding binding = (ConnectorBinding) iter.next();
            buffer.append(binding.getDeployedName());
            if (iter.hasNext()) {
                buffer.append(", "); //$NON-NLS-1$
            }
        }
        
        return buffer.toString();
    }
    
    /** 
     * @return Returns the manager.
     * @since 4.3
     */
    public EmbeddedConnectionFactoryImpl getManager() {
        return this.manager;
    }
    
    VDBService getVDBService() {
        return (VDBService)getManager().findService(DQPServiceNames.VDB_SERVICE);            
    }
    
    DataService getDataService() {
        return (DataService)getManager().findService(DQPServiceNames.DATA_SERVICE);
    }
    
    TransactionService getTransactionService() {
    	return (TransactionService)getManager().findService(DQPServiceNames.TRANSACTION_SERVICE);
    }

    MembershipServiceInterface getMembershipService() {
    	return (MembershipServiceInterface)getManager().findService(DQPServiceNames.MEMBERSHIP_SERVICE);
    }
    
    AuthorizationService getAuthorizationService() {
    	return (AuthorizationService)getManager().findService(DQPServiceNames.AUTHORIZATION_SERVICE);
    }
    
    ConfigurationService getConfigurationService() {
        return (ConfigurationService)getManager().findService(DQPServiceNames.CONFIGURATION_SERVICE);
    }
    
    SessionServiceInterface getSessionService() {
        return (SessionServiceInterface)getManager().findService(DQPServiceNames.SESSION_SERVICE);
    }
        
    protected Object convertToAdminObjects(Object src) {
        return convertToAdminObjects(src,null);
    }
    
    protected Object convertToAdminObjects(Object src, Object parent) {
        if (src == null) {
            return src;
        }
        
        if (src instanceof List) {
            List modified = new ArrayList();
            List list = (List)src;
            for (final Iterator i = list.iterator(); i.hasNext();) {
                final Object e = i.next();
                Object converted = convertToAdminObject(e, parent);
                modified.add(converted);
            } 
            return modified;
        }
        else if (src instanceof Collection) {
            List modified = new ArrayList();
            for (Iterator i = ((Collection)src).iterator(); i.hasNext();) {
                final Object e = i.next();
                Object converted = convertToAdminObject(e, parent);
                modified.add(converted);
            } 
            return modified;
        }
        else if (src instanceof Object[] ) {
            List modified = new ArrayList();
            Object[] srcArray = (Object[])src;
            for (int i = 0; i < srcArray.length; i++) {
                final Object converted = convertToAdminObject(srcArray[i], parent);
                modified.add(converted);                
            }
            return modified;
        }
        return convertToAdminObject(src, parent);
    }    
        
    
    private Object convertToAdminObject(Object src, Object parent) {
        if (src != null && src instanceof com.metamatrix.common.config.api.ConnectorBinding) {
            com.metamatrix.common.config.api.ConnectorBinding binding = (com.metamatrix.common.config.api.ConnectorBinding)src;
            return convertConnectorType(binding, parent);
        }
        else if (src != null && src instanceof com.metamatrix.common.config.api.ConnectorBindingType) {
            com.metamatrix.common.config.api.ConnectorBindingType type = (com.metamatrix.common.config.api.ConnectorBindingType)src;
            return convertConnectorType(type, parent);
        }
        else if (src != null && src instanceof com.metamatrix.common.vdb.api.VDBDefn) {
            com.metamatrix.common.vdb.api.VDBDefn vdb = (com.metamatrix.common.vdb.api.VDBDefn)src;
            return convertVDB(vdb, parent);
        }
        else if (src != null && src instanceof VDBArchive) {
        	VDBArchive vdb = (VDBArchive)src;
            return convertVDB(vdb.getConfigurationDef(), parent);
        }        
        else if (src != null && src instanceof com.metamatrix.common.vdb.api.ModelInfo) {
            com.metamatrix.common.vdb.api.ModelInfo model = (com.metamatrix.common.vdb.api.ModelInfo)src;
            return convertModel(model, parent);
        }
        else if (src != null && src instanceof com.metamatrix.common.log.LogConfiguration) {
            com.metamatrix.common.log.LogConfiguration config = (com.metamatrix.common.log.LogConfiguration)src;
            return covertLogConfiguration(config);
        }
        else if (src != null && src instanceof com.metamatrix.server.serverapi.RequestInfo) {
        	com.metamatrix.server.serverapi.RequestInfo request = (com.metamatrix.server.serverapi.RequestInfo)src;
            return convertRequest(request);
        }
        else if (src != null && src instanceof com.metamatrix.common.queue.WorkerPoolStats) {
            com.metamatrix.common.queue.WorkerPoolStats stats = (com.metamatrix.common.queue.WorkerPoolStats)src;
            return Util.convertStats(stats, stats.getQueueName());
        }
        else if (src != null && src instanceof MetaMatrixSessionInfo) {
        	MetaMatrixSessionInfo conn = (MetaMatrixSessionInfo)src;
            return convertConnection(conn);
        }
        else if (src != null && src instanceof com.metamatrix.common.config.api.ExtensionModule) {
            com.metamatrix.common.config.api.ExtensionModule extModule = (com.metamatrix.common.config.api.ExtensionModule)src;
            return convertExtensionModule(extModule);
        }         
        else {
            throw new UnsupportedOperationException(DQPEmbeddedPlugin.Util.getString("UnSupported_object_conversion"));  //$NON-NLS-1$
        }
    }

    Object convertToNativeObjects(Object src) {
        if (src instanceof org.teiid.adminapi.LogConfiguration) {
            org.teiid.adminapi.LogConfiguration config = (org.teiid.adminapi.LogConfiguration)src;
            return covertToNativeLogConfiguration(config);
        }
        throw new UnsupportedOperationException(DQPEmbeddedPlugin.Util.getString("UnSupported_object_conversion"));  //$NON-NLS-1$            
    }
     
    
    private ExtensionModule convertExtensionModule(com.metamatrix.common.config.api.ExtensionModule src) {
        MMExtensionModule module = new MMExtensionModule(new String[] {src.getFullName()}) ;
        module.setDescription(src.getDescription());
        module.setFileContents(src.getFileContents());
        module.setModuleType(src.getModuleType());
        return module;
    }
    
    private Session convertConnection(MetaMatrixSessionInfo src) {
        MMSession session = new MMSession(new String[] {src.getSessionID().toString()});
        session.setVDBName(src.getProductInfo(ProductInfoConstants.VIRTUAL_DB));
        session.setVDBVersion(src.getProductInfo(ProductInfoConstants.VDB_VERSION)); 
        session.setApplicationName(src.getApplicationName());
        session.setIPAddress(src.getClientIp());
        session.setHostName(src.getClientHostname());
        session.setUserName(src.getUserName());
        session.setLastPingTime(src.getLastPingTime());
        session.setCreated(new Date(src.getTimeCreated()));
        return session;
    }
    
    /**
     * Convert LogConfiguration to Admin Object 
     */
    private org.teiid.adminapi.LogConfiguration covertLogConfiguration(final com.metamatrix.common.log.LogConfiguration src) {
    	Map<String, Integer> contextMap = new HashMap<String, Integer>();
    	for(String context:src.getContexts()) {
    		contextMap.put(context, src.getLogLevel(context));
    	}
        return new MMLogConfiguration(contextMap);
    }

    /**
     * Convert LogConfiguration to Admin Object 
     */
    private com.metamatrix.common.log.LogConfiguration covertToNativeLogConfiguration(final org.teiid.adminapi.LogConfiguration src) {
    	Map<String, Integer> contextMap = new HashMap<String, Integer>();
    	for(String context:src.getContexts()) {
    		contextMap.put(context, src.getLogLevel(context));
    	}
    	return new LogConfigurationImpl(contextMap);
    }

    /** 
     * @param binding
     * @return
     * @since 4.3
     */
    private org.teiid.adminapi.ConnectorBinding convertConnectorType(final com.metamatrix.common.config.api.ConnectorBinding src, final Object parent) {
        MMConnectorBinding binding = new MMConnectorBinding(new String[] {src.getDeployedName()});
        
        binding.setConnectorTypeName(src.getComponentTypeID().getFullName());
        binding.setCreated(src.getCreatedDate());
        binding.setCreatedBy(src.getCreatedBy());
        binding.setDescription(src.getDescription());
        binding.setEnabled(src.isEnabled());
        binding.setLastUpdated(src.getLastChangedDate());
        binding.setLastUpdatedBy(src.getLastChangedBy());
        binding.setProperties(src.getProperties());
        binding.setRegistered(true);
        binding.setRoutingUUID(src.getRoutingUUID());
        binding.setServiceID(0); // TODO:
        
        // Binding state needs to be converted into pool state; until then we use
        // binding state  as pool state.
        try {
        	ConnectorStatus status = getDataService().getConnectorBindingState(src.getDeployedName());
        	switch(status) {
        	case OPEN:
        		binding.setState(org.teiid.adminapi.ConnectorBinding.STATE_OPEN);
        		break;
        	case NOT_INITIALIZED:
        		binding.setState(org.teiid.adminapi.ConnectorBinding.STATE_NOT_INITIALIZED);
        		break;
        	case CLOSED:
        		binding.setState(org.teiid.adminapi.ConnectorBinding.STATE_CLOSED);
        		break;
        	case DATA_SOURCE_UNAVAILABLE:
        		binding.setState(org.teiid.adminapi.ConnectorBinding.STATE_DATA_SOURCE_UNAVAILABLE);
        		break;
        	case INIT_FAILED:
        		binding.setState(org.teiid.adminapi.ConnectorBinding.STATE_INIT_FAILED);
        		break;
        	case UNABLE_TO_CHECK:
        		binding.setState(org.teiid.adminapi.ConnectorBinding.STATE_FAILED_TO_CHECK);
        		break;
        	}        	
        }catch(MetaMatrixComponentException e) {
            binding.setState(org.teiid.adminapi.ConnectorBinding.STATE_NOT_DEPLOYED);            
        }
        binding.setStateChangedTime(src.getLastChangedDate());
        return binding;       
    }

    /** 
     * @param type
     * @return
     * @since 4.3
     */
    private org.teiid.adminapi.ConnectorType convertConnectorType(final com.metamatrix.common.config.api.ConnectorBindingType src, final Object parent) {
        MMConnectorType type = new MMConnectorType(new String[] {src.getName()});
        type.setCreated(src.getCreatedDate());
        type.setCreatedBy(src.getCreatedBy());
        type.setEnabled(true);
        type.setLastUpdated(src.getLastChangedDate());
        type.setRegistered(true);
        
        return type;       
    }

    /** 
     * @param vdb
     * @return
     * @since 4.3
     */
    private org.teiid.adminapi.VDB convertVDB(final com.metamatrix.common.vdb.api.VDBDefn src, final Object parent) {
        
        MMVDB vdb = new MMVDB(new String[] {src.getName(), src.getVersion()});
        vdb.setCreated(src.getDateCreated());
        vdb.setCreatedBy(src.getCreatedBy());
        vdb.setEnabled(src.isActiveStatus());
        vdb.setLastUpdated(src.getDateCreated());
        vdb.setLastUpdatedBy(src.getCreatedBy());
        vdb.setMaterializedViews(src.getMatertializationModel() != null);
        vdb.setModels((Collection)convertToAdminObjects(src.getModels(), src));
        vdb.setProperties(null);
        vdb.setRegistered(true);
        vdb.setStatus(src.getStatus());
        vdb.setUID(0); // TODO: src.getUUID());
        vdb.setVersionedBy(src.getCreatedBy());
        vdb.setVersionedDate(src.getDateCreated());
        vdb.setHasWSDL(src.hasWSDLDefined());
        
        return vdb;        
    }
    
    private org.teiid.adminapi.Model convertModel(final com.metamatrix.common.vdb.api.ModelInfo src, final Object parent) {
        final com.metamatrix.common.vdb.api.VDBDefn vdb = (com.metamatrix.common.vdb.api.VDBDefn)parent;
        MMModel model = new MMModel(new String[] {src.getName()});
        model.setCreated(vdb.getDateCreated());
        model.setCreatedBy(vdb.getCreatedBy());
        model.setEnabled(vdb.isActiveStatus());
        model.setLastUpdated(vdb.getDateCreated());
        model.setLastUpdatedBy(vdb.getCreatedBy());
        model.setModelType(src.getModelTypeName());
        model.setModelURI(src.getModelURI());
        model.setMaterialization(src.isMaterialization());
        model.setPhysical(src.isPhysical());
        model.setRegistered(true);        
        model.setSupportsMultiSourceBindings(src.isMultiSourceBindingEnabled());
        model.setVisible(src.isVisible());
        if (src.isPhysical()) {
            List bindings = src.getConnectorBindingNames();
            if (bindings != null && !bindings.isEmpty()) {
                List names = new ArrayList();
                for (int i=0; i<bindings.size();i++) {
                    names.add(convertToAdminObject(vdb.getConnectorBindingByName((String)bindings.get(i)), parent));
                }
                model.setConnectorBindingNames(names);
            }
        }
        return model;
    }

    private org.teiid.adminapi.Request convertRequest(final RequestInfo src) {
        
        String connId = src.getRequestID().getConnectionID();
        
        MMRequest request = null;
        if (src.getConnectorBindingUUID() != null) {
            request = new MMRequest(new String[] {connId, String.valueOf(src.getRequestID().getExecutionID()), String.valueOf(src.getNodeID()), String.valueOf(src.getExecutionID())}); 
        }
        else {
            request = new MMRequest(new String[] {connId, String.valueOf(src.getRequestID().getExecutionID())}); 
        }
        
        request.setSqlCommand(src.getCommand());
        
        request.setCreated(src.getProcessingTimestamp());
        
        if (src.getConnectorBindingUUID() != null) {
            request.setSource(true);
            request.setNodeID(String.valueOf(src.getNodeID()));
        }
        return request;
    }
    
    /**
     * Get the connection connection object for the given id. 
     * @param identifier
     * @return
     * @since 4.3
     */
    MetaMatrixSessionInfo getClientConnection(String identifier) throws AdminException {
        Collection<MetaMatrixSessionInfo> sessions = getClientConnections();
        for (MetaMatrixSessionInfo info:sessions) {
            if (info.getSessionID().toString().equals(identifier)) {
                return info;
            }
        }
        return null;
    }

    /**
     * Get all the available connections 
     * @return
     * @throws AdminException
     */
    Collection<MetaMatrixSessionInfo> getClientConnections() throws AdminException {
        try {
			return getSessionService().getActiveSessions();
		} catch (SessionServiceException e) {
			// SessionServiceException not in the client known exception (in common-internal)
			throw new AdminComponentException(e.getMessage(), e.getCause());
		}
    }

    boolean matches(String regEx, String value) {
        regEx = regEx.replaceAll(AdminObject.ESCAPED_WILDCARD, ".*"); //$NON-NLS-1$ 
        regEx = regEx.replaceAll(AdminObject.ESCAPED_DELIMITER, ""); //$NON-NLS-1$ 
        return value.matches(regEx);
    }
    
    List matchedCollection(String identifier, List adminObjects) {
        ArrayList matched = new ArrayList();
        for (Iterator i = adminObjects.iterator(); i.hasNext();) {
            AdminObject aObj = (AdminObject)i.next();
            if (matches(identifier, aObj.getName()) || matches(identifier, aObj.getIdentifier())) {
                matched.add(aObj);
            }
        }        
        return matched;
    }
    
    /**
     * Get list of available connector bindings 
     * @param identifier
     */
    Collection getConnectorBindings(String identifier) throws AdminException{
        try {
            List connectorBindings = getDataService().getConnectorBindings();
            connectorBindings = (List)convertToAdminObjects(connectorBindings);
            return matchedCollection(identifier, connectorBindings);
        } catch (MetaMatrixComponentException e) {
            throw new AdminComponentException(e);
        }
    }
    
    
    /**
     * Get list of available connector types 
     * @param identifier
     */
    Collection getConnectorTypes(String identifier) throws AdminException{
   
        try {
            List connectorTypes = getConfigurationService().getConnectorTypes();
            connectorTypes = (List)convertToAdminObjects(connectorTypes);
            return matchedCollection(identifier, connectorTypes);
        } catch (MetaMatrixComponentException err) {
            throw new AdminComponentException(err);
        } 
    }

    /**
     * Get the system state. 
     * @return
     */
    public SystemObject getSystem() {
        MMSystem system = new MMSystem();
        system.setStartTime(new Date(manager.getStartTime()));
        system.setStarted(manager.isAlive());
        system.setProperties(manager.getProperties());
        return system;
    }

    
    
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
     * Encrypt a string 
     * @param value
     * @return
     * @throws AdminException
     * @since 4.3
     */
    String encryptString(String value) throws AdminException {
        try {
            return CryptoUtil.stringEncrypt(value);
        } catch (CryptoException e) {
            throw new AdminComponentException(e);
        }
    }
    
    
    
    /**
     * Convert a ComponentType and a set of properties into a Collection of 
     * com.metamatrix.admin.api.objects.PropertyDefinition objects
     * @param ctype
     * @param properties
     * @return
     * @since 4.3
     */
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
    
    
    /**
     * Convert a set of properties into a Collection of 
     * com.metamatrix.admin.api.objects.PropertyDefinition objects
     *  
     * @param ctype
     * @param properties
     * @return
     * @since 4.3
     */
    protected Collection convertPropertyDefinitions(Properties properties) {
        ArrayList results = new ArrayList();
        for (Iterator iter = properties.keySet().iterator(); iter.hasNext(); ) {
            String key = (String) iter.next();
            String value = properties.getProperty(key);
                        
            MMPropertyDefinition result = new MMPropertyDefinition(new String[] {key});
            result.setDisplayName(key);
            result.setValue(value);
            
            results.add(result);
        }
        
        
        return results;
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
            case MMAdminObject.OBJECT_TYPE_CONNECTOR_BINDING:
                return getConnectorBindings(identifier);
            case MMAdminObject.OBJECT_TYPE_CONNECTOR_TYPE:
                return getConnectorTypes(identifier);
            case MMAdminObject.OBJECT_TYPE_SYSTEM_OBJECT:
                list = new ArrayList();
                list.add(getSystem());
                return list;
                
            default:
                throw new AdminProcessingException(DQPEmbeddedPlugin.Util.getString("AdminImpl.Unsupported_Admin_Object", className)); //$NON-NLS-1$
                
        }
    }
    
    protected SessionToken validateSession() {
        return DQPWorkContext.getWorkContext().getSessionToken();
    }
    
}
