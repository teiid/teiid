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

package org.teiid.adminapi.jboss;

import java.io.InputStream;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import org.jboss.deployers.spi.management.ManagementView;
import org.jboss.deployers.spi.management.deploy.DeploymentManager;
import org.jboss.managed.api.ComponentType;
import org.jboss.managed.api.DeploymentTemplateInfo;
import org.jboss.managed.api.ManagedComponent;
import org.jboss.managed.api.ManagedObject;
import org.jboss.managed.api.ManagedProperty;
import org.jboss.managed.plugins.DefaultFieldsImpl;
import org.jboss.managed.plugins.WritethroughManagedPropertyImpl;
import org.jboss.metatype.api.types.CollectionMetaType;
import org.jboss.metatype.api.types.EnumMetaType;
import org.jboss.metatype.api.types.SimpleMetaType;
import org.jboss.metatype.api.values.CollectionValueSupport;
import org.jboss.metatype.api.values.MetaValue;
import org.jboss.metatype.api.values.MetaValueFactory;
import org.jboss.metatype.api.values.SimpleValue;
import org.jboss.metatype.api.values.SimpleValueSupport;
import org.jboss.profileservice.spi.NoSuchDeploymentException;
import org.jboss.profileservice.spi.ProfileKey;
import org.jboss.virtual.VFS;
import org.teiid.adminapi.AdminComponentException;
import org.teiid.adminapi.AdminException;
import org.teiid.adminapi.AdminObject;
import org.teiid.adminapi.AdminProcessingException;
import org.teiid.adminapi.CacheStatistics;
import org.teiid.adminapi.PropertyDefinition;
import org.teiid.adminapi.Request;
import org.teiid.adminapi.Session;
import org.teiid.adminapi.Transaction;
import org.teiid.adminapi.Translator;
import org.teiid.adminapi.VDB;
import org.teiid.adminapi.WorkerPoolStatistics;
import org.teiid.adminapi.VDB.ConnectionType;
import org.teiid.adminapi.impl.CacheStatisticsMetadata;
import org.teiid.adminapi.impl.PropertyDefinitionMetadata;
import org.teiid.adminapi.impl.RequestMetadata;
import org.teiid.adminapi.impl.SessionMetadata;
import org.teiid.adminapi.impl.TransactionMetadata;
import org.teiid.adminapi.impl.TranslatorMetaData;
import org.teiid.adminapi.impl.VDBMetaData;
import org.teiid.adminapi.impl.WorkerPoolStatisticsMetadata;
import org.teiid.deployers.VDBStatusChecker;
import org.teiid.jboss.IntegrationPlugin;

public class Admin implements org.teiid.adminapi.Admin, Serializable {	
	private static final String CONNECTOR_PREFIX = "connector-"; //$NON-NLS-1$
	private static final ProfileKey DEFAULT_PROFILE_KEY = new ProfileKey(ProfileKey.DEFAULT);
	private static final long serialVersionUID = 7081309086056911304L;
	private static ComponentType VDBTYPE = new ComponentType("teiid", "vdb");//$NON-NLS-1$ //$NON-NLS-2$
	private static ComponentType DQPTYPE = new ComponentType("teiid", "dqp");//$NON-NLS-1$ //$NON-NLS-2$	
	private static String DQPNAME = "RuntimeEngineDeployer"; //$NON-NLS-1$
	private static ComponentType TRANSLATOR_TYPE = new ComponentType("teiid", "translator");//$NON-NLS-1$ //$NON-NLS-2$
	
	private static final String[] DS_TYPES = {"XA", "NoTx", "LocalTx"}; //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
	private static final String[] CF_TYPES = {"NoTx", "Tx"}; //$NON-NLS-1$ //$NON-NLS-2$
	
	
	private ManagementView view;
	private DeploymentManager deploymentMgr;
	
	final private VDBStatusChecker statusChecker;
	
	static {
		VFS.init();
	}
	
	public Admin(ManagementView view, DeploymentManager deployMgr, VDBStatusChecker statusChecker) {
		this.view = view;
		this.statusChecker = statusChecker;
		this.view.load();
				
		this.deploymentMgr =  deployMgr;
        try {
        	this.deploymentMgr.loadProfile(DEFAULT_PROFILE_KEY);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
	}
	
	private ManagementView getView() throws AdminProcessingException {
		if (this.view == null) {
			throw new AdminProcessingException(IntegrationPlugin.Util.getString("admin_connection_closed")); //$NON-NLS-1$
		}
		this.view.load();
		return this.view;
	}
	
	private DeploymentManager getDeploymentManager() throws AdminProcessingException{
		if (this.deploymentMgr == null) {
			throw new AdminProcessingException(IntegrationPlugin.Util.getString("admin_connection_closed")); //$NON-NLS-1$
		}
		return this.deploymentMgr;
	}
	
	public void close() {
		this.view = null;
		this.deploymentMgr = null;
	}	
	
//	private DQPManagement getDQPManagement() throws Exception {
//		final ManagedComponent mc = getView().getComponent(DQPManagementView.class.getName(), DQPTYPE);	
//		
//		return (DQPManagement)Proxy.newProxyInstance(this.getClass().getClassLoader(), new Class[] {DQPManagement.class}, new InvocationHandler() {
//			@Override
//			public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
//				
//				MetaValue value = ManagedUtil.executeOperation(mc, method.getName());
//				Class returnType = method.getReturnType();
//				if (returnType.equals(Void.class)) {
//					return value;
//				}
//				return null;
//			}
//		});
//	}
	
	@Override
	public Collection<Translator> getTranslators() throws AdminException {
		ArrayList<Translator> factories = new ArrayList<Translator>();
		try {
			Set<ManagedComponent> mcSet = getView().getComponentsForType(TRANSLATOR_TYPE);
			for (ManagedComponent mc:mcSet) {
				factories.add(AdminObjectBuilder.buildAO(mc, TranslatorMetaData.class));
			}
		} catch (Exception e) {
			throw new AdminComponentException(e);
		}
		return factories;
	}

	@Override
	public Translator getTranslator(String deployedName) throws AdminException {
		try {
			ManagedComponent mc = getView().getComponent(deployedName, TRANSLATOR_TYPE);
			if (mc != null) {
				return AdminObjectBuilder.buildAO(mc, TranslatorMetaData.class);
			}
			return null;
		} catch(Exception e) {
			throw new AdminProcessingException(e.getMessage(), e);
		}
	}

    boolean matches(String regEx, String value) {
        regEx = regEx.replaceAll(AdminObject.ESCAPED_WILDCARD, ".*"); //$NON-NLS-1$ 
        regEx = regEx.replaceAll(AdminObject.ESCAPED_DELIMITER, ""); //$NON-NLS-1$ 
        return value.matches(regEx);
    }	
    
	@Override
	public void deployVDB(String fileName, InputStream vdb) throws AdminException {
		if (!fileName.endsWith(".vdb") && !fileName.endsWith("-vdb.xml")) {//$NON-NLS-1$ //$NON-NLS-2$
			throw new AdminProcessingException(IntegrationPlugin.Util.getString("bad_vdb_extension")); //$NON-NLS-1$
		}
		ManagedUtil.deployArchive(getDeploymentManager(), fileName, vdb, false);
	}

	
	@Override
	public void deleteVDB(String vdbName, int vdbVersion) throws AdminException {
		ManagedComponent mc = getVDBManagedComponent(vdbName, vdbVersion);
		if (mc != null) {
			ManagedUtil.removeArchive(getDeploymentManager(), mc.getDeployment().getName());
		}
	}	
	
	@Override
	public VDB getVDB(String vdbName, int vdbVersion) throws AdminException{
		ManagedComponent mc = getVDBManagedComponent(vdbName, vdbVersion);
		if (mc != null) {
			return AdminObjectBuilder.buildAO(mc, VDBMetaData.class);
		}
		return null;
	}
	
	private ManagedComponent getVDBManagedComponent(String vdbName, int vdbVersion) throws AdminException{
		try {
			Set<ManagedComponent> vdbComponents = getView().getComponentsForType(VDBTYPE);
			for (ManagedComponent mc: vdbComponents) {
				String name = ManagedUtil.getSimpleValue(mc, "name", String.class);//$NON-NLS-1$
			    int version = ManagedUtil.getSimpleValue(mc, "version", Integer.class);//$NON-NLS-1$
			    if (name.equalsIgnoreCase(vdbName) && version == vdbVersion) {
			    	return mc;
			    }
			}
			return null;
		} catch (Exception e) {
			throw new AdminComponentException(e.getMessage(), e);
		}	
	}
	
	@Override
	public Set<VDB> getVDBs() throws AdminException {
		try {
			Set<VDB> vdbs = new HashSet<VDB>();
			Set<ManagedComponent> vdbComponents = getView().getComponentsForType(VDBTYPE);
			for (ManagedComponent mc: vdbComponents) {
				vdbs.add(AdminObjectBuilder.buildAO(mc, VDBMetaData.class));
			}
			return vdbs;
		} catch (Exception e) {
			throw new AdminComponentException(e.getMessage(), e);
		}
	}
		
	@Override
	public Collection<Session> getSessions() throws AdminException {
		try {
			Collection<Session> sessionList = new ArrayList<Session>();
			ManagedComponent mc = getView().getComponent(DQPNAME, DQPTYPE);	
			MetaValue value = ManagedUtil.executeOperation(mc, "getActiveSessions");//$NON-NLS-1$
			MetaValue[] sessions = ((CollectionValueSupport)value).getElements();
			for (MetaValue mv:sessions) {
				sessionList.add((SessionMetadata)MetaValueFactory.getInstance().unwrap(mv, SessionMetadata.class));
			}
			return sessionList;
		} catch (Exception e) {
			throw new AdminComponentException(e.getMessage(), e);
		}		
	}
	
	@Override
	public void terminateSession(String sessionId) throws AdminException {
		try {
			ManagedComponent mc = getView().getComponent(DQPNAME, DQPTYPE);	
			ManagedUtil.executeOperation(mc, "terminateSession", SimpleValueSupport.wrap(sessionId));//$NON-NLS-1$
		} catch (Exception e) {
			throw new AdminComponentException(e.getMessage(), e);
		}     	
	}	
	
	@Override
    public Collection<Request> getRequests() throws AdminException {
		try {
			Collection<Request> requestList = new ArrayList<Request>();
			ManagedComponent mc = getView().getComponent(DQPNAME, DQPTYPE);	
			MetaValue value = ManagedUtil.executeOperation(mc, "getRequests");//$NON-NLS-1$
			MetaValue[] requests = ((CollectionValueSupport)value).getElements();			
			for (MetaValue mv:requests) {
				requestList.add((RequestMetadata)MetaValueFactory.getInstance().unwrap(mv, RequestMetadata.class));
			}
			return requestList;
		} catch (Exception e) {
			throw new AdminComponentException(e.getMessage(), e);
		}    	
    }
    
	@Override
    public Collection<Request> getRequestsForSession(String sessionId) throws AdminException {
		try {
			Collection<Request> requestList = new ArrayList<Request>();
			ManagedComponent mc = getView().getComponent(DQPNAME, DQPTYPE);	
			MetaValue value = ManagedUtil.executeOperation(mc, "getRequestsForSession", SimpleValueSupport.wrap(sessionId));//$NON-NLS-1$
			MetaValue[] requests = ((CollectionValueSupport)value).getElements();
			for (MetaValue mv:requests) {
				requestList.add((RequestMetadata)MetaValueFactory.getInstance().unwrap(mv, RequestMetadata.class));
			}
			return requestList;
		} catch (Exception e) {
			throw new AdminComponentException(e.getMessage(), e);
		}     	
    }
	
	@Override
	public void cancelRequest(String sessionId, long executionId) throws AdminException{
		try {
			ManagedComponent mc = getView().getComponent(DQPNAME, DQPTYPE);	
			ManagedUtil.executeOperation(mc, "cancelRequest", SimpleValueSupport.wrap(sessionId), SimpleValueSupport.wrap(executionId));//$NON-NLS-1$
		} catch (Exception e) {
			throw new AdminComponentException(e.getMessage(), e);
		}     	
	}
	
	@Override
	public Collection<String> getCacheTypes() throws AdminException {
		try {
			Collection<String> requestList = new ArrayList<String>();
			ManagedComponent mc = getView().getComponent(DQPNAME, DQPTYPE);	
			MetaValue value = ManagedUtil.executeOperation(mc, "getCacheTypes");//$NON-NLS-1$
			MetaValue[] requests = ((CollectionValueSupport)value).getElements();
			for (MetaValue mv:requests) {
				requestList.add(ManagedUtil.stringValue(mv));
			}
			return requestList;
		} catch (Exception e) {
			throw new AdminComponentException(e.getMessage(), e);
		} 
	}	
	
	@Override
	public void clearCache(String cacheType) throws AdminException{
		try {
			ManagedComponent mc = getView().getComponent(DQPNAME, DQPTYPE);	
			ManagedUtil.executeOperation(mc, "clearCache", SimpleValueSupport.wrap(cacheType));//$NON-NLS-1$
		} catch (Exception e) {
			throw new AdminComponentException(e.getMessage(), e);
		} 		
	}
	
	@Override
	public void clearCache(String cacheType, String vdbName, int version) throws AdminException{
		try {
			ManagedComponent mc = getView().getComponent(DQPNAME, DQPTYPE);	
			ManagedUtil.executeOperation(mc, "clearCache", SimpleValueSupport.wrap(cacheType), //$NON-NLS-1$
					SimpleValueSupport.wrap(vdbName), SimpleValueSupport.wrap(version));
		} catch (Exception e) {
			throw new AdminComponentException(e.getMessage(), e);
		} 		
	}
	
	@Override
	public Collection<Transaction> getTransactions() throws AdminException {
		try {
			Collection<Transaction> txnList = new ArrayList<Transaction>();
			ManagedComponent mc = getView().getComponent(DQPNAME, DQPTYPE);	
			MetaValue value = ManagedUtil.executeOperation(mc, "getTransactions");//$NON-NLS-1$
			MetaValue[] requests = ((CollectionValueSupport)value).getElements();
			for (MetaValue mv:requests) {
				txnList.add((TransactionMetadata)MetaValueFactory.getInstance().unwrap(mv, TransactionMetadata.class));
			}
			return txnList;
		} catch (Exception e) {
			throw new AdminComponentException(e.getMessage(), e);
		}  
	}	
	
	@Override
	public void terminateTransaction(String xid) throws AdminException {
		try {
			ManagedComponent mc = getView().getComponent(DQPNAME, DQPTYPE);	
			ManagedUtil.executeOperation(mc, "terminateTransaction", MetaValueFactory.getInstance().create(xid));//$NON-NLS-1$
		} catch (Exception e) {
			throw new AdminComponentException(e.getMessage(), e);
		} 	
	}
	
	@Override
	public WorkerPoolStatistics getWorkerPoolStats() throws AdminException {
		try {
			ManagedComponent mc = getView().getComponent(DQPNAME, DQPTYPE);
			MetaValue value = ManagedUtil.executeOperation(mc, "getWorkerPoolStatistics");//$NON-NLS-1$
			return (WorkerPoolStatistics)MetaValueFactory.getInstance().unwrap(value, WorkerPoolStatisticsMetadata.class);	
		} catch (Exception e) {
			throw new AdminComponentException(e.getMessage(), e);
		}
	}	
	
	
	@Override
	public Collection<PropertyDefinition> getTemplatePropertyDefinitions(String templateName) throws AdminException {
		
		DeploymentTemplateInfo info = null;
		
		try {
			
			try {
				info = getView().getTemplate(templateName);
			} catch (Exception e) {
				// ignore..
			}
			
			if (info == null && !templateName.startsWith(TranslatorMetaData.TRANSLATOR_PREFIX)) {
				info = getView().getTemplate(TranslatorMetaData.TRANSLATOR_PREFIX+templateName);
			}
			if(info == null) {
				throw new AdminProcessingException(IntegrationPlugin.Util.getString("template_not_found", templateName)); //$NON-NLS-1$
			}
			
			ArrayList<PropertyDefinition> props = new ArrayList<PropertyDefinition>();
			Map<String, ManagedProperty> propertyMap = info.getProperties();
			
			for (ManagedProperty mp:propertyMap.values()) {
					if (!includeInTemplate(mp)) {
						continue;
					}
					PropertyDefinitionMetadata p = new PropertyDefinitionMetadata();
					p.setName(mp.getName());
					p.setDescription(mp.getDescription());
					p.setDisplayName(mp.getMappedName());
					if (mp.getDefaultValue() != null) {
						p.setDefaultValue(((SimpleValueSupport)mp.getDefaultValue()).getValue());
					}
					p.setPropertyTypeClassName(mp.getMetaType().getTypeName());
					p.setModifiable(!mp.isReadOnly());
					
					if (mp.getField("masked", Boolean.class) != null) {//$NON-NLS-1$
						p.setMasked(mp.getField("masked", Boolean.class));//$NON-NLS-1$
					}
					else {
						p.setMasked(false);
					}
					
					if (mp.getField("advanced", Boolean.class) != null) {//$NON-NLS-1$
						p.setAdvanced(mp.getField("advanced", Boolean.class));//$NON-NLS-1$
					}
					else {
						p.setAdvanced(true);
					}
					if (mp.getLegalValues() != null) {
						HashSet<String> values = new HashSet<String>();
						for (MetaValue value:mp.getLegalValues()) {
							values.add(ManagedUtil.stringValue(value));
						}
						p.setAllowedValues(values);
					}
					
					p.setRequired(mp.isMandatory());
					props.add(p);
			};
			return props;
		} catch (NoSuchDeploymentException e) {
			throw new AdminComponentException(e.getMessage(), e);
		} catch(Exception e) {
			throw new AdminComponentException(e.getMessage(), e);
		}
	}	
	
    private boolean includeInTemplate(ManagedProperty mp) {
    	Boolean teiidProperty = mp.getField("teiid-property", Boolean.class);//$NON-NLS-1$
		if ( teiidProperty != null && teiidProperty.booleanValue()) {
			return true;
		}
		if (mp.isMandatory() && mp.getDefaultValue() == null) {
			return true;
		}
		return false;
	}
    
    @Override
    public void changeVDBConnectionType(String vdbName, int vdbVersion,
    		ConnectionType type) throws AdminException {
    	ManagedComponent mc = getVDBManagedComponent(vdbName, vdbVersion);
		if (mc == null) {
			throw new AdminProcessingException(IntegrationPlugin.Util.getString("vdb_not_found", vdbName, vdbVersion)); //$NON-NLS-1$
		}
		
    	ManagedProperty connectionTypeProperty = mc.getProperty("connectionType"); //$NON-NLS-1$
    	if (connectionTypeProperty != null) {
    		connectionTypeProperty.setValue(ManagedUtil.wrap(new EnumMetaType(ConnectionType.values()), type != null ?type.name():ConnectionType.BY_VERSION.name()));
    	}
		
		try {
			getView().updateComponent(mc);
		} catch (Exception e) {
			throw new AdminComponentException(e.getMessage(), e);
		}
    }

	@Override
	public void assignToModel(String vdbName, int vdbVersion, String modelName, String sourceName, String translatorName, String dsName) throws AdminException {
		ManagedComponent mc = getVDBManagedComponent(vdbName, vdbVersion);
		if (mc == null) {
			throw new AdminProcessingException(IntegrationPlugin.Util.getString("vdb_not_found", vdbName, vdbVersion)); //$NON-NLS-1$
		}
		
		ManagedProperty mp = mc.getProperty("models");//$NON-NLS-1$
		List<ManagedObject> models = (List<ManagedObject>)MetaValueFactory.getInstance().unwrap(mp.getValue());
		ManagedObject managedModel = null;
		if (models != null && !models.isEmpty()) {
			for(ManagedObject mo:models) {
				String name = ManagedUtil.getSimpleValue(mo, "name", String.class); //$NON-NLS-1$
				if (modelName.equals(name)) {
					managedModel = mo;
				}
			}		
		}
		
		if (managedModel == null) {
			throw new AdminProcessingException(IntegrationPlugin.Util.getString("model_not_found", modelName, vdbName, vdbVersion)); //$NON-NLS-1$
		}
		
        ManagedProperty sourceMappings = managedModel.getProperty("sourceMappings");//$NON-NLS-1$
        if (sourceMappings != null){
            List<ManagedObject> mappings = (List<ManagedObject>)MetaValueFactory.getInstance().unwrap(sourceMappings.getValue());
            for (ManagedObject mo:mappings) {
                String sName = ManagedUtil.getSimpleValue(mo, "name", String.class);//$NON-NLS-1$
                if (sName.equals(sourceName)) {
                	
                	ManagedProperty translatorProperty = mo.getProperty("translatorName"); //$NON-NLS-1$
                	if (translatorProperty == null) {
                		translatorProperty = new WritethroughManagedPropertyImpl(mo, new DefaultFieldsImpl("translatorName")); //$NON-NLS-1$
                	}
                	translatorProperty.setValue(ManagedUtil.wrap(SimpleMetaType.STRING, translatorName));
                	
                	// set the jndi name for the ds.
                	ManagedProperty jndiProperty = mo.getProperty("connectionJndiName"); //$NON-NLS-1$
                	if (jndiProperty == null) {
                		jndiProperty = new WritethroughManagedPropertyImpl(mo, new DefaultFieldsImpl("connectionJndiName")); //$NON-NLS-1$
                	}
                	jndiProperty.setValue(ManagedUtil.wrap(SimpleMetaType.STRING, dsName));
                }
            }
        } else {
        	//TODO: this can be in the default situation when no source mappings are specified
        	throw new AdminProcessingException(IntegrationPlugin.Util.getString("sourcename_not_found", sourceName, vdbName, vdbVersion, modelName)); //$NON-NLS-1$
        }
        
		try {
			getView().updateComponent(mc);
		} catch (Exception e) {
			throw new AdminComponentException(e.getMessage(), e);
		}		        
	}

	private void manageRoleToDataPolicy(String vdbName, int vdbVersion, String policyName, String role, boolean add)  throws AdminException {
		ManagedComponent mc = getVDBManagedComponent(vdbName, vdbVersion);
		if (mc == null) {
			throw new AdminProcessingException(IntegrationPlugin.Util.getString("vdb_not_found", vdbName, vdbVersion)); //$NON-NLS-1$
		}
		
		ManagedProperty mp = mc.getProperty("dataPolicies");//$NON-NLS-1$
		List<ManagedObject> policies = (List<ManagedObject>)MetaValueFactory.getInstance().unwrap(mp.getValue());
		ManagedObject managedPolicy = null;
		if (policies != null && !policies.isEmpty()) {
			for(ManagedObject mo:policies) {
				String name = ManagedUtil.getSimpleValue(mo, "name", String.class); //$NON-NLS-1$
				if (policyName.equals(name)) {
					managedPolicy = mo;
				}
			}		
		}
		
		if (managedPolicy == null) {
			throw new AdminProcessingException(IntegrationPlugin.Util.getString("policy_not_found", policyName, vdbName, vdbVersion)); //$NON-NLS-1$
		}
		
		if (role != null) {
	        ManagedProperty mappedRoleNames = managedPolicy.getProperty("mappedRoleNames");//$NON-NLS-1$
	        CollectionValueSupport roleCollection = (CollectionValueSupport)mappedRoleNames.getValue();
	        ArrayList<MetaValue> modifiedRoleNames = new ArrayList<MetaValue>();
	        if (roleCollection != null) {
		        MetaValue[] roleNames = roleCollection.getElements();
		        for (MetaValue mv:roleNames) {
		        	String existing = (String)((SimpleValueSupport)mv).getValue();
		        	if (!existing.equals(role)) {
		        		modifiedRoleNames.add(mv);
		        	}
		        }
	        }
	        else {
	        	roleCollection = new CollectionValueSupport(new CollectionMetaType("java.util.List", SimpleMetaType.STRING)); //$NON-NLS-1$
	        	mappedRoleNames.setValue(roleCollection);
	        }
	        
	        if (add) {
	        	modifiedRoleNames.add(ManagedUtil.wrap(SimpleMetaType.STRING, role));
	        }
	        
	        roleCollection.setElements(modifiedRoleNames.toArray(new MetaValue[modifiedRoleNames.size()]));
		} else {
			ManagedProperty anyAuthenticated = managedPolicy.getProperty("anyAuthenticated");//$NON-NLS-1$
			anyAuthenticated.setValue(SimpleValueSupport.wrap(add));
		}
		
		try {
			getView().updateComponent(mc);
		} catch (Exception e) {
			throw new AdminComponentException(e.getMessage(), e);
		}		
	}

	
	@Override
	public void addDataRoleMapping(String vdbName, int vdbVersion, String policyName, String role)  throws AdminException {
		manageRoleToDataPolicy(vdbName, vdbVersion, policyName, role, true);
	}
	
	@Override
	public void removeDataRoleMapping(String vdbName, int vdbVersion, String policyName, String role)  throws AdminException{
		manageRoleToDataPolicy(vdbName, vdbVersion, policyName, role, false);
	}	
	
	@Override
	public void setAnyAuthenticatedForDataRole(String vdbName, int vdbVersion,
			String dataRole, boolean anyAuthenticated) throws AdminException {
		manageRoleToDataPolicy(vdbName, vdbVersion, dataRole, null, anyAuthenticated);
	}

	@Override
	public void mergeVDBs(String sourceVDBName, int sourceVDBVersion, String targetVDBName, int targetVDBVersion) throws AdminException {
		try {
			ManagedComponent mc = getView().getComponent(DQPNAME, DQPTYPE);	
			ManagedUtil.executeOperation(mc, "mergeVDBs",  //$NON-NLS-1$
					SimpleValueSupport.wrap(sourceVDBName), 
					SimpleValueSupport.wrap(sourceVDBVersion), 
					SimpleValueSupport.wrap(targetVDBName), 
					SimpleValueSupport.wrap(targetVDBVersion));
		} catch (Exception e) {
			throw new AdminComponentException(e.getMessage(), e);
		}   		
	}

	private ManagedComponent getDatasource(String deployedName) throws Exception {
		ManagedComponent mc = null;
		for (String type:DS_TYPES) {
			ComponentType ct = new ComponentType("DataSource", type); //$NON-NLS-1$
			mc = getView().getComponent(deployedName, ct);
			if (mc != null) {
				return mc;
			}				
		}		
		for (String type:CF_TYPES) {
			ComponentType ct = new ComponentType("ConnectionFactory", type); //$NON-NLS-1$
			mc = getView().getComponent(deployedName, ct);
			if (mc != null) {
				return mc;
			}				
		}
		return mc;
	}
	
	
	@Override
	public void createDataSource(String deploymentName, String templateName, Properties properties) throws AdminException {
		try {
			ManagedComponent mc = getDatasource(deploymentName);
			if (mc != null) {
				throw new AdminProcessingException(IntegrationPlugin.Util.getString("datasource_exists",deploymentName)); //$NON-NLS-1$;	
			}
			
			DeploymentTemplateInfo info = getView().getTemplate(templateName);
			if(info == null) {
				throw new AdminProcessingException(IntegrationPlugin.Util.getString("datasource_template_not_found", templateName)); //$NON-NLS-1$
			}
			
			// template properties specific to the template
			Map<String, ManagedProperty> propertyMap = info.getProperties();
			
			// walk through the supplied properties and assign properly to template
			for (String key:properties.stringPropertyNames()) {
				ManagedProperty mp = propertyMap.get(key);
				if (mp != null) {
					String value = properties.getProperty(key);
					if (!ManagedUtil.sameValue(mp.getDefaultValue(), value)){
						mp.setValue(SimpleValueSupport.wrap(value));
					}
				}
			}
			info.getProperties().get("jndi-name").setValue(SimpleValueSupport.wrap(deploymentName)); //$NON-NLS-1$
			getView().applyTemplate(deploymentName, info);
		} catch (NoSuchDeploymentException e) {
			throw new AdminComponentException(e.getMessage(), e);
		} catch(Exception e) {
			throw new AdminComponentException(e.getMessage(), e);
		} 
	}

	@Override
	public void deleteDataSource(String deployedName) throws AdminException {
		try {
			ManagedComponent mc = getDatasource(deployedName);
			if (mc != null) {
				ManagedUtil.removeArchive(getDeploymentManager(),mc.getDeployment().getName());
			}
		} catch (Exception e) {
			throw new AdminComponentException(e);
		}
	}

	@Override
	public Collection<String> getDataSourceNames() throws AdminException {
		ArrayList<String> names = new ArrayList<String>();
		try {
			for (String type:DS_TYPES) {
				ComponentType ct = new ComponentType("DataSource", type); //$NON-NLS-1$
				Set<ManagedComponent> mcs = getView().getComponentsForType(ct);
				for (ManagedComponent mc:mcs) {
					names.add(((SimpleValue)mc.getProperty("jndi-name").getValue()).getValue().toString()); //$NON-NLS-1$
				}
			}		
			for (String type:CF_TYPES) {
				ComponentType ct = new ComponentType("ConnectionFactory", type); //$NON-NLS-1$
				Set<ManagedComponent> mcs = getView().getComponentsForType(ct);
				for (ManagedComponent mc:mcs) {
					names.add(((SimpleValue)mc.getProperty("jndi-name").getValue()).getValue().toString()); //$NON-NLS-1$
				}			
			}
		} catch (Exception e) {
			throw new AdminComponentException(e);
		}
		return names;
	}
	
	@Override
	public Set<String> getDataSourceTemplateNames() throws AdminException{
		Set<String> names = getView().getTemplateNames();
		HashSet<String> matched = new HashSet<String>();
		for(String name:names) {
			if (name.startsWith(CONNECTOR_PREFIX)) {
				matched.add(name);
			}
		}
		return matched;		
	}

	@Override
	public CacheStatistics getCacheStats(String cacheType) throws AdminException {
		try {
			ManagedComponent mc = getView().getComponent(DQPNAME, DQPTYPE);
			MetaValue value = ManagedUtil.executeOperation(mc, "getCacheStatistics", SimpleValueSupport.wrap(cacheType));//$NON-NLS-1$
			return (CacheStatistics)MetaValueFactory.getInstance().unwrap(value, CacheStatisticsMetadata.class);	
		} catch (Exception e) {
			throw new AdminComponentException(e.getMessage(), e);
		}
	}
	
	@Override
	public void markDataSourceAvailable(String name) throws AdminException {
		statusChecker.dataSourceAdded(name);
	}
}
