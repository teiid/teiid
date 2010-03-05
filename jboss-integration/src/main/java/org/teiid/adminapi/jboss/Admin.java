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

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.jar.JarEntry;
import java.util.jar.JarOutputStream;

import org.jboss.deployers.spi.management.ManagementView;
import org.jboss.deployers.spi.management.deploy.DeploymentManager;
import org.jboss.logging.Logger;
import org.jboss.managed.api.ComponentType;
import org.jboss.managed.api.DeploymentTemplateInfo;
import org.jboss.managed.api.ManagedComponent;
import org.jboss.managed.api.ManagedObject;
import org.jboss.managed.api.ManagedProperty;
import org.jboss.metatype.api.types.MapCompositeMetaType;
import org.jboss.metatype.api.values.CollectionValueSupport;
import org.jboss.metatype.api.values.MapCompositeValueSupport;
import org.jboss.metatype.api.values.MetaValue;
import org.jboss.metatype.api.values.MetaValueFactory;
import org.jboss.metatype.api.values.SimpleValueSupport;
import org.jboss.profileservice.spi.NoSuchDeploymentException;
import org.jboss.profileservice.spi.ProfileKey;
import org.jboss.virtual.VFS;
import org.teiid.adminapi.AdminComponentException;
import org.teiid.adminapi.AdminException;
import org.teiid.adminapi.AdminObject;
import org.teiid.adminapi.AdminProcessingException;
import org.teiid.adminapi.ConnectionPoolStatistics;
import org.teiid.adminapi.ConnectorBinding;
import org.teiid.adminapi.Model;
import org.teiid.adminapi.PropertyDefinition;
import org.teiid.adminapi.Request;
import org.teiid.adminapi.Session;
import org.teiid.adminapi.TeiidAdmin;
import org.teiid.adminapi.Transaction;
import org.teiid.adminapi.VDB;
import org.teiid.adminapi.WorkerPoolStatistics;
import org.teiid.adminapi.impl.ConnectionPoolStatisticsMetadata;
import org.teiid.adminapi.impl.ConnectorBindingMetaData;
import org.teiid.adminapi.impl.ModelMetaData;
import org.teiid.adminapi.impl.PropertyDefinitionMetadata;
import org.teiid.adminapi.impl.RequestMetadata;
import org.teiid.adminapi.impl.SessionMetadata;
import org.teiid.adminapi.impl.TransactionMetadata;
import org.teiid.adminapi.impl.VDBMetaData;
import org.teiid.adminapi.impl.ModelMetaData.SourceMapping;
import org.teiid.adminapi.impl.ModelMetaData.ValidationError;
import org.teiid.jboss.deployers.RuntimeEngineDeployer;

public class Admin extends TeiidAdmin {
	protected Logger log = Logger.getLogger(getClass());
	private static final ProfileKey DEFAULT_PROFILE_KEY = new ProfileKey(ProfileKey.DEFAULT);
	
	private static final String XA_DATA_SOURCE_TEMPLATE = "XADataSourceTemplate";
	private static final long serialVersionUID = 7081309086056911304L;
	private static ComponentType VDBTYPE = new ComponentType("teiid", "vdb");
	private static ComponentType NOTXTYPE = new ComponentType("ConnectionFactory", "NoTx");
	private static ComponentType TXTYPE = new ComponentType("ConnectionFactory", "Tx");
	private static ComponentType DQPTYPE = new ComponentType("teiid", "dqp");
	private static ComponentType DSTYPE = new ComponentType("DataSource", "XA");
	private static String DQPNAME = RuntimeEngineDeployer.class.getName();
	
	private ManagementView view;
	private DeploymentManager deploymentMgr;
	
	static {
		VFS.init();
	}
	
	public Admin(ManagementView view, DeploymentManager deployMgr) {
		this.view = view;
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
			throw new AdminProcessingException("The admin connection is already closed");
		}
		this.view.load();
		return this.view;
	}
	
	private DeploymentManager getDeploymentManager() throws AdminProcessingException{
		if (this.deploymentMgr == null) {
			throw new AdminProcessingException("The admin connection is already closed");
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
	public Collection<ConnectorBinding> getConnectorBindings() throws AdminException {
		ArrayList<ConnectorBinding> bindings = new ArrayList<ConnectorBinding>();
		findConnectorBindings(bindings, "NoTx");			
		findConnectorBindings(bindings, "Tx");
		return bindings;
	}

	@Override
	public ConnectorBinding getConnectorBinding(String deployedName) throws AdminException {
		ManagedComponent mc = getConnectorBindingComponent(deployedName);
		if (mc != null) {
			return buildConnectorBinding(mc);
		}
		return null;
	}

	@Override
	public InputStream exportConnectorBinding(String deployedName) throws AdminException {
		ManagedComponent mc = getConnectorBindingComponent(deployedName);
		if (mc != null) {
			return exportDeployment(mc.getDeployment().getName());
		}
		return null;
	}

	private InputStream exportDeployment(String url) throws AdminComponentException {
		try {
			URL contentURL = new URL(url);
			return contentURL.openStream();
		} catch (MalformedURLException e) {
			throw new AdminComponentException(e);
		} catch (IOException e) {
			throw new AdminComponentException(e);
		}
	}
	
	private ManagedComponent getConnectorBindingComponent(String deployedName) throws AdminProcessingException {
		try {
			if (deployedName.startsWith("java:")) {
				deployedName = deployedName.substring(5);
			}
			ManagementView view = getView();
			ManagedComponent mc = view.getComponent(deployedName, NOTXTYPE);
			if (mc != null) {
				if (isConnectorBinding(mc)) {
					return mc;	
				}
			}
	
			mc = view.getComponent(deployedName, TXTYPE);
			if (mc != null) {
				if (isConnectorBinding(mc)) {
					return mc;	
				}
			}			
		} catch(Exception e) {
			throw new AdminProcessingException(e.getMessage(), e);
		}
		return null;
	}
	
	private ConnectorBinding buildConnectorBinding(ManagedComponent mc) {
		ConnectorBindingMetaData connector = new ConnectorBindingMetaData();
		connector.setName(mc.getName());
		connector.setComponentType(mc.getType());
		connector.addProperty("deployer-name", mc.getDeployment().getName());
		
		for (String key:mc.getProperties().keySet()) {
			ManagedProperty property = mc.getProperty(key);
			MetaValue value = property.getValue();
			
			//TODO: All properties need to be added
			if (value != null) {
				if(value.getMetaType().isSimple()) {
					connector.addProperty(key, ManagedUtil.stringValue(value));
				}
				else if (key.equals("config-property")) {
					MapCompositeValueSupport v1 = (MapCompositeValueSupport)value;
					MapCompositeMetaType metaType = v1.getMetaType();
					for (String configProperty:metaType.keySet()) {
						if (!configProperty.endsWith(".type")) {
							connector.addProperty(configProperty, ManagedUtil.stringValue(v1.get(configProperty)));
						}
					}
				}
				else {
					log.info(key+" property is not added to connector properties");
				}
			}
		}
		return connector;
	}
	
	private boolean isConnectorBinding(ManagedComponent mc) {
	    String connectionDefinition = ManagedUtil.getSimpleValue(mc, "connection-definition", String.class);
	    return "org.teiid.connector.api.Connector".equals(connectionDefinition);
	}
	
	private void findConnectorBindings(ArrayList<ConnectorBinding> bindings, String subType) throws AdminException {
		try {
			ComponentType type = new ComponentType("ConnectionFactory", subType);
			Set<ManagedComponent> jcaConnectors = getView().getComponentsForType(type);
			
			for(ManagedComponent mc:jcaConnectors) {
			    ManagedProperty mp = mc.getProperty("connection-definition");
				SimpleValueSupport v = (SimpleValueSupport)mp.getValue();
				if (v.getValue().equals("org.teiid.connector.api.Connector")){
					bindings.add(buildConnectorBinding(mc));
			    }
			}
		}catch(Exception e) {
			throw new AdminComponentException(e);
		}
	}
	
	@Override
	public void addConnectorBinding(String deploymentName, String typeName, Properties properties) throws AdminException {
		if (getConnectorBinding(deploymentName) != null) {
			throw new AdminProcessingException("Connector binding with name "+deploymentName+" already exists.");
		}
		properties.setProperty("connection-definition", "org.teiid.connector.api.Connector");
		addConnectionfactory(deploymentName, typeName, properties);
	}
	
	@Override
	public void setConnectorBindingProperty(String deployedName, String propertyName, String propertyValue) throws AdminException{
		ManagedComponent mc = getConnectorBindingComponent(deployedName);
		if (mc == null) {
			throw new AdminProcessingException("Connector binding with name "+deployedName+" does not exist.");
		}
		if (mc.getProperty(propertyName) != null) {
			mc.getProperty(propertyName).setValue(SimpleValueSupport.wrap(propertyValue));
		}
		else {
			Map<String, String> configProps = new HashMap<String, String>();
			configProps.put(propertyName, propertyValue);
			configProps.put(propertyValue+".type", "java.lang.String");
			MetaValue metaValue = ManagedUtil.compositeValueMap(configProps);
			mc.getProperty("config-property").setValue(metaValue);
		}
		try {
			getView().updateComponent(mc);
			getView().load();
		} catch (Exception e) {
			throw new AdminComponentException(e);
		}
	}
		
	@Override
	public void deleteConnectorBinding(String deployedName) throws AdminException {
		ManagedComponent mc = getConnectorBindingComponent(deployedName);
		if (mc != null) {
			ManagedUtil.removeArchive(getDeploymentManager(),mc.getDeployment().getName());
		}
	}
	
	@Override
	public void startConnectorBinding(ConnectorBinding binding) throws AdminException {
		try {
			String deployerName = binding.getPropertyValue("deployer-name");
			if (deployerName == null) {
				throw new AdminProcessingException("Failed to find deployer name of the connector. Can not start!");
			}
			ManagedUtil.execute(getDeploymentManager().start(deployerName), "Failed to start Connector Binding = " + binding.getName());
		} catch (Exception e) {
			ManagedUtil.handleException(e);
		}
	}

	@Override
	public void stopConnectorBinding(ConnectorBinding binding) throws AdminException {
		try {
			String deployerName = binding.getPropertyValue("deployer-name");
			if (deployerName == null) {
				throw new AdminProcessingException("Failed to find deployer name of the connector. Can not stop!");
			}			
			ManagedUtil.execute(getDeploymentManager().stop(deployerName), "Failed to Stop Connector Binding = " + binding.getName());
		} catch (Exception e) {
			ManagedUtil.handleException(e);
		}
	}	
	
	@Override
	public Collection<ConnectorBinding> getConnectorBindingsInVDB(String vdbName, int vdbVersion) throws AdminException {
		HashMap<String, ConnectorBinding> bindingMap = new HashMap<String, ConnectorBinding>();
		VDBMetaData vdb = (VDBMetaData) getVDB(vdbName, vdbVersion);
		if (vdb != null) {
			for (Model model:vdb.getModels()) {
				if (model.isSource()) {
					for (String sourceName : model.getSourceNames()) {
						ConnectorBinding binding = getConnectorBinding(((ModelMetaData)model).getSourceJndiName(sourceName));
						if (binding != null) {
							bindingMap.put(sourceName, binding);
						}
					}
				}
			}
		}
		return new ArrayList(bindingMap.values());
	}
	
	
	@Override
	public Set<String> getConnectorTypes() throws AdminException{
		Set<String> names = getView().getTemplateNames();
		HashSet<String> matched = new HashSet<String>();
		for(String name:names) {
			if (name.startsWith("connector-")) {
				matched.add(name);
			}
		}
		return matched;
	}
	
    boolean matches(String regEx, String value) {
        regEx = regEx.replaceAll(AdminObject.ESCAPED_WILDCARD, ".*"); //$NON-NLS-1$ 
        regEx = regEx.replaceAll(AdminObject.ESCAPED_DELIMITER, ""); //$NON-NLS-1$ 
        return value.matches(regEx);
    }	
    
	@Override
	public void deployVDB(String fileName, URL vdbURL) throws AdminException {
		if (!fileName.endsWith(".vdb") && !fileName.endsWith("-vdb.xml")) {
			throw new AdminProcessingException("The extension of the file name must be either .vdb designer vdbs or -vdb.xml for dynamic VDBs");
		}
		ManagedUtil.deployArchive(getDeploymentManager(), fileName, vdbURL, false);
	}

	
	@Override
	public void deleteVDB(String vdbName, int vdbVersion) throws AdminException {
		ManagedComponent mc = getVDBManagedComponent(vdbName, vdbVersion);
		if (mc != null) {
			ManagedUtil.removeArchive(getDeploymentManager(), mc.getDeployment().getName());
		}
	}	
	
	@Override
	public InputStream exportVDB(String vdbName, int vdbVersion) throws AdminException{
		ManagedComponent mc = getVDBManagedComponent(vdbName, vdbVersion);
		if (mc != null) {
			return exportDeployment(mc.getDeployment().getName());
		}
		return null;
	}
	
	@Override
	public VDB getVDB(String vdbName, int vdbVersion) throws AdminException{
		ManagedComponent mc = getVDBManagedComponent(vdbName, vdbVersion);
		if (mc != null) {
			return buildVDB(mc);
		}
		return null;
	}
	
	private ManagedComponent getVDBManagedComponent(String vdbName, int vdbVersion) throws AdminException{
		try {
			Set<ManagedComponent> vdbComponents = getView().getComponentsForType(VDBTYPE);
			for (ManagedComponent mc: vdbComponents) {
				String name = ManagedUtil.getSimpleValue(mc, "name", String.class);
			    int version = ManagedUtil.getSimpleValue(mc, "version", Integer.class);
			    if (name.equals(vdbName) && version == vdbVersion) {
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
				vdbs.add(buildVDB(mc));
			}
			return vdbs;
		} catch (Exception e) {
			throw new AdminComponentException(e.getMessage(), e);
		}
	}
	
	private VDBMetaData buildVDB(ManagedComponent mc) {
		VDBMetaData vdb = new VDBMetaData();
		vdb.setName(ManagedUtil.getSimpleValue(mc, "name", String.class));
		vdb.setDescription(ManagedUtil.getSimpleValue(mc, "description", String.class));
		String status = ManagedUtil.getSimpleValue(mc, "status", String.class);
		if (status != null) {
			vdb.setStatus(VDB.Status.valueOf(status));
		}
		vdb.setVersion(ManagedUtil.getSimpleValue(mc, "version", Integer.class));
		vdb.setUrl(mc.getDeployment().getName());
		vdb.setProperties(ManagedUtil.getPropertiesValue(mc, "properties"));
		
		// models
		ManagedProperty mp = mc.getProperty("models");
		List<ManagedObject> models = (List<ManagedObject>)MetaValueFactory.getInstance().unwrap(mp.getValue());
		for(ManagedObject mo:models) {
			vdb.addModel(buildModel(mo));
		}
		
		// TODO: add the following
		// SecurityRoleMappings
		
		return vdb;
	}

	private ModelMetaData buildModel(ManagedObject mc) {
		ModelMetaData model = new ModelMetaData();
		model.setName(ManagedUtil.getSimpleValue(mc, "name", String.class));
		model.setVisible(ManagedUtil.getSimpleValue(mc, "visible", Boolean.class));
		model.setModelType(ManagedUtil.getSimpleValue(mc, "modelType", String.class));
		model.setProperties(ManagedUtil.getPropertiesValue(mc, "properties"));
		
		List<SourceMapping> mappings = (List<SourceMapping>)MetaValueFactory.getInstance().unwrap(mc.getProperty("sourceMappings").getValue());
		for (SourceMapping s:mappings) {
			model.addSourceMapping(s.getName(), s.getJndiName());
		}
		
		List<ValidationError> errors = (List<ValidationError>)MetaValueFactory.getInstance().unwrap(mc.getProperty("errors").getValue());
		if (errors != null) {
			for (ValidationError error:errors) {
				model.addError(error.getSeverity(), error.getValue());
			}
		}
		return model;
	}
	
	@Override
	public Collection<Session> getSessions() throws AdminException {
		try {
			Collection<Session> sessionList = new ArrayList<Session>();
			ManagedComponent mc = getView().getComponent(DQPNAME, DQPTYPE);	
			MetaValue value = ManagedUtil.executeOperation(mc, "getActiveSessions");
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
	public void terminateSession(long sessionId) throws AdminException {
		try {
			ManagedComponent mc = getView().getComponent(DQPNAME, DQPTYPE);	
			ManagedUtil.executeOperation(mc, "terminateSession", SimpleValueSupport.wrap(sessionId));
		} catch (Exception e) {
			throw new AdminComponentException(e.getMessage(), e);
		}     	
	}	
	
	@Override
    public Collection<Request> getRequests() throws AdminException {
		try {
			Collection<Request> requestList = new ArrayList<Request>();
			ManagedComponent mc = getView().getComponent(DQPNAME, DQPTYPE);	
			MetaValue value = ManagedUtil.executeOperation(mc, "getRequests");
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
    public Collection<Request> getRequestsForSession(long sessionId) throws AdminException {
		try {
			Collection<Request> requestList = new ArrayList<Request>();
			ManagedComponent mc = getView().getComponent(DQPNAME, DQPTYPE);	
			MetaValue value = ManagedUtil.executeOperation(mc, "getRequestsForSession", SimpleValueSupport.wrap(sessionId));
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
	public void cancelRequest(long sessionId, long requestId) throws AdminException{
		try {
			ManagedComponent mc = getView().getComponent(DQPNAME, DQPTYPE);	
			ManagedUtil.executeOperation(mc, "cancelRequest", SimpleValueSupport.wrap(sessionId), SimpleValueSupport.wrap(requestId));
		} catch (Exception e) {
			throw new AdminComponentException(e.getMessage(), e);
		}     	
	}
	
	private String getRarDeployerName(String rarName) throws AdminException{
		try {
			Set<String> rarDeployments = getView().getDeploymentNamesForType("rar");
			for (String name: rarDeployments) {
				if (name.endsWith(rarName+"/")) {
					return name;
				}
			}
			return null;
		} catch (Exception e) {
			throw new AdminComponentException(e.getMessage(), e);
		}	
	}	
	
	@Override
	public void addConnectorType(String connectorName, URL rarURL) throws AdminException{
		if (!connectorName.startsWith("connector-")) {
			throw new AdminProcessingException("Teiid connector names must start with \"connector-\"");
		}
		
		if (!connectorName.endsWith(".rar")) {
			connectorName = connectorName + ".rar";
		}
		
		String deployerName = getRarDeployerName(connectorName);
		if (deployerName != null) {
			throw new AdminProcessingException("A Connectory with name:"+connectorName+" already exists!");
		}
		
		ManagedUtil.deployArchive(getDeploymentManager(), connectorName, rarURL, false);
		
		//also need to add a template for the properties
		try {
			String connectorNameWithoutExt = connectorName.substring(0, connectorName.length()-4);
			File jarFile = Admin.createConnectorTypeTemplate(connectorNameWithoutExt);
			ManagedUtil.deployArchive(getDeploymentManager(), connectorNameWithoutExt+"-template.jar", jarFile.toURI().toURL(), false);
			jarFile.delete();
		} catch (IOException e) {
			deleteConnectorType(connectorName);
		}
	}
	
	@Override
	public void deleteConnectorType(String connectorName) throws AdminException {
		if (!connectorName.endsWith(".rar")) {
			connectorName = connectorName + ".rar";
		}
		String deployerName = getRarDeployerName(connectorName);
		if (deployerName != null) {
			ManagedUtil.removeArchive(getDeploymentManager(), deployerName);

			//also need to delete template for the properties
			String connectorNameWithoutExt = connectorName.substring(0, connectorName.length()-4);
			ManagedUtil.removeArchive(getDeploymentManager(), connectorNameWithoutExt+"-template.jar");
		}
	}
	
	@Override
	public InputStream exportConnectorType(String connectorName) throws AdminException {
		if (!connectorName.endsWith(".rar")) {
			connectorName = connectorName + ".rar";
		}
		String deployerName = getRarDeployerName(connectorName);
		if (deployerName != null) {
			return exportDeployment(deployerName);			
		}
		return null;
	}
	

	
	@Override
	public Collection<String> getCacheTypes() throws AdminException {
		try {
			Collection<String> requestList = new ArrayList<String>();
			ManagedComponent mc = getView().getComponent(DQPNAME, DQPTYPE);	
			MetaValue value = ManagedUtil.executeOperation(mc, "getCacheTypes");
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
			ManagedUtil.executeOperation(mc, "clearCache", SimpleValueSupport.wrap(cacheType));
		} catch (Exception e) {
			throw new AdminComponentException(e.getMessage(), e);
		} 		
	}
	
	@Override
	public Collection<Transaction> getTransactions() throws AdminException {
		try {
			Collection<Transaction> txnList = new ArrayList<Transaction>();
			ManagedComponent mc = getView().getComponent(DQPNAME, DQPTYPE);	
			MetaValue value = ManagedUtil.executeOperation(mc, "getTransactions");
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
			ManagedUtil.executeOperation(mc, "terminateTransaction", MetaValueFactory.getInstance().create(xid));
		} catch (Exception e) {
			throw new AdminComponentException(e.getMessage(), e);
		} 	
	}
	
	@Override
	public WorkerPoolStatistics getWorkManagerStats(String identifier) throws AdminException {
		try {
			ManagedComponent mc = getView().getComponent(DQPNAME, DQPTYPE);
			MetaValue value = ManagedUtil.executeOperation(mc, "getWorkManagerStatistics", SimpleValueSupport.wrap(identifier));
			return (WorkerPoolStatistics)MetaValueFactory.getInstance().unwrap(value, WorkerPoolStatistics.class);	
		} catch (Exception e) {
			throw new AdminComponentException(e.getMessage(), e);
		}
	}	
	
	@Override
	public ConnectionPoolStatistics getConnectorConnectionPoolStats(String deployedName) throws AdminException {
		ManagedComponent mc = getConnectorBindingComponent(deployedName);
		if (mc != null) {
			return buildConnectorConnectionPool(mc);
		}
		return null;
	}

	private ConnectionPoolStatistics buildConnectorConnectionPool(ManagedComponent mc) {
		ConnectionPoolStatisticsMetadata stats = new ConnectionPoolStatisticsMetadata();
		stats.setName(mc.getName());
		stats.setAvailableConnectionCount(ManagedUtil.getSimpleValue(mc, "availableConnectionCount", Long.class));
		stats.setConnectionCount(ManagedUtil.getSimpleValue(mc, "connectionCount", Integer.class));
		stats.setConnectionCreatedCount(ManagedUtil.getSimpleValue(mc, "connectionCreatedCount", Integer.class));
		stats.setConnectionDestroyedCount(ManagedUtil.getSimpleValue(mc, "connectionDestroyedCount", Integer.class));
		stats.setInUseConnectionCount(ManagedUtil.getSimpleValue(mc, "inUseConnectionCount", Long.class));
		stats.setMaxConnectionsInUseCount(ManagedUtil.getSimpleValue(mc, "maxConnectionsInUseCount", Long.class));
		stats.setMaxSize(ManagedUtil.getSimpleValue(mc, "maxSize", Integer.class));
		stats.setMinSize(ManagedUtil.getSimpleValue(mc, "minSize", Integer.class));
		return stats;
	}	
	
	@Override
	public Collection<PropertyDefinition> getConnectorTypePropertyDefinitions(String typeName) throws AdminException {
		try {
			DeploymentTemplateInfo info = getView().getTemplate(typeName);
			if(info == null) {
				throw new AdminProcessingException("Connector Type template supplied not found in the configuration."+typeName);
			}
			
			ArrayList<PropertyDefinition> props = new ArrayList<PropertyDefinition>();
			Map<String, ManagedProperty> propertyMap = info.getProperties();
			
			for (ManagedProperty mp:propertyMap.values()) {
					PropertyDefinitionMetadata p = new PropertyDefinitionMetadata();
					p.setName(mp.getName());
					p.setDescription(mp.getDescription());
					p.setDisplayName(mp.getMappedName());
					if (mp.getDefaultValue() != null) {
						p.setDefaultValue(((SimpleValueSupport)mp.getDefaultValue()).getValue());
					}
					p.setPropertyTypeClassName(mp.getMetaType().getTypeName());
					p.setModifiable(!mp.isReadOnly());
					
					if (mp.getField("masked", Boolean.class) != null) {
						p.setMasked(mp.getField("masked", Boolean.class));
					}
					else {
						p.setMasked(false);
					}
					
					if (mp.getField("advanced", Boolean.class) != null) {
						p.setAdvanced(mp.getField("advanced", Boolean.class));
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
	
	@Override
    public void addDataSource(String deploymentName, Properties properties) throws AdminException {
		addConnectionfactory(deploymentName, XA_DATA_SOURCE_TEMPLATE, properties);
	}
	
    private void addConnectionfactory(String deploymentName, String typeName, Properties properties) throws AdminException {	
		try {
			DeploymentTemplateInfo info = getView().getTemplate(typeName);
			if(info == null) {
				throw new AdminProcessingException("Connector Type template supplied not found in the configuration."+typeName);
			}
		
			//config-properties list
			Map<String, String> configProps = new HashMap<String, String>();
			
			// template properties specific to the template
			Map<String, ManagedProperty> propertyMap = info.getProperties();
			
			// walk through the supplied properties and assign properly to either template
			// of config-properties.
			for (String key:properties.stringPropertyNames()) {
				ManagedProperty mp = propertyMap.get(key);
				if (mp != null) {
					String value = properties.getProperty(key);
					if (!ManagedUtil.sameValue(mp.getDefaultValue(), value)){
						mp.setValue(SimpleValueSupport.wrap(value));
					}
				}
				else {
					configProps.put(key, properties.getProperty(key));
					configProps.put(key+".type", "java.lang.String");
				}
			}
			
			if (configProps.size() > 0) {
				MetaValue metaValue = ManagedUtil.compositeValueMap(configProps);
				info.getProperties().get("config-property").setValue(metaValue);				
			}
			
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
			ManagementView view = getView();
			ManagedComponent mc = view.getComponent(deployedName, DSTYPE);
			if (mc != null) {
				ManagedUtil.removeArchive(getDeploymentManager(),mc.getDeployment().getName());
			}
		} catch (Exception e) {
			throw new AdminComponentException(e.getMessage(), e);
		}
    }
    
	@Override
    public Collection<PropertyDefinition> getDataSourcePropertyDefinitions() throws AdminException {
		return getConnectorTypePropertyDefinitions(XA_DATA_SOURCE_TEMPLATE);
	}
	
	private static final String connectorTemplate = 
		"<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n"+
        "<deployment xmlns=\"urn:jboss:bean-deployer:2.0\">\n" +
		"<!-- This is Teiid connector type template - DO NOT DELETE -->\n"+
		"<bean name=\"${name}\" class=\"org.teiid.templates.connector.ConnectorTypeTemplate\">\n" +
		"    <property name=\"info\"><inject bean=\"${name}-templateinfo\"/></property>\n" +
        "    <property name=\"targetTemplate\"><inject bean=\"NoTxConnectionFactoryTemplate\"/></property>\n" +
        "</bean>\n" +
        "<bean name=\"${name}-templateinfo\" class=\"org.teiid.templates.connector.ConnectorTypeTemplateInfo\">\n" +
        "  <constructor factoryMethod=\"createTemplateInfo\">\n" +
        "  <factory bean=\"DSDeploymentTemplateInfoFactory\"/>\n" +
        "    <parameter class=\"java.lang.Class\">org.teiid.templates.connector.ConnectorTypeTemplateInfo</parameter>\n" +
        "    <parameter class=\"java.lang.Class\">org.jboss.resource.metadata.mcf.NoTxConnectionFactoryDeploymentMetaData</parameter>\n" +
        "    <parameter class=\"java.lang.String\">${name}</parameter>\n" +
        "    <parameter class=\"java.lang.String\">${name}</parameter>\n"+
        "  </constructor>\n" +
        "</bean>\n"+
        "</deployment>";
	
	private static File createConnectorTypeTemplate(String name) throws IOException {
		String content = connectorTemplate.replace("${name}", name);
		
		File jarFile = File.createTempFile(name, ".jar");
		JarOutputStream jo = new JarOutputStream(new BufferedOutputStream(new FileOutputStream(jarFile)));
		
		JarEntry je = new JarEntry("META-INF/jboss-beans.xml");
		jo.putNextEntry(je);
		
		jo.write(content.getBytes());
		
		jo.close();
		return jarFile;
	}
	
	
	@Override
	public void assignBindingsToModel(String vdbName, int vdbVersion, String modelName, String[] connectorBindingNames) throws AdminException {

//		ManagedComponent mc = getVDBManagedComponent(vdbName, vdbVersion);
//		if (mc == null) {
//			throw new AdminProcessingException("VDB with name = "+vdbName + " version = "+ vdbVersion + " not found in configuration");
//		}
//		VDBMetaData vdb = buildVDB(mc);
//		ModelMetaData model = vdb.getModel(modelName);
//		if (model == null) {
//			throw new AdminProcessingException("Model name = "+modelName+" not found in the VDB with name = "+vdbName + " version = "+ vdbVersion);
//		}
//
//		String referenceName = model.getConnectorReference();
//		ArrayList<MetaValue> newBindings = new ArrayList<MetaValue>();
//		for (String name:connectorBindingNames) {
//			newBindings.add(new SimpleValueSupport(SimpleMetaType.STRING, name));
//		}
//		
//		ManagedProperty mappings = mc.getProperty("connectorMappings");
//		MetaValue[] elements = ((CollectionValueSupport)mappings.getValue()).getElements();
//		ArrayList<MetaValue> modifiedElements = new ArrayList<MetaValue>();
//		for (MetaValue mv:elements) {
//			MetaValue value = ((CompositeValueSupport)mv).get("refName");
//			if (value != null && ManagedUtil.stringValue(value).equals(referenceName)) {
//				CollectionValueSupport bindings = (CollectionValueSupport)((CompositeValueSupport)mv).get("resourceNames");
//				bindings.setElements(newBindings.toArray(new MetaValue[newBindings.size()]));
//			}
//			else {
//				modifiedElements.add(mv);
//			}
//		}
//		
//		try {
//			getView().updateComponent(mc);
//		} catch (Exception e) {
//			throw new AdminComponentException(e.getMessage(), e);
//		}
	}	

}
