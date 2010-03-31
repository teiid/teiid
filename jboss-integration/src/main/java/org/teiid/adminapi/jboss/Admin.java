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
import java.io.InputStreamReader;
import java.io.Reader;
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
import org.jboss.managed.api.ComponentType;
import org.jboss.managed.api.DeploymentTemplateInfo;
import org.jboss.managed.api.ManagedComponent;
import org.jboss.managed.api.ManagedObject;
import org.jboss.managed.api.ManagedProperty;
import org.jboss.managed.plugins.DefaultFieldsImpl;
import org.jboss.managed.plugins.WritethroughManagedPropertyImpl;
import org.jboss.metatype.api.types.MapCompositeMetaType;
import org.jboss.metatype.api.types.SimpleMetaType;
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
import org.teiid.adminapi.impl.WorkerPoolStatisticsMetadata;
import org.teiid.connector.api.Connector;
import org.teiid.jboss.IntegrationPlugin;
import org.teiid.jboss.deployers.RuntimeEngineDeployer;

public class Admin extends TeiidAdmin {
	private static final ProfileKey DEFAULT_PROFILE_KEY = new ProfileKey(ProfileKey.DEFAULT);
	
	private static final String XA_DATA_SOURCE_TEMPLATE = "XADataSourceTemplate"; //$NON-NLS-1$
	private static final long serialVersionUID = 7081309086056911304L;
	private static ComponentType VDBTYPE = new ComponentType("teiid", "vdb");//$NON-NLS-1$ //$NON-NLS-2$
	private static ComponentType NOTXTYPE = new ComponentType("ConnectionFactory", "NoTx");//$NON-NLS-1$ //$NON-NLS-2$
	private static ComponentType TXTYPE = new ComponentType("ConnectionFactory", "Tx");//$NON-NLS-1$ //$NON-NLS-2$
	private static ComponentType DQPTYPE = new ComponentType("teiid", "dqp");//$NON-NLS-1$ //$NON-NLS-2$
	private static ComponentType DSTYPE = new ComponentType("DataSource", "XA");//$NON-NLS-1$ //$NON-NLS-2$
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
	public Collection<ConnectorBinding> getConnectorBindings() throws AdminException {
		ArrayList<ConnectorBinding> bindings = new ArrayList<ConnectorBinding>();
		findConnectorBindings(bindings, "NoTx"); //$NON-NLS-1$			
		findConnectorBindings(bindings, "Tx"); //$NON-NLS-1$
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
	public Reader exportConnectorBinding(String deployedName) throws AdminException {
		ManagedComponent mc = getConnectorBindingComponent(deployedName);
		if (mc != null) {
			return new InputStreamReader(exportDeployment(mc.getDeployment().getName()));
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
			if (deployedName.startsWith("java:")) { //$NON-NLS-1$
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
		connector.addProperty("deployer-name", mc.getDeployment().getName());//$NON-NLS-1$	
		
		for (String key:mc.getProperties().keySet()) {
			ManagedProperty property = mc.getProperty(key);
			MetaValue value = property.getValue();
			
			//TODO: All properties need to be added
			if (value != null) {
				if(value.getMetaType().isSimple()) {
					connector.addProperty(key, ManagedUtil.stringValue(value));
				}
				else if (key.equals("config-property")) { //$NON-NLS-1$	
					MapCompositeValueSupport v1 = (MapCompositeValueSupport)value;
					MapCompositeMetaType metaType = v1.getMetaType();
					for (String configProperty:metaType.keySet()) {
						if (!configProperty.endsWith(".type")) { //$NON-NLS-1$	
							connector.addProperty(configProperty, ManagedUtil.stringValue(v1.get(configProperty)));
						}
					}
				}
				else {
					//log.info(key+" property is not added to connector properties");
				}
			}
		}
		return connector;
	}
	
	private boolean isConnectorBinding(ManagedComponent mc) {
	    String connectionDefinition = ManagedUtil.getSimpleValue(mc, "connection-definition", String.class); //$NON-NLS-1$	
	    return Connector.class.getName().equals(connectionDefinition);
	}
	
	private void findConnectorBindings(ArrayList<ConnectorBinding> bindings, String subType) throws AdminException {
		try {
			ComponentType type = new ComponentType("ConnectionFactory", subType); //$NON-NLS-1$	
			Set<ManagedComponent> jcaConnectors = getView().getComponentsForType(type);
			
			for(ManagedComponent mc:jcaConnectors) {
			    ManagedProperty mp = mc.getProperty("connection-definition"); //$NON-NLS-1$	
				SimpleValueSupport v = (SimpleValueSupport)mp.getValue();
				if (v.getValue().equals(Connector.class.getName())){
					bindings.add(buildConnectorBinding(mc));
			    }
			}
		}catch(Exception e) {
			throw new AdminComponentException(e);
		}
	}
	
	@Override
	public ConnectorBinding addConnectorBinding(String deploymentName, String typeName, Properties properties) throws AdminException {
		if (getConnectorBinding(deploymentName) != null) {
			throw new AdminProcessingException(IntegrationPlugin.Util.getString("connector_binding_exists",deploymentName)); //$NON-NLS-1$;
		}
		properties.setProperty("connection-definition", Connector.class.getName()); //$NON-NLS-1$	
		addConnectionfactory(deploymentName, typeName, properties);
		
		return getConnectorBinding(deploymentName);
	}
	
	@Override
	public void setConnectorBindingProperty(String deployedName, String propertyName, String propertyValue) throws AdminException{
		ManagedComponent mc = getConnectorBindingComponent(deployedName);
		if (mc == null) {
			throw new AdminProcessingException(IntegrationPlugin.Util.getString("connector_binding_exists",deployedName)); //$NON-NLS-1$;
		}
		if (mc.getProperty(propertyName) != null) {
			mc.getProperty(propertyName).setValue(SimpleValueSupport.wrap(propertyValue));
		}
		else {
			Map<String, String> configProps = new HashMap<String, String>();
			configProps.put(propertyName, propertyValue);
			configProps.put(propertyValue+".type", "java.lang.String"); //$NON-NLS-1$	//$NON-NLS-2$	
			MetaValue metaValue = ManagedUtil.compositeValueMap(configProps);
			mc.getProperty("config-property").setValue(metaValue); //$NON-NLS-1$	
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
			String deployerName = binding.getPropertyValue("deployer-name"); //$NON-NLS-1$
			if (deployerName == null) {
				throw new AdminProcessingException(IntegrationPlugin.Util.getString("failed_to_connector_deployer")); //$NON-NLS-1$
			}
			ManagedUtil.execute(getDeploymentManager().start(deployerName), IntegrationPlugin.Util.getString("failed_to_start_connector", binding.getName())); //$NON-NLS-1$
		} catch (Exception e) {
			ManagedUtil.handleException(e);
		}
	}

	@Override
	public void stopConnectorBinding(ConnectorBinding binding) throws AdminException {
		try {
			String deployerName = binding.getPropertyValue("deployer-name");//$NON-NLS-1$
			if (deployerName == null) {
				throw new AdminProcessingException(IntegrationPlugin.Util.getString("failed_to_connector_deployer")); //$NON-NLS-1$
			}			
			ManagedUtil.execute(getDeploymentManager().stop(deployerName), IntegrationPlugin.Util.getString("failed_to_stop_connector", binding.getName())); //$NON-NLS-1$
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
			if (name.startsWith("connector-")) {//$NON-NLS-1$
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
				String name = ManagedUtil.getSimpleValue(mc, "name", String.class);//$NON-NLS-1$
			    int version = ManagedUtil.getSimpleValue(mc, "version", Integer.class);//$NON-NLS-1$
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
		vdb.setName(ManagedUtil.getSimpleValue(mc, "name", String.class));//$NON-NLS-1$
		vdb.setDescription(ManagedUtil.getSimpleValue(mc, "description", String.class));//$NON-NLS-1$
		String status = ManagedUtil.getSimpleValue(mc, "status", String.class);//$NON-NLS-1$
		if (status != null) {
			vdb.setStatus(VDB.Status.valueOf(status));
		}
		vdb.setVersion(ManagedUtil.getSimpleValue(mc, "version", Integer.class));//$NON-NLS-1$
		vdb.setUrl(mc.getDeployment().getName());
		ManagedProperty prop = mc.getProperty("JAXBProperties"); //$NON-NLS-1$
		List<ManagedObject> properties = (List<ManagedObject>)MetaValueFactory.getInstance().unwrap(prop.getValue());
		for (ManagedObject managedProperty:properties) {
			vdb.addProperty(ManagedUtil.getSimpleValue(managedProperty, "name", String.class), ManagedUtil.getSimpleValue(managedProperty, "value", String.class)); //$NON-NLS-1$ //$NON-NLS-2$
		}
		
		// models
		ManagedProperty mp = mc.getProperty("models");//$NON-NLS-1$
		List<ManagedObject> models = (List<ManagedObject>)MetaValueFactory.getInstance().unwrap(mp.getValue());
		for(ManagedObject mo:models) {
			vdb.addModel(buildModel(mo));
		}
		
		// TODO: add the following
		// SecurityRoleMappings
		
		return vdb;
	}

	private ModelMetaData buildModel(ManagedObject managedModel) {
		ModelMetaData model = new ModelMetaData();
		model.setName(ManagedUtil.getSimpleValue(managedModel, "name", String.class));//$NON-NLS-1$
		model.setVisible(ManagedUtil.getSimpleValue(managedModel, "visible", Boolean.class));//$NON-NLS-1$
		model.setModelType(ManagedUtil.getSimpleValue(managedModel, "modelType", String.class));//$NON-NLS-1$

		ManagedProperty prop = managedModel.getProperty("JAXBProperties"); //$NON-NLS-1$
		List<ManagedObject> properties = (List<ManagedObject>)MetaValueFactory.getInstance().unwrap(prop.getValue());
		for (ManagedObject managedProperty:properties) {
			model.addProperty(ManagedUtil.getSimpleValue(managedProperty, "name", String.class), ManagedUtil.getSimpleValue(managedProperty, "value", String.class)); //$NON-NLS-1$ //$NON-NLS-2$
		}
		
        ManagedProperty sourceMappings = managedModel.getProperty("sourceMappings");//$NON-NLS-1$
        if (sourceMappings != null){
            List<ManagedObject> mappings = (List<ManagedObject>)MetaValueFactory.getInstance().unwrap(sourceMappings.getValue());
            for (ManagedObject mo:mappings) {
                String name = ManagedUtil.getSimpleValue(mo, "name", String.class);//$NON-NLS-1$
                String jndiName = ManagedUtil.getSimpleValue(mo, "jndiName", String.class);//$NON-NLS-1$
                model.addSourceMapping(name, jndiName);
            }
        }
        
        ManagedProperty validationErrors = managedModel.getProperty("errors");//$NON-NLS-1$
        if (validationErrors != null) {
    		List<ManagedObject> errors = (List<ManagedObject>)MetaValueFactory.getInstance().unwrap(validationErrors.getValue());
    		if (errors != null) {
    			for (ManagedObject mo:errors) {
    				model.addError(ManagedUtil.getSimpleValue(mo, "severity", String.class), ManagedUtil.getSimpleValue(mo, "value", String.class));//$NON-NLS-1$ //$NON-NLS-2$
    			}
    		}        	
        }
		return model;
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
	public void terminateSession(long sessionId) throws AdminException {
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
    public Collection<Request> getRequestsForSession(long sessionId) throws AdminException {
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
	public void cancelRequest(long sessionId, long requestId) throws AdminException{
		try {
			ManagedComponent mc = getView().getComponent(DQPNAME, DQPTYPE);	
			ManagedUtil.executeOperation(mc, "cancelRequest", SimpleValueSupport.wrap(sessionId), SimpleValueSupport.wrap(requestId));//$NON-NLS-1$
		} catch (Exception e) {
			throw new AdminComponentException(e.getMessage(), e);
		}     	
	}
	
	private String getRarDeployerName(String rarName) throws AdminException{
		try {
			Set<String> rarDeployments = getView().getDeploymentNamesForType("rar");//$NON-NLS-1$
			for (String name: rarDeployments) {
				if (name.endsWith(rarName+"/")) { //$NON-NLS-1$
					return name;
				}
			}
			return null;
		} catch (Exception e) {
			throw new AdminComponentException(e.getMessage(), e);
		}	
	}	
	
	@Override
	public void addConnectorType(String connectorName, InputStream rar) throws AdminException{
		if (!connectorName.startsWith("connector-")) {//$NON-NLS-1$
			throw new AdminProcessingException(IntegrationPlugin.Util.getString("bad_connector_type_name")); //$NON-NLS-1$
		}
		
		if (!connectorName.endsWith(".rar")) {//$NON-NLS-1$
			connectorName = connectorName + ".rar";//$NON-NLS-1$
		}
		
		String deployerName = getRarDeployerName(connectorName);
		if (deployerName != null) {
			throw new AdminProcessingException(IntegrationPlugin.Util.getString("connector_type_exists", deployerName)); //$NON-NLS-1$
		}
		
		ManagedUtil.deployArchive(getDeploymentManager(), connectorName, rar, false);
		
		//also need to add a template for the properties
		try {
			String connectorNameWithoutExt = connectorName.substring(0, connectorName.length()-4);
			File jarFile = Admin.createConnectorTypeTemplate(connectorNameWithoutExt);
			ManagedUtil.deployArchive(getDeploymentManager(), connectorNameWithoutExt+"-template.jar", jarFile.toURI().toURL(), false);//$NON-NLS-1$
			jarFile.delete();
		} catch (IOException e) {
			deleteConnectorType(connectorName);
		}
	}
	
	@Override
	public void deleteConnectorType(String connectorName) throws AdminException {
		if (!connectorName.endsWith(".rar")) {//$NON-NLS-1$
			connectorName = connectorName + ".rar";//$NON-NLS-1$
		}
		String deployerName = getRarDeployerName(connectorName);
		if (deployerName != null) {
			//also need to delete template for the properties
			String connectorNameWithoutExt = connectorName.substring(0, connectorName.length()-4);
			ManagedUtil.removeArchive(getDeploymentManager(), connectorNameWithoutExt+"-template.jar");//$NON-NLS-1$
			
			ManagedUtil.removeArchive(getDeploymentManager(), deployerName);
		}
	}
	
	@Override
	public InputStream exportConnectorType(String connectorName) throws AdminException {
		if (!connectorName.endsWith(".rar")) {//$NON-NLS-1$
			connectorName = connectorName + ".rar";//$NON-NLS-1$
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
	public WorkerPoolStatistics getWorkManagerStats(String identifier) throws AdminException {
		try {
			ManagedComponent mc = getView().getComponent(DQPNAME, DQPTYPE);
			MetaValue value = ManagedUtil.executeOperation(mc, "getWorkManagerStatistics", SimpleValueSupport.wrap(identifier));//$NON-NLS-1$
			return (WorkerPoolStatistics)MetaValueFactory.getInstance().unwrap(value, WorkerPoolStatisticsMetadata.class);	
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
		stats.setAvailableConnectionCount(ManagedUtil.getSimpleValue(mc, "availableConnectionCount", Long.class));//$NON-NLS-1$
		stats.setConnectionCount(ManagedUtil.getSimpleValue(mc, "connectionCount", Integer.class));//$NON-NLS-1$
		stats.setConnectionCreatedCount(ManagedUtil.getSimpleValue(mc, "connectionCreatedCount", Integer.class));//$NON-NLS-1$
		stats.setConnectionDestroyedCount(ManagedUtil.getSimpleValue(mc, "connectionDestroyedCount", Integer.class));//$NON-NLS-1$
		stats.setInUseConnectionCount(ManagedUtil.getSimpleValue(mc, "inUseConnectionCount", Long.class));//$NON-NLS-1$
		stats.setMaxConnectionsInUseCount(ManagedUtil.getSimpleValue(mc, "maxConnectionsInUseCount", Long.class));//$NON-NLS-1$
		stats.setMaxSize(ManagedUtil.getSimpleValue(mc, "maxSize", Integer.class));//$NON-NLS-1$
		stats.setMinSize(ManagedUtil.getSimpleValue(mc, "minSize", Integer.class));//$NON-NLS-1$
		return stats;
	}	
	
	@Override
	public Collection<PropertyDefinition> getConnectorTypePropertyDefinitions(String typeName) throws AdminException {
		try {
			DeploymentTemplateInfo info = getView().getTemplate(typeName);
			if(info == null) {
				throw new AdminProcessingException(IntegrationPlugin.Util.getString("connector_type_not_found", typeName)); //$NON-NLS-1$
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
	
	@Override
    public void addDataSource(String deploymentName, Properties properties) throws AdminException {
		addConnectionfactory(deploymentName, XA_DATA_SOURCE_TEMPLATE, properties);
	}
	
    private void addConnectionfactory(String deploymentName, String typeName, Properties properties) throws AdminException {	
		try {
			DeploymentTemplateInfo info = getView().getTemplate(typeName);
			if(info == null) {
				throw new AdminProcessingException(IntegrationPlugin.Util.getString("connector_type_not_found", typeName)); //$NON-NLS-1$
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
					configProps.put(key+".type", "java.lang.String");//$NON-NLS-1$ //$NON-NLS-2$
				}
			}
			
			if (configProps.size() > 0) {
				MetaValue metaValue = ManagedUtil.compositeValueMap(configProps);
				info.getProperties().get("config-property").setValue(metaValue);//$NON-NLS-1$				
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
		"<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n"+ //$NON-NLS-1$
        "<deployment xmlns=\"urn:jboss:bean-deployer:2.0\">\n" + //$NON-NLS-1$
		"<!-- This is Teiid connector type template - DO NOT DELETE -->\n"+ //$NON-NLS-1$
		"<bean name=\"${name}\" class=\"org.teiid.templates.connector.ConnectorTypeTemplate\">\n" + //$NON-NLS-1$
		"    <property name=\"info\"><inject bean=\"${name}-templateinfo\"/></property>\n" + //$NON-NLS-1$
        "    <property name=\"targetTemplate\"><inject bean=\"NoTxConnectionFactoryTemplate\"/></property>\n" + //$NON-NLS-1$
        "</bean>\n" + //$NON-NLS-1$
        "<bean name=\"${name}-templateinfo\" class=\"org.teiid.templates.connector.ConnectorTypeTemplateInfo\">\n" + //$NON-NLS-1$
        "  <constructor factoryMethod=\"createTemplateInfo\">\n" + //$NON-NLS-1$
        "  <factory bean=\"DSDeploymentTemplateInfoFactory\"/>\n" + //$NON-NLS-1$
        "    <parameter class=\"java.lang.Class\">org.teiid.templates.connector.ConnectorTypeTemplateInfo</parameter>\n" + //$NON-NLS-1$
        "    <parameter class=\"java.lang.Class\">org.jboss.resource.metadata.mcf.NoTxConnectionFactoryDeploymentMetaData</parameter>\n" + //$NON-NLS-1$
        "    <parameter class=\"java.lang.String\">${name}</parameter>\n" + //$NON-NLS-1$
        "    <parameter class=\"java.lang.String\">${name}</parameter>\n"+ //$NON-NLS-1$
        "  </constructor>\n" + //$NON-NLS-1$
        "</bean>\n"+ //$NON-NLS-1$
        "</deployment>"; //$NON-NLS-1$
	
	private static File createConnectorTypeTemplate(String name) throws IOException {
		String content = connectorTemplate.replace("${name}", name);//$NON-NLS-1$
		
		File jarFile = File.createTempFile(name, ".jar");//$NON-NLS-1$
		JarOutputStream jo = new JarOutputStream(new BufferedOutputStream(new FileOutputStream(jarFile)));
		
		JarEntry je = new JarEntry("META-INF/jboss-beans.xml");//$NON-NLS-1$
		jo.putNextEntry(je);
		
		jo.write(content.getBytes());
		
		jo.close();
		return jarFile;
	}
	
	
	@Override
	public void assignBindingToModel(String vdbName, int vdbVersion, String modelName, String sourceName, String jndiName) throws AdminException {

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
                	ManagedProperty jndiProperty = mo.getProperty("jndiName"); //$NON-NLS-1$
                	if (jndiProperty == null) {
                		jndiProperty = new WritethroughManagedPropertyImpl(mo, new DefaultFieldsImpl("jndiName")); //$NON-NLS-1$
                	}
                	jndiProperty.setValue(ManagedUtil.wrap(SimpleMetaType.STRING, jndiName));
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

}
