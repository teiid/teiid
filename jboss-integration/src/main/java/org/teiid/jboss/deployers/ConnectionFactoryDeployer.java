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
package org.teiid.jboss.deployers;

import java.util.List;
import java.util.Map;
import java.util.Set;

import org.jboss.deployers.spi.DeploymentException;
import org.jboss.deployers.spi.deployer.helpers.AbstractSimpleRealDeployer;
import org.jboss.deployers.spi.deployer.managed.ManagedObjectCreator;
import org.jboss.deployers.structure.spi.DeploymentUnit;
import org.jboss.managed.api.ManagedObject;
import org.jboss.managed.api.ManagedProperty;
import org.jboss.managed.api.factory.ManagedObjectFactory;
import org.jboss.metatype.api.types.MetaType;
import org.jboss.metatype.api.values.CollectionValue;
import org.jboss.metatype.api.values.GenericValue;
import org.jboss.metatype.api.values.MapCompositeValueSupport;
import org.jboss.metatype.api.values.MetaValue;
import org.jboss.resource.metadata.mcf.ManagedConnectionFactoryDeploymentGroup;
import org.jboss.resource.metadata.mcf.ManagedConnectionFactoryDeploymentMetaData;
import org.jboss.resource.metadata.mcf.ManagedConnectionFactoryPropertyMetaData;
import org.teiid.adminapi.ConnectionFactory;
import org.teiid.adminapi.impl.ConnectionFactoryMetaData;
import org.teiid.adminapi.jboss.ManagedUtil;
import org.teiid.connector.api.Connector;
import org.teiid.deployers.VDBStatusChecker;
import org.teiid.dqp.internal.datamgr.impl.ConnectorManager;
import org.teiid.dqp.internal.datamgr.impl.ConnectorManagerRepository;
import org.teiid.jboss.IntegrationPlugin;

import com.metamatrix.common.log.LogConstants;
import com.metamatrix.common.log.LogManager;

/**
 * This is a deployer that looks for the teiid connection factories deployed with their own "-ds.xml"
 * file, then this class creates a ConnectorManager for it. So, the access to the "Connector" is through look-up
 * in jndi tree for Connector Manager. Connector is JCA component.
 */
public class ConnectionFactoryDeployer extends AbstractSimpleRealDeployer<ManagedConnectionFactoryDeploymentGroup> implements ManagedObjectCreator {
	
	private ManagedObjectFactory mof;
	private ConnectorManagerRepository connectorManagerRepository;
	private VDBStatusChecker vdbChecker;
	
	public ConnectionFactoryDeployer() {
		super(ManagedConnectionFactoryDeploymentGroup.class);
		setRelativeOrder(3000);
	}

	@Override
	public void deploy(DeploymentUnit unit, ManagedConnectionFactoryDeploymentGroup group) throws DeploymentException {
		List<ManagedConnectionFactoryDeploymentMetaData> deployments = group.getDeployments();
		
		ConnectionFactoryMetadataGroup cfGroup = new ConnectionFactoryMetadataGroup();
		
		for (ManagedConnectionFactoryDeploymentMetaData data : deployments) {
			String connectorDefinition = data.getConnectionDefinition();
			String connectorName = "java:"+data.getJndiName(); //$NON-NLS-1$
			
			if (connectorDefinition.equals(Connector.class.getName())) {
				ConnectorManager cm = new ConnectorManager(connectorName, data.getMaxSize());
				// start the connector manager
				cm.start();

				// Add the references to the mgr as loaded.
	            this.connectorManagerRepository.addConnectorManager(connectorName, cm);    
	            cfGroup.addConnectionFactory(buildCF(data));
	            
	            this.vdbChecker.connectorAdded(connectorName);
	            LogManager.logInfo(LogConstants.CTX_RUNTIME, IntegrationPlugin.Util.getString("connector_started", connectorName)); //$NON-NLS-1$
			}
		}
		
		if (!cfGroup.getConnectionFactories().isEmpty()) {
			unit.addAttachment(ConnectionFactoryMetadataGroup.class, cfGroup);
		}		
	}
    
	public void setConnectorManagerRepository(ConnectorManagerRepository repo) {
		this.connectorManagerRepository = repo;
	}

	@Override
	public void undeploy(DeploymentUnit unit, ManagedConnectionFactoryDeploymentGroup group) {
		super.undeploy(unit, group);
		List<ManagedConnectionFactoryDeploymentMetaData> deployments = group.getDeployments();

		for (ManagedConnectionFactoryDeploymentMetaData data : deployments) {
			String connectorDefinition = data.getConnectionDefinition();
			if (connectorDefinition.equals(Connector.class.getName())) {
				String connectorName = "java:"+data.getJndiName(); //$NON-NLS-1$
				if (this.connectorManagerRepository != null) {
					ConnectorManager cm = this.connectorManagerRepository.removeConnectorManager(connectorName);
					if (cm != null) {
						cm.stop();
						this.vdbChecker.connectorRemoved(connectorName);
						LogManager.logInfo(LogConstants.CTX_RUNTIME, IntegrationPlugin.Util.getString("connector_stopped", connectorName)); //$NON-NLS-1$				
					}
				}
			}
		}
	}
	
	@Override
	public void build(DeploymentUnit unit, Set<String> outputs,Map<String, ManagedObject> managedObjects) throws DeploymentException {
		ConnectionFactoryMetadataGroup group = unit.getAttachment(ConnectionFactoryMetadataGroup.class);
		
		ManagedObject mcfdgMO = managedObjects.get(ManagedConnectionFactoryDeploymentGroup.class.getName());
		if (mcfdgMO != null) {
			ManagedProperty deployments = mcfdgMO.getProperty("deployments"); //$NON-NLS-1$
			if (deployments.getMetaType().isCollection()) {
				if (deployments.getValue() != null) {
					MetaValue[] elements = ((CollectionValue)deployments.getValue()).getElements();
					for (MetaValue element:elements) {
						ManagedObject managed = (ManagedObject) ((GenericValue)element).getValue();
						
						ConnectionFactoryMetaData data = group.getConnectionFactory("teiid-cf/"+managed.getName()); //$NON-NLS-1$
						populateConnectionFactory(data, managed);
						if (data != null) {
							ManagedObject mo = this.mof.initManagedObject(data, ConnectionFactory.class, data.getName(), data.getName());
							if (mo == null) {
								throw new DeploymentException("could not create managed object"); //$NON-NLS-1$
							}
							managedObjects.put(mo.getName(), mo);	
						}
					}
				}
			}
		}
	}	
	


	public void setManagedObjectFactory(ManagedObjectFactory mof) {
		this.mof = mof;
	}
		
	public void setVDBStatusChecker(VDBStatusChecker vdbChecker) {
		this.vdbChecker = vdbChecker;
	}
	
	static ConnectionFactoryMetaData buildCF(ManagedConnectionFactoryDeploymentMetaData data) {
		ConnectionFactoryMetaData c = new ConnectionFactoryMetaData();
		// The name is prefixed here because, the Managed objects map overwrites this MO, with the main
		// -ds.xml based MO, since they have same names.
		c.setName("teiid-cf/"+data.getJndiName()); //$NON-NLS-1$
		c.setRARFileName(data.getRarName());
		c.setJNDIName("java:"+data.getJndiName()); //$NON-NLS-1$
		
		List<ManagedConnectionFactoryPropertyMetaData> props = data.getManagedConnectionFactoryProperties();
		for (ManagedConnectionFactoryPropertyMetaData p:props) {
			c.addProperty(p.getName(), p.getValue());
		}
		return c;
	}
	
	private void populateConnectionFactory(ConnectionFactoryMetaData data, ManagedObject managed) {
		Map<String, ManagedProperty> props = managed.getProperties();
		
		for (String key:props.keySet()) {
			ManagedProperty mp = props.get(key);
			
			MetaValue value = mp.getValue();
			if (value != null) {
				MetaType type = value.getMetaType();
				if (type.isSimple()) {
					data.addProperty(mp.getName(), ManagedUtil.stringValue(value));
				}
				else if (type.isComposite()) {
					if (value instanceof MapCompositeValueSupport) {
						MapCompositeValueSupport map = (MapCompositeValueSupport)value;
						if (key.equals("config-property") || key.equals("connection-properties") || key.equals("xa-datasource-properties")) { //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
							for (String subKey:map.getMetaType().keySet()) {
								MetaValue subValue = map.get(subKey);
								if (subValue.getMetaType().isSimple()) {
									data.addProperty(subKey, ManagedUtil.stringValue(subValue));
								}
							}
						}
					}
				}
			}
		}
	}	
}
