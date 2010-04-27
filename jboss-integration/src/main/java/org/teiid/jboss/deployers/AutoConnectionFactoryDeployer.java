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
import java.util.Properties;
import java.util.Set;

import javax.resource.ResourceException;

import org.jboss.deployers.spi.DeploymentException;
import org.jboss.deployers.spi.deployer.helpers.AbstractSimpleRealDeployer;
import org.jboss.deployers.spi.deployer.managed.ManagedObjectCreator;
import org.jboss.deployers.structure.spi.DeploymentUnit;
import org.jboss.managed.api.ManagedObject;
import org.jboss.managed.api.factory.ManagedObjectFactory;
import org.jboss.resource.metadata.mcf.ManagedConnectionFactoryDeploymentMetaData;
import org.jboss.resource.metadata.mcf.ManagedConnectionFactoryPropertyMetaData;
import org.teiid.adminapi.ConnectionFactory;
import org.teiid.adminapi.impl.ConnectionFactoryMetaData;
import org.teiid.connector.api.Connector;
import org.teiid.connector.api.ConnectorEnvironment;
import org.teiid.connector.basic.BasicManagedConnectionFactory;
import org.teiid.deployers.VDBStatusChecker;
import org.teiid.dqp.internal.datamgr.impl.ConnectorManager;
import org.teiid.dqp.internal.datamgr.impl.ConnectorManagerRepository;
import org.teiid.jboss.IntegrationPlugin;

import com.metamatrix.common.log.LogConstants;
import com.metamatrix.common.log.LogManager;
import com.metamatrix.common.util.PropertiesUtils;
import com.metamatrix.core.MetaMatrixCoreException;
import com.metamatrix.core.util.ReflectionHelper;

/**
 * If the data source's "-ds.xml" file contains couple of extra Teiid Connection Factory properties, this deployer
 * creates a stateful "Connector" and creates the "ConnectorManager" using the "Connector" created. In this case the 
 * "Connector" is not the JCA component. No lookup done for the "Connector". This way Teiid can combine the creation of
 * data source and connection factory in one step.
 */
public class AutoConnectionFactoryDeployer extends AbstractSimpleRealDeployer<AutoConnectionFactoryDeploymentGroup> implements ManagedObjectCreator {
	
	private ConnectorManagerRepository connectorManagerRepository;
	private VDBStatusChecker vdbChecker;
	private ManagedObjectFactory mof;
	
	public AutoConnectionFactoryDeployer() {
		super(AutoConnectionFactoryDeploymentGroup.class);
		setRelativeOrder(3000);
	}

	@Override
	public void deploy(DeploymentUnit unit, AutoConnectionFactoryDeploymentGroup group) throws DeploymentException {
		List<ManagedConnectionFactoryDeploymentMetaData> deployments = group.getDeployments();
		
		ConnectionFactoryMetadataGroup cfGroup = new ConnectionFactoryMetadataGroup();
		
		for (ManagedConnectionFactoryDeploymentMetaData data : deployments) {
			String connectorName = "java:"+data.getJndiName(); //$NON-NLS-1$
			ConnectorManager cm = null;

			// check if a Teiid connection factory needs to be created for this data source.
			List<ManagedConnectionFactoryPropertyMetaData> props = data.getManagedConnectionFactoryProperties();
			
			String cfClass = getPropertyValue("ConnectionFactoryClass", props); //$NON-NLS-1$	
			if (cfClass != null) {
				Properties teiidProps = convertToProperties(props);
				
				// This indicates that a Teiid Connection factory 
				Connector connector = buildTeiidConnector(cfClass, teiidProps, connectorName);
				cm = new ConnectorManager(connectorName, connector, data.getMaxSize());
				
				cm.start();
	
				// Add the references to the mgr as loaded.
	            this.connectorManagerRepository.addConnectorManager(connectorName, cm);   
	            cfGroup.addConnectionFactory(ConnectionFactoryDeployer.buildCF(data));
	            
	            this.vdbChecker.connectorAdded(connectorName);
	            LogManager.logInfo(LogConstants.CTX_RUNTIME, IntegrationPlugin.Util.getString("connector_started", connectorName)); //$NON-NLS-1$
			}
		}
		
		if (!cfGroup.getConnectionFactories().isEmpty()) {
			unit.addAttachment(ConnectionFactoryMetadataGroup.class, cfGroup);
		}		
	}
	
	private String getPropertyValue(String name, List<ManagedConnectionFactoryPropertyMetaData> props) {
		for (ManagedConnectionFactoryPropertyMetaData prop:props) {
			if (prop.getName().equals(name)) {
				return prop.getValue();
			}
		}
		return null;
	}
	
	private Properties convertToProperties(List<ManagedConnectionFactoryPropertyMetaData> props) {
		Properties convertedProps = new Properties();
		for (ManagedConnectionFactoryPropertyMetaData prop:props) {
			convertedProps.setProperty(prop.getName(), prop.getValue());
		}
		return convertedProps;
	}	

	private Connector buildTeiidConnector(String cfClass, Properties props, String sourceJndiName) throws DeploymentException {
		BasicManagedConnectionFactory cf;
		try {
			Object o = ReflectionHelper.create(cfClass, null, Thread.currentThread().getContextClassLoader());
			if(!(o instanceof ConnectorEnvironment)) {
				throw new DeploymentException(IntegrationPlugin.Util.getString("invalid_class", cfClass));//$NON-NLS-1$	
			}
			cf = (BasicManagedConnectionFactory)o;
			
			PropertiesUtils.setBeanProperties(cf, props, null);
			cf.setSourceJNDIName(sourceJndiName);
			
			return (Connector)cf.createConnectionFactory();
			
		} catch (MetaMatrixCoreException e) {
			throw new DeploymentException(e);
		} catch (ResourceException e) {
			throw new DeploymentException(e);
		}
	}
    
	public void setConnectorManagerRepository(ConnectorManagerRepository repo) {
		this.connectorManagerRepository = repo;
	}

	/*
	 * undeploy covered by the other deployer already.
	@Override
	public void undeploy(DeploymentUnit unit, AutoConnectionFactoryDeploymentGroup group) {
		super.undeploy(unit, group);
		List<ManagedConnectionFactoryDeploymentMetaData> deployments = group.getDeployments();

		for (ManagedConnectionFactoryDeploymentMetaData data : deployments) {
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
	*/
	
	@Override
	public void build(DeploymentUnit unit, Set<String> attachmentNames, Map<String, ManagedObject> managedObjects)
			throws DeploymentException {
		
		ConnectionFactoryMetadataGroup group = unit.getAttachment(ConnectionFactoryMetadataGroup.class);
		if (group != null) {
			for (ConnectionFactoryMetaData data : group.getConnectionFactories()) {
				ManagedObject mo = this.mof.initManagedObject(data, ConnectionFactory.class, data.getName(), data.getName());
				if (mo == null) {
					throw new DeploymentException("could not create managed object"); //$NON-NLS-1$
				}
				managedObjects.put(mo.getName(), mo);					
			}
		}
	}
	
	public void setVDBStatusChecker(VDBStatusChecker vdbChecker) {
		this.vdbChecker = vdbChecker;
	}	
	
	public void setManagedObjectFactory(ManagedObjectFactory mof) {
		this.mof = mof;
	}
}
