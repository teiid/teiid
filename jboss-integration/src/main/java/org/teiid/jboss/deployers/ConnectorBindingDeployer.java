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
import org.jboss.logging.Logger;
import org.jboss.managed.api.ManagedObject;
import org.jboss.managed.api.factory.ManagedObjectFactory;
import org.jboss.resource.metadata.mcf.ManagedConnectionFactoryDeploymentGroup;
import org.jboss.resource.metadata.mcf.ManagedConnectionFactoryDeploymentMetaData;
import org.teiid.connector.api.Connector;
import org.teiid.dqp.internal.datamgr.impl.ConnectorManager;
import org.teiid.dqp.internal.datamgr.impl.ConnectorManagerRepository;
import org.teiid.security.SecurityHelper;

public class ConnectorBindingDeployer extends AbstractSimpleRealDeployer<ManagedConnectionFactoryDeploymentGroup> implements ManagedObjectCreator {
	protected Logger log = Logger.getLogger(getClass());
	private ManagedObjectFactory mof;
	private SecurityHelper securityHelper;
	
	private ConnectorManagerRepository connectorManagerRepository;
	
	public ConnectorBindingDeployer() {
		super(ManagedConnectionFactoryDeploymentGroup.class);
		setRelativeOrder(3000);
	}

	@Override
	public void deploy(DeploymentUnit unit, ManagedConnectionFactoryDeploymentGroup group) throws DeploymentException {
		List<ManagedConnectionFactoryDeploymentMetaData> deployments = group.getDeployments();
		
		ConnectorManagerGroup cmGroup = new ConnectorManagerGroup();
		
		for (ManagedConnectionFactoryDeploymentMetaData data : deployments) {
			String connectorDefinition = data.getConnectionDefinition();
			if (connectorDefinition.equals(Connector.class.getName())) {
				String connectorName = data.getJndiName();

				ConnectorManager cm = createConnectorManger("java:"+connectorName, data.getMaxSize()); //$NON-NLS-1$
				cm.start();
				cmGroup.addConnectorManager(cm);

				// Add the references to the mgr as loaded.
	            this.connectorManagerRepository.addConnectorManager("java:"+connectorName, cm);  //$NON-NLS-1$    
	            
	            log.info("Teiid Connector Started = " + connectorName); //$NON-NLS-1$
			}
		}
		
		if (!cmGroup.getConnectorManagers().isEmpty()) {
			unit.addAttachment(ConnectorManagerGroup.class, cmGroup);
		}
	}


    ConnectorManager createConnectorManger(String deployedConnectorName, int maxThreads) {
        ConnectorManager mgr = new ConnectorManager(deployedConnectorName, maxThreads, securityHelper);       
        return mgr;
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
				String connectorName = data.getJndiName();
				if (this.connectorManagerRepository != null) {
					ConnectorManager cm = this.connectorManagerRepository.removeConnectorManager("java:"+connectorName); //$NON-NLS-1$
					if (cm != null) {
						cm.stop();
					}
				}
				log.info("Teiid Connector Stopped = " + connectorName); //$NON-NLS-1$
			}
		}
	}

	@Override
	public void build(DeploymentUnit unit, Set<String> attachmentNames, Map<String, ManagedObject> managedObjects)
			throws DeploymentException {
		
		ConnectorManagerGroup cmGroup = unit.removeAttachment(ConnectorManagerGroup.class);
		if (cmGroup != null) {
			for (ConnectorManager mgr:cmGroup.getConnectorManagers()) {
				ManagedObject mo = this.mof.initManagedObject(mgr, ConnectorManager.class, mgr.getName(), mgr.getName());
				if (mo == null) {
					throw new DeploymentException("could not create managed object"); //$NON-NLS-1$
				}
				managedObjects.put(mo.getName(), mo);				
			}
		}
	}
	
	public void setManagedObjectFactory(ManagedObjectFactory mof) {
		this.mof = mof;
	}	
	
	public void setSecurityHelper(SecurityHelper securityHelper) {
		this.securityHelper = securityHelper;
	}
}
