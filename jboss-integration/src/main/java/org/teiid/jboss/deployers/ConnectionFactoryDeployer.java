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

import org.jboss.deployers.spi.DeploymentException;
import org.jboss.deployers.spi.deployer.helpers.AbstractSimpleRealDeployer;
import org.jboss.deployers.structure.spi.DeploymentUnit;
import org.jboss.resource.metadata.mcf.ManagedConnectionFactoryDeploymentGroup;
import org.jboss.resource.metadata.mcf.ManagedConnectionFactoryDeploymentMetaData;
import org.teiid.deployers.VDBStatusChecker;

/**
 * This deployer listens to the data source load and unload events and manages the connectionManager status based 
 * on these events.
 */
public class ConnectionFactoryDeployer extends AbstractSimpleRealDeployer<ManagedConnectionFactoryDeploymentGroup> {
	
	private VDBStatusChecker vdbChecker;
	
	public ConnectionFactoryDeployer() {
		super(ManagedConnectionFactoryDeploymentGroup.class);
		setRelativeOrder(3000);
	}

	@Override
	public void deploy(DeploymentUnit unit, ManagedConnectionFactoryDeploymentGroup group) throws DeploymentException {
		List<ManagedConnectionFactoryDeploymentMetaData> deployments = group.getDeployments();
		
		for (ManagedConnectionFactoryDeploymentMetaData data : deployments) {
            this.vdbChecker.dataSourceAdded(data.getJndiName());   
		}
	}
    
	@Override
	public void undeploy(DeploymentUnit unit, ManagedConnectionFactoryDeploymentGroup group) {
		super.undeploy(unit, group);
		List<ManagedConnectionFactoryDeploymentMetaData> deployments = group.getDeployments();

		for (ManagedConnectionFactoryDeploymentMetaData data : deployments) {
			this.vdbChecker.dataSourceRemoved(data.getJndiName());
		}
	}
	
	public void setVDBStatusChecker(VDBStatusChecker checker) {
		this.vdbChecker = checker;
	}
}
