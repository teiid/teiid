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

package com.metamatrix.common.config.api;

import com.metamatrix.common.namedobject.IDVerifier;
import com.metamatrix.core.util.Assertion;

public class DeployedComponentID extends ComponentObjectID {

    private final ConfigurationID configID;
    private final HostID hostID;
    private final VMComponentDefnID vmID;
    private final ServiceComponentDefnID serviceID;

    /**
     * Instantiate a VM Deployed Component ID 
     */
    public DeployedComponentID(String name, ConfigurationID configId, HostID hostId, VMComponentDefnID vmId) {
        super(DeployedComponentID.createDeployedName(name, configId, hostId, vmId));
        this.configID = configId;
        this.hostID = hostId;
        this.vmID = vmId;
        this.serviceID = null;
    }

    /**
     * Instantiate a Service or Connector Binding deployed service, that incorporates the
     * PSC name into it
     */
    public DeployedComponentID(String name, ConfigurationID configId, HostID hostId, VMComponentDefnID vmId, ServiceComponentDefnID serviceId) {
        super(DeployedComponentID.createDeployedName(name, configId, hostId, vmId,  serviceId));
        this.configID = configId;
        this.hostID = hostId;
        this.vmID = vmId;
        this.serviceID = serviceId;
    }

    /**
     * Responsible for creating the structuring VM id for this deployed component
     */
    private static final String createDeployedName(String name, ConfigurationID configID, HostID hostID, VMComponentDefnID vmComponentID) {
		Assertion.isNotNull(configID);
		Assertion.isNotNull(name);
		Assertion.isNotNull(hostID);
		Assertion.isNotNull(vmComponentID);

        StringBuffer sb = new StringBuffer(configID.getName());
        sb.append(IDVerifier.DELIMITER_CHARACTER);
        sb.append(hostID.getName());
        sb.append(IDVerifier.DELIMITER_CHARACTER);
        sb.append(vmComponentID.getName());

        sb.append(IDVerifier.DELIMITER_CHARACTER);
        sb.append(name);

        return sb.toString();

    }

    /**
     * Responsible for creating the structuring Service id for this deployed component
     */
    private static final String createDeployedName(String name, ConfigurationID configID, HostID hostID, VMComponentDefnID vmComponentID, ServiceComponentDefnID serviceComponentID) {
		Assertion.isNotNull(configID);
		Assertion.isNotNull(hostID);
		Assertion.isNotNull(vmComponentID);
		Assertion.isNotNull(serviceComponentID);
        
  
        StringBuffer sb = new StringBuffer(configID.getName());
        sb.append(IDVerifier.DELIMITER_CHARACTER);
        sb.append(hostID.getName());
        sb.append(IDVerifier.DELIMITER_CHARACTER);
        sb.append(vmComponentID.getName());
        
        sb.append(IDVerifier.DELIMITER_CHARACTER);
        sb.append(serviceComponentID.getName());
 
        
        sb.append(IDVerifier.DELIMITER_CHARACTER);
        sb.append(name);


        return sb.toString();
    }

    public ConfigurationID getConfigID() {
        return configID;
    }

    public HostID getHostID() {
        return hostID;
    }
    public VMComponentDefnID getVMID() {
        return vmID;
    }
    public ServiceComponentDefnID getServiceID() {
        return serviceID;
    }
}

