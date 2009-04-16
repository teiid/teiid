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

import com.metamatrix.common.config.model.ConfigurationVisitor;

/**
 * <p>DeployedComponent is a wrapper for a <code>ComponentDefn</code> -
 * it represents a component that is declared deployed
 * to a specied machine.  Therefore, once deployed, a component cannot be
 * changed.</p>
 *
 * <p>A DeployedComponent instance can represent one of two things:
 * <ol>
 * <li>It can represent a deployed VMComponentDefn, in which case the
 * <code>getServiceComponentDefnID</code> and <code>getProductServiceConfigID</code>
 * methods will both return null.  Also, both the
 * <code>getDeployedComponentDefnID</code> and the
 * <code>getVMComponentDefnID</code> methods will return identical VMComponentDefnID
 * objects, representing the ComponentDefn object from which this object was
 * deployed.
 * </li>
 *
 * <li>It can represent a deployed ServiceComponentDefnID, in which case the
 * <code>getVMComponentDefnID</code> and <code>getProductServiceConfigID</code>
 * methods will both return the appropriate ID object.  Also, both the
 * <code>getDeployedComponentDefnID</code> and the
 * <code>getServiceComponentDefnID</code> methods will return identical
 * ServiceComponentDefnID objects, representing the ComponentDefn object from
 * which this object was deployed.
 * </li>
 * </ol></p> 
 *
 * <p>As you will notice, there is no reference held to a <code>Configuration</code>.
 * This is done so that a <code>DeployedComponent</code> can be serialized
 * independently from having to also serialize the whole configuration of objects.</p>
 */
public interface DeployedComponent extends ComponentObject {

    /**
     * The SERVICE_UID_FOR_DEPLOYED_VM is the default value assigned in the database
     * when the deployed component is a VM.
     */
    public static final Long SERVICE_UID_FOR_DEPLOYED_VM = new Long(0);
    
    /**
     * Indicates if the deployed component is enabled for starting.
     * @return true if the deployed component is enabled for starting
     */
    boolean isEnabled();

    /**
     * <p>
     * Indicates whether this object represents a deployed
     * {@link ServiceComponentDefn defn} (returns true) or
     * a deployed vm component definition (returns false).
     * </p>
     * <p>
     * To determine if the service is a connector or not,
     * use the {@see #isDeployedConnector} method.
     * </p>
     * @return true if this is a deployed service, false if it is
     * a deployed vm
     */
    boolean isDeployedService();
    
    
    /**
     * Indicates whether this object represents a deployed
     * connector.  If {@see #isDeployedService } returns false,
     * this will always return false.
     * @return true if this is a deployed connector.
     */
    boolean isDeployedConnector();

    /**
     * Returns the <code>ComponentID</code> for the service component that is deployed.
     * Null will be returned if this <code>DeployedComponent</code> represents
     * either a a deployed VM.
     * @return the component id, null when this is a deployed VM
     */
    public ServiceComponentDefnID getServiceComponentDefnID();

    /**
     * Returns the <code>ComponentID</code> for the VM that this component
     * is deployed on, or if this object represents the deployed VM itself.
     * @return the vm id
     */
    public VMComponentDefnID getVMComponentDefnID();

    /**
     * Returns the <code>ConfigurationID</code> indicating the configuration
     * under which this deployed component belongs.
     * @return the configuration id
     */
    public ConfigurationID getConfigurationID();

    /**
     * Returns the <code>HostID</code> for the Host that this component
     * is deployed on.
     * @return the host id
     */
    public HostID getHostID();

    /**
     * Returns the <code>ComponentDefnID</code> for the component that is
     * deployed.  This can either be a <code>ServiceComponentDefnId</code>
     * or a <code>VMComponentDefnID</code>.
     * @return ComponentDefnID that is represented as the deployed component
     */
    public ComponentDefnID getDeployedComponentDefnID();

    /**
     * Returns the <code>ComponentDefn</code> that is deployed.
     */
    public ComponentDefn getDeployedComponentDefn(Configuration configuration);
        

    public void accept(ConfigurationVisitor visitor);
}

