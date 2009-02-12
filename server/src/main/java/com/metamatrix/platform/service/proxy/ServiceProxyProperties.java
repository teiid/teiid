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

package com.metamatrix.platform.service.proxy;

/**
 * Holds definition of all proxy property names.
 */
public class ServiceProxyProperties {

    /**
     * Required property denoting the class for instantiation of the service proxy.
     */
    public static final String SERVICE_PROXY_CLASS_NAME = "metamatrix.core.proxy.serviceProxyClassName"; //$NON-NLS-1$

    /**
     * Required property denoting the name of the service selection policy.
     */
    public static final String SERVICE_SELECTION_POLICY_NAME = "metamatrix.core.proxy.serviceSelectionPolicyName"; //$NON-NLS-1$

    /**
     * Optional boolean to force a service to use the same instance. defaults to false. this is not meaningful for multiple delegations 
     */
    public static final String SERVICE_SELECTION_STICKY = "metamatrix.core.proxy.serviceSelectionSticky"; //$NON-NLS-1$
    
    /**
     * Optional boolean to denote a multiple delegation proxy (will delegate to all instances). defaults to false.
     */
    public static final String SERVICE_MULTIPLE_DELEGATION = "metamatrix.core.proxy.serviceMultipleDelegation"; //$NON-NLS-1$

}
