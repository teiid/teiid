/*
 * JBoss, Home of Professional Open Source.
 * Copyright (C) 2008 Red Hat, Inc.
 * Copyright (C) 2000-2007 MetaMatrix, Inc.
 * Licensed to Red Hat, Inc. under one or more contributor 
 * license agreements.  See the copyright.txt file in the
 * distribution for a full listing of individual contributors.
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

import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import com.metamatrix.platform.registry.ServiceRegistryBinding;
import com.metamatrix.platform.service.ServiceMessages;
import com.metamatrix.platform.service.ServicePlugin;
import com.metamatrix.platform.service.api.ServiceID;
import com.metamatrix.platform.service.api.ServiceInterface;
import com.metamatrix.platform.service.api.exception.ServiceNotFoundException;

/**
 * Represents a selection policy for services of a given type.<br>
 */
public abstract class ServiceSelectionPolicy {

    // The policy types
    static final int RANDOM_SELECTION_POLICY_TYPE            = 0;
    static final int RANDOM_LOCAL_SELECTION_POLICY_TYPE      = 1;
    static final int ROUND_ROBIN_SELECTION_POLICY_TYPE       = 2;
    static final int ROUND_ROBIN_LOCAL_SELECTION_POLICY_TYPE = 3;

    /** The service selection policy type names */
    static final String RANDOM_SELECTION_POLICY_NAME            = ServiceProxyProperties.RANDOM_SELECTION_POLICY_NAME;
    static final String RANDOM_LOCAL_SELECTION_POLICY_NAME      = ServiceProxyProperties.RANDOM_LOCAL_SELECTION_POLICY_NAME;
    static final String ROUND_ROBIN_SELECTION_POLICY_NAME       = ServiceProxyProperties.ROUND_ROBIN_SELECTION_POLICY_NAME;
    static final String ROUND_ROBIN_LOCAL_SELECTION_POLICY_NAME = ServiceProxyProperties.ROUND_ROBIN_LOCAL_SELECTION_POLICY_NAME;

    private static Map policyTypes;
    private static Map policyNames;

    static {
        policyTypes = new HashMap();
        policyTypes.put(RANDOM_SELECTION_POLICY_NAME,            new Integer(RANDOM_SELECTION_POLICY_TYPE));
        policyTypes.put(RANDOM_LOCAL_SELECTION_POLICY_NAME,      new Integer(RANDOM_LOCAL_SELECTION_POLICY_TYPE));
        policyTypes.put(ROUND_ROBIN_SELECTION_POLICY_NAME,       new Integer(ROUND_ROBIN_SELECTION_POLICY_TYPE));
        policyTypes.put(ROUND_ROBIN_LOCAL_SELECTION_POLICY_NAME, new Integer(ROUND_ROBIN_LOCAL_SELECTION_POLICY_TYPE));

        policyNames = new HashMap();
        policyNames.put(new Integer(RANDOM_SELECTION_POLICY_TYPE),              RANDOM_SELECTION_POLICY_NAME);
        policyNames.put(new Integer(RANDOM_LOCAL_SELECTION_POLICY_TYPE),        RANDOM_LOCAL_SELECTION_POLICY_NAME);
        policyNames.put(new Integer(ROUND_ROBIN_SELECTION_POLICY_TYPE),         ROUND_ROBIN_SELECTION_POLICY_NAME);
        policyNames.put(new Integer(ROUND_ROBIN_LOCAL_SELECTION_POLICY_TYPE),   ROUND_ROBIN_LOCAL_SELECTION_POLICY_NAME);
    }

    /**
     * Get the policy type given a policy name.
     * @param policyTypeName The name of a type of <code>ServiceSelectionPolicy</code>.
     * @return The <code>ServiceSelectionPolicy</code> type.
     */
    static int getPolicyTypeFromName(String policyTypeName) {
        Integer policyType = (Integer) policyTypes.get(policyTypeName.toUpperCase());

        if ( policyType == null ) {
            throw new IllegalArgumentException(ServicePlugin.Util.getString(ServiceMessages.SERVICE_0059,  policyTypeName, policyTypeName.toUpperCase()));
        }

        return policyType.intValue();
    }

    /**
     * Get the policy name given a policy type.
     * @param policyType The type of a <code>ServiceSelectionPolicy</code>.
     * @return The <code>ServiceSelectionPolicy</code> name.
     */
    static String getPolicyNameFromType(int policyType) {
        String policyTypeName = (String) policyNames.get(new Integer(policyType));

        if ( policyTypeName == null ) {
            throw new IllegalArgumentException(ServicePlugin.Util.getString(ServiceMessages.SERVICE_0060, policyType));
        }

        return policyTypeName;
    }

    /**
     * Determine if the service selection policy has a local preference by
     * examining its type.
     * @param policyType The service selection policy type. One of the types
     * given above.
     * @return true if the policy type is one of the local selection policy types.
     */
    static boolean isPolicyPreferenceLocal(int policyType) {
        boolean isLocal = false;

        switch(policyType) {
            case RANDOM_LOCAL_SELECTION_POLICY_TYPE: {
                isLocal = true;
                break;
            }
            case ROUND_ROBIN_LOCAL_SELECTION_POLICY_TYPE: {
                isLocal = true;
                break;
            }
            default: {
                // Do nothing, isLocal will still be false.
            }
        }

        return isLocal;
    }

    /**
     * Create a <code>ServiceSelectionPolicy</code> given its type.
     * @param policyType The type of a type of <code>ServiceSelectionPolicy</code>.
     * @return The created <code>ServiceSelectionPolicy</code>.
     */
    static ServiceSelectionPolicy createPolicy(int policyType) {
        ServiceSelectionPolicy policy = null;

        switch(policyType) {
            case RANDOM_SELECTION_POLICY_TYPE: {
                policy = new RandomSelectionPolicy();
                break;
            }
            case ROUND_ROBIN_SELECTION_POLICY_TYPE: {
                policy = new RoundRobinSelectionPolicy();
                break;
            }
            case RANDOM_LOCAL_SELECTION_POLICY_TYPE: {
                policy = new RandomLocalSelectionPolicy();
                break;
            }
            case ROUND_ROBIN_LOCAL_SELECTION_POLICY_TYPE: {
                policy = new RoundRobinLocalSelectionPolicy();
                break;
            }
        }

        if ( policy == null ) {
            throw new IllegalArgumentException(ServicePlugin.Util.getString(ServiceMessages.SERVICE_0060, policyType));
        }

        return policy;
    }

    /**
     * Return the ID for the service.
     */
    ServiceID getIDForService(ServiceInterface service) {

        Iterator iter = null;
        try {
            iter = this.getInstances().iterator();
        } catch (ServiceNotFoundException e) {
            return null;
        }

        while (iter.hasNext()) {
            ServiceRegistryBinding binding = (ServiceRegistryBinding) iter.next();
            if (binding.getService() == service) {
                return binding.getServiceID();
            }
        }
        return null;
    }

    /**
     * Create a <code>ServiceSelectionPolicy</code> given its name.
     * @param policyTypeName The name of a type of <code>ServiceSelectionPolicy</code>.
     * @return The created <code>ServiceSelectionPolicy</code>.
     */
    static ServiceSelectionPolicy createPolicy(String policyTypeName) {
        int policyType = ServiceSelectionPolicy.getPolicyTypeFromName(policyTypeName);
        ServiceSelectionPolicy policy = null;

        policy = ServiceSelectionPolicy.createPolicy(policyType);
        return policy;
    }

    /**
     * Get the name of the policy - useful for logging/debugging.
     */
    public abstract String getServiceSelectionPolicyName();

    /**
     * Return whether or not the policy preference is local.
     * @return the local preference of this policy.
     */
    public abstract boolean prefersLocal();

    /**
     * Get the next service instance from the given service selection policy.
     * @return The <code>ServiceRegistryBinding</code> for the next instance after
     * being operated on by the selection policy.
     * @see com.metamatrix.common.service.ServiceInterface
     * @throws ServiceNotFoundException if the policy has no more services to
     * hand out.
     */
    public abstract ServiceRegistryBinding getNextInstance() throws ServiceNotFoundException;

    /**
     * Get list of instances from the given service selection policy.
     * @return The List of <code>ServiceRegistryBinding</code> objects for the instances after
     * being operated on by the selection policy.
     * @see com.metamatrix.common.service.ServiceInterface
     * @throws ServiceNotFoundException if the policy has no more services to
     * hand out.
     */
    public abstract List<ServiceRegistryBinding> getInstances() throws ServiceNotFoundException;

}





