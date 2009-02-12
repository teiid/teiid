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

import com.metamatrix.core.util.HashCodeUtil;
import com.metamatrix.platform.service.ServiceMessages;
import com.metamatrix.platform.service.ServicePlugin;

/**
 * A key class that hides the hideousness of policy naming and serves to
 * store and retreive <code>ServiceSelectionPolicies</code>.<br>
 * This class is also used for <code>ServiceSelectionPolicies</code> identification.<p>
 * <p>
 * Once instantiated, this object can never change. Thus, the hashCode is computed
 * and cached.
 */
class ServiceSelectionPolicyKey implements Comparable {

    /**
     * The cached value of the hash code for this object.
     */
    private int hashCode;

    /**
     * Cache the service type name
     */
    private final String serviceTypeName;

    /**
     * Cache the service selection policy type
     */
    private final String policyType;

    /**
     * Sets the hashcode for this key object using given parameters.  Once
     * set, the hashcode never changes.
     * @parm serviceTypeName The name the proxy goes by. A combination of
     * service type and service subtype.
     * @param policyType One of those listed in {@link ServiceSelectionPolicy}.
     * @param prefersLocal Indicates whether this proxy prefers to use a service
     * that's local to thiis VM.
     */
    ServiceSelectionPolicyKey(String serviceTypeName, String policyType) {
        int hashCode = HashCodeUtil.hashCode(0, serviceTypeName);
        hashCode = HashCodeUtil.hashCode(hashCode, policyType);
        this.hashCode = hashCode;

        this.serviceTypeName = serviceTypeName;
        this.policyType = policyType;
    }

    /**
     * Overrides Object hashCode method.
     * @return a hash code value for this object.
     * @see Object#hashCode()
     * @see Object#equals(Object)
     */
    public int hashCode() {
        return this.hashCode;
    }

    /**
     * Returns true if the specified object is semantically equal to this instance.
     * Note:  this method is consistent with <code>compareTo()</code>.
     * <p>
     * @param obj the object that this instance is to be compared to.
     * @return whether the object is equal to this object.
     */
    public boolean equals(Object obj) {
        // Check if instances are identical ...
        if (this == obj) {
            return true;
        }
        // Check if object can be compared to this one
        // (this includes checking for null ) ...
        if (obj instanceof ServiceSelectionPolicyKey) {
            return compare(this, (ServiceSelectionPolicyKey)obj) == 0;
        }

        // Otherwise not comparable ...
        return false;
    }

    /**
     * Compares this ServiceSelectionPolicyKey to another Object. If the Object is an
     * Note:  this method is consistent with<code>equals()</code>.
     * @param obj the object that this instance is to be compared to.
     * @return a negative integer, zero, or a positive integer as this object
     *      is less than, equal to, or greater than the specified object, respectively.
     * @throws ClassCastException if the specified object's type prevents it
     *      from being compared to this AuthorizationPermission.
     */
    public int compareTo(Object o) throws ClassCastException {
        // Check if instances are identical ...
        if (this == o) {
            return 0;
        }
        if (o == null) {
            throw new IllegalArgumentException(ServicePlugin.Util.getString(ServiceMessages.SERVICE_0061));
        }

        // Check if object cannot be compared to this one
        // (this includes checking for null ) ...
        if (!(o instanceof ServiceSelectionPolicyKey)) {
            throw new ClassCastException(ServicePlugin.Util.getString(ServiceMessages.SERVICE_0062, o.getClass()));
        }

        // Check if everything else is equal ...
        return compare(this, (ServiceSelectionPolicyKey)o);
    }

    /**
     * Utility method to compare two ServiceSelectionPolicyKey instances.  Returns a negative integer, zero,
     * or a positive integer as this object is less than, equal to, or greater than
     * the specified object. <p>
     *
     * This method assumes that all type-checking has already been performed. <p>
     *
     * @param obj1 the first ServiceSelectionPolicyKey to be compared
     * @param obj2 the second ServiceSelectionPolicyKey to be compared
     * @return -1, 0, +1 based on whether obj1 is less than, equal to, or greater than obj2
     */
    static public final int compare(ServiceSelectionPolicyKey obj1, ServiceSelectionPolicyKey obj2) {
        // Because the hash codes were computed using the attributes,
        // returning the difference in the hash code values will give a
        // consistent (but NOT lexicographical) ordering for both equals and compareTo.

        // If the hash codes are different, then simply return the difference
        if (obj1.hashCode != obj2.hashCode) {
            return obj1.hashCode - obj2.hashCode;
        }

        // TODO: compare other fields?
        // If the hash codes are the same, consider them equal
        return 0;
    }

    /**
     * Returns a string representing this object.
     */
    public String toString() {
        StringBuffer s = new StringBuffer();
        s.append("("); //$NON-NLS-1$
        s.append(this.getClass().getName());
        s.append(" ServiceName <"); //$NON-NLS-1$
        s.append(this.serviceTypeName);
        s.append("> PolicyType <"); //$NON-NLS-1$
        s.append(this.policyType);
        s.append("> Local <"); //$NON-NLS-1$
        s.append(">)"); //$NON-NLS-1$
        return s.toString();
    }

    /**
     * Return the service selection policy type.
     * @see com.metamatrix.platform.service.proxy.ServiceSelectionPolicy
     */
    String getPolicyType() {
        return policyType;
    }

    /**
     * Return the name of the service type that this selection policy uses.
     * @return the service type name for this policy.
     */
    String getServiceTypeName() {
        return serviceTypeName;
    }
}  // End class ServiceSelectionPolicyKey

