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

package com.metamatrix.dqp.service;

import java.util.*;

import com.metamatrix.admin.api.exception.security.InvalidSessionException;
import com.metamatrix.api.exception.MetaMatrixComponentException;
import com.metamatrix.api.exception.security.AuthorizationException;
import com.metamatrix.api.exception.security.AuthorizationMgmtException;
import com.metamatrix.common.application.ApplicationEnvironment;
import com.metamatrix.common.application.exception.ApplicationInitializationException;
import com.metamatrix.common.application.exception.ApplicationLifecycleException;
import com.metamatrix.platform.security.api.AuthorizationPolicy;
import com.metamatrix.platform.security.api.AuthorizationRealm;
import com.metamatrix.platform.security.api.MetaMatrixPrincipalName;
import com.metamatrix.platform.security.api.SessionToken;

/**
 */
public class FakeAuthorizationService implements AuthorizationService {

    // Inaccessible resources
    private Set knownResources = new HashSet();
    private boolean defaultAllow;

    public FakeAuthorizationService(boolean defaultAllow) {
        this.defaultAllow = defaultAllow;
    }

    public void addResource(String connectionID, int action, String resource) {
        knownResources.add(new Resource(connectionID, action, resource));
    }

    /*
     * @see com.metamatrix.dqp.service.AuthorizationService#getInaccessibleResources(java.lang.String, int, java.util.Collection, int)
     */
    public Collection getInaccessibleResources(String connectionID, int action, Collection resources, int context)
        throws MetaMatrixComponentException {

        List found = new ArrayList();
        
        if (resources.isEmpty()) {
            throw new MetaMatrixComponentException("expected resources"); //$NON-NLS-1$
        }

        Iterator rIter = resources.iterator();
        while(rIter.hasNext()) {
            String resourceName = (String) rIter.next();

            Resource key = new Resource(connectionID, action, resourceName);
            
            boolean foundResource = knownResources.contains(key);
            if (!foundResource && !defaultAllow) {
                found.add(resourceName);
            } else if (foundResource && defaultAllow) {
                found.add(resourceName);
            }
        }

        return found;
    }

    /**
     * Determine whether entitlements checking is enabled on the server.
     *
     * @return <code>true</code> iff server-side entitlements checking is enabled.
     */
    public boolean checkingEntitlements() {
        return true;
    }

    /*
     * @see com.metamatrix.common.application.ApplicationService#initialize(java.util.Properties)
     */
    public void initialize(Properties props) throws ApplicationInitializationException {

    }

    /*
     * @see com.metamatrix.common.application.ApplicationService#start(com.metamatrix.common.application.ApplicationEnvironment)
     */
    public void start(ApplicationEnvironment environment) throws ApplicationLifecycleException {

    }

    /*
     * @see com.metamatrix.common.application.ApplicationService#stop()
     */
    public void stop() throws ApplicationLifecycleException {

    }

    private static class Resource {
        public String connectionID;
        public int action;
        public String resource;

        public Resource(String connectionID, int action, String resource) {
            this.connectionID = connectionID;
            this.action = action;
            this.resource = resource;
        }

        public String toString() {
            return resource;
        }
        
        /** 
         * @see java.lang.Object#hashCode()
         */
        public int hashCode() {
            return resource.hashCode() * action;
        }
        
        /**
         * @see java.lang.Object#equals(java.lang.Object)
         */
        public boolean equals(Object obj) {
            if (obj == this) {
                return true;
            }

            if (!(obj instanceof Resource)) {
                return false;
            }

            Resource other = (Resource)obj;

            return other.action == this.action
                   && other.connectionID.equalsIgnoreCase(this.connectionID)
                   && other.resource.equalsIgnoreCase(this.resource);
        }
    }

    /** 
     * @see com.metamatrix.dqp.service.AuthorizationService#hasRole(java.lang.String, java.lang.String, java.lang.String)
     */
    public boolean hasRole(String connectionID,
                           String roleType,
                           String roleName) throws MetaMatrixComponentException {
        return false;
    }

	@Override
	public boolean isCallerInRole(SessionToken session, String roleName)
			throws AuthorizationMgmtException {
		return false;
	}

	@Override
	public Collection<AuthorizationPolicy> getPoliciesInRealm(
			AuthorizationRealm realm)
			throws AuthorizationException, AuthorizationMgmtException {
		return null;
	}

	@Override
	public Collection<String> getRoleNamesForPrincipal(MetaMatrixPrincipalName principal) throws InvalidSessionException,
			AuthorizationException, AuthorizationMgmtException {
		return null;
	}

	@Override
	public void updatePoliciesInRealm(AuthorizationRealm realm,
			Collection<AuthorizationPolicy> policies)
			throws AuthorizationMgmtException {
		
	}
}
