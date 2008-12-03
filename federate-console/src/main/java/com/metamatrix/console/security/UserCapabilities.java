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

package com.metamatrix.console.security;

import java.util.HashMap;
import java.util.Map;

import com.metamatrix.admin.api.server.AdminRoles.RoleName;
import com.metamatrix.api.exception.ComponentNotFoundException;
import com.metamatrix.api.exception.MetaMatrixComponentException;
import com.metamatrix.api.exception.security.AuthorizationException;
import com.metamatrix.api.exception.security.InvalidSessionException;
import com.metamatrix.api.exception.security.MetaMatrixSecurityException;
import com.metamatrix.console.connections.ConnectionInfo;
import com.metamatrix.console.models.ModelManager;
import com.metamatrix.console.ui.views.users.RoleDisplay;
import com.metamatrix.console.util.ExternalException;
import com.metamatrix.platform.security.api.MetaMatrixPrincipal;
import com.metamatrix.platform.security.api.MetaMatrixPrincipalName;
import com.metamatrix.platform.security.api.MetaMatrixSessionID;

/**
 * This is a singleton for accessing user capabilities.
 */
public class UserCapabilities {

    /*
     * UserCapabilities implements the same matrix of roles and
     * capabilities that is used on the server side.  This solution is
     * not the one envisioned by the 'Securable' strategy.  That one relied
     * on the server to translate the user's roles into a set of permission
     * policies; the Console would in turn test appropriate policies to
     * decide what actions the user could or could not do.
     *
     * This solution has fewer moving parts, but relies on long-term
     * synchronization of the policy of the server with the policy of the
     * Console as implemented here.
     *
     * Parts of the Console will ask this class whether or not the signed-on
     * user can do a certain action.  Based on the answer that part may not
     * be constructed, or it may be constructed but certain controls may be
     * hidden or greyed out.
     *
     * This implementation is intended to be as transparent as possible, to
     * simplify (and encourage) verification of its rules.  For this reason,
     * and because the rules involved will seldom change, this
     * implementation is not data-driven.  Fancy data structures which would
     * obscure the data are not used.
     * Instead roles are simply represented by
     * booleans.  Capabilities are simply accessed by method calls (one per
     * capability) that return boolean.  The internal logic of these methods
     * just tests the Role booleans.  This should achieve the goal of making
     * the resulting code as easy to verify as possible.
     *
     */
    
    private static UserCapabilities theInstance;

	public static UserCapabilities getInstance() {
		return theInstance;
	}
	
    /**
     * Create the one instance of this class.
     */
    public static UserCapabilities createInstance() 
        	throws AuthorizationException,
        	ComponentNotFoundException, MetaMatrixSecurityException,
        	MetaMatrixComponentException, ExternalException {
		theInstance = new UserCapabilities();
        return theInstance;
    }

	private Map /*<ConnectionInfo to Roles>*/ rolesMap = new HashMap();
	
    private UserCapabilities() {
		super();
	}
	 
    /**
     * Initialize...
     */
    public void init(ConnectionInfo conn)
        throws IllegalArgumentException,
               ComponentNotFoundException,
               AuthorizationException,
               InvalidSessionException,
               MetaMatrixSecurityException,
               MetaMatrixComponentException,
               ExternalException {
               	
        boolean systemAdmin = false;
        boolean systemAdminMMS = false;
        boolean viewOnly = false;
        
        // 1. Get the Session ID
        MetaMatrixSessionID mmsidSessionID = conn.getSessionID();
        // 2. Get the principal
        MetaMatrixPrincipal mmpPrincipal = ModelManager.getSessionAPI(conn).getPrincipal(
                mmsidSessionID);

        // 3. Get this Principal's Roles
        RoleDisplay[] rd = ModelManager.getGroupsManager(conn).getRolesForPrincipal(
                mmpPrincipal, true);

        // From the roles set the Role booleans
        for (int i = 0; i < rd.length; i++) {
            String sRole = rd[i].getName();

            if (sRole.equals(RoleName.ADMIN_SYSTEM)) {
                systemAdmin = true;
            } else if (sRole.equals(RoleName.ADMIN_PRODUCT)) {
                systemAdminMMS = true;
            } else if (sRole.equals(RoleName.ADMIN_READONLY)) {
                viewOnly = true;
            }
        }
        
        Roles roles = new Roles(systemAdmin, systemAdminMMS,
        		viewOnly);
        rolesMap.put(conn, roles);
    }

	public void remove(ConnectionInfo connection) {
		rolesMap.remove(connection);
	}
	
    // Capabilities methods

    public boolean hasAnyRole(ConnectionInfo conn) {
    	Roles roles = (Roles)rolesMap.get(conn);
    	if (roles != null) {
    		return (roles.isSystemAdmin() ||
    				roles.isProductAdmin() || roles.isViewOnly());
    	}
    	return false;
    }
    
    public boolean isProductAdmin(ConnectionInfo conn) {
        Roles roles = (Roles)rolesMap.get(conn);
        if (roles != null) {
            return roles.isProductAdmin();
        }
        return false;
    }
    
    public boolean isSystemAdmin(ConnectionInfo conn) {
        Roles roles = (Roles)rolesMap.get(conn);
        if (roles != null) {
            return roles.isSystemAdmin();
        }
        return false;
    }

    public boolean canViewPrincipalInfo(ConnectionInfo conn) {
    	return hasAnyRole(conn);
    }

    public boolean canViewRoleInfo(ConnectionInfo conn) {
    	return hasAnyRole(conn);
    }

    public boolean canModifyRoleInfo(ConnectionInfo conn) {
    	return isSystemAdmin(conn);
	}

    public boolean canViewLoggingConfig(ConnectionInfo conn) {
        return hasAnyRole(conn);
    }

    public boolean canModifyLoggingConfig(ConnectionInfo conn) {
    	return isSystemAdmin(conn);
	}

    public boolean canViewLogMessages(ConnectionInfo conn) {
        return hasAnyRole(conn);
    }

    public boolean canViewEntitlements(ConnectionInfo conn) {
        return hasAnyRole(conn);
    }

    public boolean canModifyEntitlements(ConnectionInfo conn) {
    	return isSystemAdmin(conn);
    }
    
    public boolean canModifySecurity(ConnectionInfo conn) {
        return isSystemAdmin(conn);
    }

	public boolean canViewMetadataEntitlements(ConnectionInfo conn) {
	    return hasAnyRole(conn);
	}
	
	public boolean canModifyMetadataEntitlements(ConnectionInfo conn) {
    	return isSystemAdmin(conn);
	}
	
    public boolean canViewSystemHealth(ConnectionInfo conn) {
        return hasAnyRole(conn);
    }

    public boolean canViewDeployment(ConnectionInfo conn) {
        return hasAnyRole(conn);
    }

    public boolean canViewRealTimeInfo(ConnectionInfo conn) {
        return hasAnyRole(conn);
    }

    public boolean canViewSessions(ConnectionInfo conn) {
        return hasAnyRole(conn);
    }

    public boolean canModifySessions(ConnectionInfo conn) {
    	return isSystemAdmin(conn);
	}

    public boolean canViewQueries(ConnectionInfo conn) {
        return hasAnyRole(conn);
    }

    public boolean canModifyQueries(ConnectionInfo conn) {
    	return isProductAdmin(conn);
    }

    public boolean canViewServerProperties(ConnectionInfo conn) {
        return hasAnyRole(conn);
    }

    public boolean canModifyServerProperties(ConnectionInfo conn) {
    	return isSystemAdmin(conn);
	}

    public boolean canViewConnectorTypes(ConnectionInfo conn) {
        return hasAnyRole(conn);
    }

    public boolean canViewConnectors(ConnectionInfo conn) {
        return hasAnyRole(conn);
    }

    public boolean canModifyConnectors(ConnectionInfo conn) {
    	return isSystemAdmin(conn);
	}

    public boolean canModifyConnectorBindings(ConnectionInfo conn) {
    	return isSystemAdmin(conn);
	}

    public boolean canViewTransactionInfo(ConnectionInfo conn) {
        return hasAnyRole(conn);
    }

    public boolean canTerminateTransactions(ConnectionInfo conn) {
    	return isProductAdmin(conn);
	}

    public boolean canUpdateConfiguration(ConnectionInfo conn) {
    	return isProductAdmin(conn);
	}

    public boolean canUpdateSystemProperties(ConnectionInfo conn) {
    	return isSystemAdmin(conn);
	}

    public boolean canViewSystemProperties(ConnectionInfo conn) {
        return hasAnyRole(conn);
    }

    public boolean canModifyVDBs(ConnectionInfo conn) {
    	return isProductAdmin(conn);
	}

    public boolean canViewVDBs(ConnectionInfo conn) {
        return hasAnyRole(conn);
    }

    public boolean canViewExtensionSources(ConnectionInfo conn) {
        return hasAnyRole(conn);
    }

    public boolean canModifyExtensionSources(ConnectionInfo conn) {
    	return isSystemAdmin(conn);
	}
    
    public boolean canModifyPools(ConnectionInfo conn) {
        return isSystemAdmin(conn);
	}
    
    public boolean canModifyResources(ConnectionInfo conn) {
        return isSystemAdmin(conn);
	}
    
    /**
     * Indicates if the user can start, stop, bounce, shutdown, etc. different
     * aspects of the runtime system.
     * @return <code>true</code> if authorized; <code>false</code> otherwise.
     */
    public boolean canPerformRuntimeOperations(ConnectionInfo conn) {
        return isProductAdmin(conn);
	}

    public static MetaMatrixPrincipalName getLoggedInUser(
    		ConnectionInfo connection) throws Exception {
        MetaMatrixSessionID mmsidSessionID  = 
        		connection.getSessionID();
        MetaMatrixPrincipal mmpPrincipal = ModelManager.getSessionAPI(
        		connection).getPrincipal(mmsidSessionID);
        MetaMatrixPrincipalName loggedInUser = 
        		mmpPrincipal.getMetaMatrixPrincipalName();
        return loggedInUser;
    }
}
