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

package com.metamatrix.console.models;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import com.metamatrix.admin.api.exception.security.MetaMatrixSecurityException;
import com.metamatrix.admin.api.objects.Group;
import com.metamatrix.api.exception.ComponentNotFoundException;
import com.metamatrix.api.exception.security.AuthorizationException;
import com.metamatrix.console.connections.ConnectionInfo;
import com.metamatrix.console.ui.util.ConsoleConstants;
import com.metamatrix.console.ui.views.entitlements.PrincipalChangeListener;
import com.metamatrix.console.ui.views.users.RoleDisplay;
import com.metamatrix.console.util.ExternalException;
import com.metamatrix.platform.admin.api.AuthorizationAdminAPI;
import com.metamatrix.platform.security.api.MetaMatrixPrincipalName;

public class GroupsManager extends Manager {

    public final static String[][] PROPS_AND_DISPLAY_NAMES_FOR_METAMATRIX_GROUPS =
            new String[][] {
                    {"description", "Description"} //$NON-NLS-1$ //$NON-NLS-2$
            };

    private Map roles = null;
    private java.util.List /*<PrincipalChangeListener>*/ principalChangeListeners =
            new ArrayList(1);

    /**
     * Constructor
     * @param connection the connectionInfo
     */
	public GroupsManager(ConnectionInfo connection) {
		super(connection);
	}
	
    public void init() {
        super.init();
    }

    /**
     * Get map of Role name to RoleDisplay from the authorization api
     * @return map of RoleDisplays
     */
    public Map /*<String (full name) to RoleDisplay>*/ getRoles()
            throws ExternalException, AuthorizationException {
        if (roles == null) {
            try {
            	AuthorizationAdminAPI authAPI = ModelManager.getAuthorizationAPI(
            			getConnection());
                roles = authAPI.getRoleDescriptions();
				Iterator it = roles.entrySet().iterator();
				while (it.hasNext()){
					Map.Entry entry = (Map.Entry)it.next();
					String key = (String)entry.getKey();
					String val = (String)entry.getValue();
                    boolean skipping = isSkippedRole(key);
                    if (!skipping) {
                        RoleDisplay roleDisplay = new RoleDisplay(key,val);
					    roles.put(key,roleDisplay);
                    } else {
                        roles.remove(key);
                    }
				}
            } catch (AuthorizationException e) {
                throw e;
            } catch (Exception e) {
                throw new ExternalException(e);
            }
        }
		return roles;
    }

    /**
     * Get RoleDisplay for the provided role name from the roles Map.
     * @param roleName the role name
     * @return map of RoleDisplays
     */
	public RoleDisplay getRoleDisplay(String roleName)
			throws ExternalException, AuthorizationException {
		RoleDisplay rd = (RoleDisplay)roles.get(roleName);
		return rd;
	}
			
    /**
     * Get Role description for the provided role name.
     * @param roleName the role name
     * @return the Role description
     */
    public String getRoleDescription(String roleName)
            throws ExternalException, AuthorizationException {
		String desc = null;            	
        Map roles = getRoles();
        RoleDisplay rd = (RoleDisplay)roles.get(roleName);
        if (rd != null) {
        	desc = rd.getDescription();
        }
        return desc;
    }

    /**
     * Get Role displayName for the provided role name.
     * @param roleName the role name
     * @return the Role displayName
     */
    public String getRoleDisplayName(String roleName)
            throws ExternalException, AuthorizationException {
        String displayName = null;
        Map roles = getRoles();
        RoleDisplay rd = (RoleDisplay)roles.get(roleName);
        if (rd != null) {
        	displayName = rd.getDisplayName();
        }
        return displayName;
    }

    /**
     * Get Collection of Principals for the provided roleName.
     * @param roleName the role name
     * @return the principals for the provided roleName
     */
    public Collection /*<MetaMatrixPrincipalName>*/ getPrincipalsForRole(String roleName)
            throws ComponentNotFoundException, AuthorizationException,
            ExternalException {
        Collection /*<MetaMatrixPrincipalName>*/ principals;
        try {
            principals = ModelManager.getAuthorizationAPI(getConnection()).getPrincipalsForRole(roleName);
        } catch (AuthorizationException e) {
            throw e;
        } catch (ComponentNotFoundException e) {
            throw e;
        } catch (Exception e) {
            throw new ExternalException(e);
        }
        return principals;
    }

    /**
     * Get Collection of Roles for the provided MetaMatrixPrincipalName
     * @param principalName the MetaMatrixPrincipalName
     * @param includeImplicit flag to determine whether to include implicit
     * @return array of RoleDisplays for the provided principal
     */
    public RoleDisplay[] getRolesForPrincipal(MetaMatrixPrincipalName principalName)
            throws ComponentNotFoundException, AuthorizationException,
            ExternalException, MetaMatrixSecurityException {
        Collection roles;
        try {
            roles = ModelManager.getAuthorizationAPI(getConnection()).getRoleNamesForPrincipal(principalName);
        } catch (AuthorizationException e) {
            throw e;
        } catch (ComponentNotFoundException e) {
            throw e;
        } catch (Exception e) {
            throw new ExternalException(e);
        }
        java.util.List rds = new ArrayList();
        Iterator it = roles.iterator();
        while (it.hasNext()) {
        	String roleName = (String)it.next();
        	RoleDisplay rd = (RoleDisplay)getRoles().get(roleName);
        	rds.add(rd);
        }
        RoleDisplay[] rdArray = new RoleDisplay[rds.size()];
        it = rds.iterator();
        for (int i = 0; it.hasNext(); i++) {
        	rdArray[i] = (RoleDisplay)it.next();
        }
        return rdArray;
    }

    /**
     * Get Collection of Roles for the provided MetaMatrixPrincipals
     * @param principals the collection of MetaMatrixPrincipals
     * @param includeImplicit flag to determine whether to include implicit
     * @return array of RoleDisplays for the provided principal
     */
    public RoleDisplay[] getRolesForPrincipal(
            MetaMatrixPrincipalName principal, boolean includeImplicit)
            throws ComponentNotFoundException, AuthorizationException,
            ExternalException, MetaMatrixSecurityException {
        return getRolesForPrincipal(principal);
    }

    /**
     * Add collection of PrincipalNames to a Role
     * @param principals the collection of MetaMatrixPrincipalNames
     * @param role the role to add the principals to.
     */
    public void addPrincipalsToRole(
    		Collection /*<MetaMatrixPrincipalName>*/ principals, String role)
            throws ComponentNotFoundException, AuthorizationException,
            ExternalException {
        try {
            ModelManager.getAuthorizationAPI(getConnection()).addPrincipalsToRole(
            		new HashSet(principals), role);
        } catch (ComponentNotFoundException e) {
            throw e;
        } catch (AuthorizationException e) {
            throw e;
        } catch (Exception e) {
            throw new ExternalException(e);
        }
    }

    /**
     * Remove collection of PrincipalNames from a Role
     * @param principals the collection of MetaMatrixPrincipalNames
     * @param role the role to remove the principals from.
     */
    public void removePrincipalsFromRole(
    		Collection /*<MetaMatrixPrincipalName>*/ principals, String role)
            throws ComponentNotFoundException, AuthorizationException,
            ExternalException {
        try {
            ModelManager.getAuthorizationAPI(getConnection())
            		.removePrincipalsFromRole(new HashSet(principals), role);
        } catch (ComponentNotFoundException e) {
            throw e;
        } catch (AuthorizationException e) {
            throw e;
        } catch (Exception e) {
            throw new ExternalException(e);
        }
    }

    /**
     * Remove array of Roles from a principal
     * @param roles the array of roles to remove
     * @param principal the principal to remove roles from.
     */
    public void removeRolesFromPrincipal(String[] roles, 
    		MetaMatrixPrincipalName principal)
            throws ComponentNotFoundException, AuthorizationException,
            ExternalException {
        ArrayList princ = new ArrayList();
        princ.add(principal);
        for (int i = 0; i < roles.length; i++) {
            removePrincipalsFromRole(princ, roles[i]);
        }
    }

    public List<String> getDomainNames() {
    	List<String> domainNames = Collections.emptyList();
    	try {
    		domainNames = getConnection().getServerAdmin().getDomainNames();
    	} catch (Exception e) {
    		return domainNames;
    	}
    	return domainNames;
    }
    
    public Collection<Group> getGroupsForDomain (String theDomain) {    	
        try {
        	return getConnection().getServerAdmin().getGroupsForDomain(theDomain);
	    } catch (Exception e) {
	    	
	    }
    
	    return Collections.emptyList();
    }

    public Map getMetaMatrixPropNamesForGroups() {
        // PROPS_AND_DISPLAY_NAMES_FOR_METAMATRIX_GROUPS
        Hashtable htPropNamesAndDisplayNames        = new Hashtable();

        int iOuterLength = PROPS_AND_DISPLAY_NAMES_FOR_METAMATRIX_GROUPS.length;
        String[] aryTemp;

        for( int x = 0; x < iOuterLength; x++ )
        {
            aryTemp =   PROPS_AND_DISPLAY_NAMES_FOR_METAMATRIX_GROUPS[x];
            htPropNamesAndDisplayNames.put( aryTemp[0], aryTemp[1] );
        }

        return htPropNamesAndDisplayNames;
    }

    public void addPrincipalChangeListener(PrincipalChangeListener listener) {
        int loc = principalChangeListeners.indexOf(listener);
        if (loc < 0) {
            principalChangeListeners.add(listener);
        }
    }

    public boolean isSkippedRole(String roleName) {
        boolean skipped = false;
        int i = 0;
        while ((!skipped) && (i < ConsoleConstants.ROLES_NOT_DISPLAYED.length)) {
            if (roleName.equals(ConsoleConstants.ROLES_NOT_DISPLAYED[i])) {
                skipped = true;
            } else {
                i++;
            }
        }
        return skipped;
    }

}
