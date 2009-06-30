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

package com.metamatrix.admin.server;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import org.teiid.adminapi.AdminComponentException;
import org.teiid.adminapi.AdminException;
import org.teiid.adminapi.AdminObject;
import org.teiid.adminapi.AdminOptions;
import org.teiid.adminapi.AdminProcessingException;
import org.teiid.adminapi.AdminRoles;
import org.teiid.adminapi.Group;
import org.teiid.adminapi.Principal;

import com.metamatrix.admin.api.exception.security.InvalidSessionException;
import com.metamatrix.admin.api.exception.security.MetaMatrixSecurityException;
import com.metamatrix.admin.api.server.ServerSecurityAdmin;
import com.metamatrix.admin.objects.MMGroup;
import com.metamatrix.admin.objects.MMRole;
import com.metamatrix.api.exception.security.AuthorizationException;
import com.metamatrix.api.exception.security.AuthorizationMgmtException;
import com.metamatrix.api.exception.security.MembershipServiceException;
import com.metamatrix.common.actions.ModificationActionQueue;
import com.metamatrix.platform.registry.ClusteredRegistryState;
import com.metamatrix.platform.security.api.AuthorizationObjectEditor;
import com.metamatrix.platform.security.api.AuthorizationPolicy;
import com.metamatrix.platform.security.api.AuthorizationPolicyID;
import com.metamatrix.platform.security.api.Credentials;
import com.metamatrix.platform.security.api.MetaMatrixPrincipalName;
import com.metamatrix.platform.security.api.SessionToken;
import com.metamatrix.platform.security.util.RolePermissionFactory;

/**
 * @since 4.3
 */
public class ServerSecurityAdminImpl extends AbstractAdminImpl implements ServerSecurityAdmin {

    
    public ServerSecurityAdminImpl(ServerAdminImpl parent, ClusteredRegistryState registry) {
        super(parent, registry);
    }
    
    /** 
     * @see com.metamatrix.admin.api.server.ServerSecurityAdmin#addRoleToGroup(java.lang.String, java.lang.String)
     * @since 4.3
     */
    public void assignRoleToGroup(String roleIdentifier,
                               String groupIdentifier) throws AdminException {
        if ( ! AdminRoles.containsRole(roleIdentifier) ) {
            Object[] params = new Object[] {roleIdentifier};
            throw new AdminProcessingException(AdminServerPlugin.Util.getString("ServerSecurityAdminImpl.Non_existant_role", params)); //$NON-NLS-1$
        }
        if ( groupIdentifier.equals(AdminObject.WILDCARD) ) {
            throw new AdminProcessingException(AdminServerPlugin.Util.getString("ServerSecurityAdminImpl.Cant_use_wildcard")); //$NON-NLS-1$
        }
        SessionToken adminToken = validateSession();
        
        AuthorizationObjectEditor aoe = new AuthorizationObjectEditor();
        AuthorizationPolicy role = null;
        try {
            role = getAuthorizationServiceProxy().getPolicy(adminToken,
                    new AuthorizationPolicyID(roleIdentifier, null, RolePermissionFactory.getRealm()));
        } catch (InvalidSessionException e) {
        	throw new AdminComponentException(e);
        } catch (AuthorizationMgmtException e) {
        	throw new AdminComponentException(e);
        } catch (AuthorizationException e) {
        	throw new AdminComponentException(e);
        } 
        role = aoe.addPrincipal(role, new MetaMatrixPrincipalName(groupIdentifier, Principal.TYPE_GROUP));

        //Execute the transactions
        executeAuthorizationActions(aoe);
    }
    
    /** 
     * @see com.metamatrix.admin.api.server.ServerSecurityAdmin#removeRoleFromGroup(java.lang.String, java.lang.String)
     * @since 4.3
     */
    public void removeRoleFromGroup(String roleIdentifier,
                                    String groupIdentifier) throws AdminException {
        if ( ! AdminRoles.containsRole(roleIdentifier) ) {
            Object[] params = new Object[] {roleIdentifier};
            throw new AdminProcessingException(AdminServerPlugin.Util.getString("ServerSecurityAdminImpl.Non_existant_role", params)); //$NON-NLS-1$
        }
        if ( groupIdentifier.equals(AdminObject.WILDCARD) ) {
            throw new AdminProcessingException(AdminServerPlugin.Util.getString("ServerSecurityAdminImpl.Cant_use_wildcard")); //$NON-NLS-1$
        }
        SessionToken adminToken = validateSession();
        
        AuthorizationObjectEditor aoe = new AuthorizationObjectEditor();
        AuthorizationPolicy role = null;
        try {
            role = getAuthorizationServiceProxy().getPolicy(adminToken,
                    new AuthorizationPolicyID(roleIdentifier, null, RolePermissionFactory.getRealm()));
        } catch (InvalidSessionException e) {
        	throw new AdminComponentException(e);
        } catch (AuthorizationMgmtException e) {
        	throw new AdminComponentException(e);
        } catch (AuthorizationException e) {
        	throw new AdminComponentException(e);
        } 
        role = aoe.removePrincipal(role, new MetaMatrixPrincipalName(groupIdentifier, Principal.TYPE_GROUP));

        //Execute the transactions
        executeAuthorizationActions(aoe);
    }
    
    public boolean authenticateUser(String username, char[] credentials, Serializable trustePayload, String applicationName) throws AdminException {
        try {
			return getMembershipServiceProxy().authenticateUser(username, new Credentials(credentials), trustePayload, applicationName).isAuthenticated();
		} catch (MembershipServiceException e) {
			throw new AdminComponentException(e);
		} 
    }
    
    /** 
     * Execute the actions requeted of the <code>MembershipObjectEditor</code> or throw
     * an exception if unable for any reason.
     * @param aoe
     * @throws AdminComponentException 
     * @since 4.3
     */
    private void executeAuthorizationActions(AuthorizationObjectEditor aoe) throws AdminException {
        ModificationActionQueue maq = aoe.getDestination();
            try {
                getAuthorizationServiceProxy().executeTransaction(validateSession(), 
                                                               maq.popActions());
            } catch (InvalidSessionException e) {
            	throw new AdminComponentException(e);
            } catch (AuthorizationMgmtException e) {
            	throw new AdminComponentException(e);
            } catch (AuthorizationException e) {
                throw new AdminComponentException(e);
            } 
    }

    /** 
     * @see com.metamatrix.admin.api.server.ServerSecurityAdmin#getRolesForUser(java.lang.String)
     * @since 4.3
     */
    public Collection getRolesForUser(String userIdentifier) throws AdminException {
        if ( userIdentifier.equals(AdminObject.WILDCARD) ) {
            throw new AdminProcessingException(AdminServerPlugin.Util.getString("ServerSecurityAdminImpl.Cant_use_wildcard")); //$NON-NLS-1$
        }
        Collection roleNames = null;
        try {
            roleNames = getAuthorizationServiceProxy().getRoleNamesForPrincipal(
                                                    validateSession(),
                                                    new MetaMatrixPrincipalName(userIdentifier, Principal.TYPE_USER));
        } catch (InvalidSessionException e) {
        	throw new AdminComponentException(e);
        } catch (AuthorizationMgmtException e) {
        	throw new AdminComponentException(e);
        } catch (AuthorizationException e) {
            throw new AdminComponentException(e);
        }
        
        Collection roles = new ArrayList();
        Iterator roleNameItr = roleNames.iterator();
        while ( roleNameItr.hasNext() ) {
             String roleName = (String)roleNameItr.next();
             roles.add(new MMRole(new String[] {roleName}));
        }
        return roles;
    }


    /** 
     * @see com.metamatrix.admin.api.server.ServerSecurityAdmin#getGroupsForUser(java.lang.String, boolean)
     * @since 4.3
     */
    public Collection getGroupsForUser(String userIdentifier) throws AdminException {
        if (userIdentifier == null) {
            throwProcessingException("AdminImpl.requiredparameter", new Object[] {}); //$NON-NLS-1$
        }
        
        if ( userIdentifier.equals(AdminObject.WILDCARD) ) {
            throw new AdminProcessingException(AdminServerPlugin.Util.getString("ServerSecurityAdminImpl.Cant_use_wildcard")); //$NON-NLS-1$
        }
        Collection groups = new ArrayList();
        // Get all memberships - explicit and implicit
        Set allMemberships = null;
        try {
            allMemberships = getMembershipServiceProxy().getGroupsForUser(userIdentifier);
        } catch (MetaMatrixSecurityException e) {
            throw new AdminComponentException(e);
        }
        Iterator allMembershipsItr = allMemberships.iterator();
        while ( allMembershipsItr.hasNext() ) {
            groups.add(new MMGroup(new String[] {(String)allMembershipsItr.next()}));
        }
        return groups;
    }

    /** 
     * @see com.metamatrix.admin.api.server.ServerSecurityAdmin#getGroups(java.lang.String)
     * @since 4.3
     */
    public Collection<Group> getGroups(String groupIdentifier) throws AdminException {
        if (groupIdentifier == null) {
            throwProcessingException("AdminImpl.requiredparameter", new Object[] {}); //$NON-NLS-1$
        }
        
        Collection<Group> groups = new ArrayList<Group>();
        Collection allGroups = null;
        // Add all groups from internal membership domain
        try {
            allGroups = getMembershipServiceProxy().getGroupNames();
        } catch (MetaMatrixSecurityException e) {
        	throw new AdminComponentException(e);
        }

        Iterator groupItr = allGroups.iterator();
        while ( groupItr.hasNext() ) {
            String groupName = (String) groupItr.next();

            if (!groupIdentifier.equals(AdminObject.WILDCARD) && !groupName.equals(groupIdentifier)) {
                continue;
            }

            groups.add(new MMGroup(new String[] {groupName}));
        }
        return groups;
    }


    /** 
     * @see com.metamatrix.admin.api.server.ServerSecurityAdmin#getRolesForGroup(java.lang.String)
     * @since 4.3
     */
    public Collection getRolesForGroup(String groupIdentifier) throws AdminException {
        if (groupIdentifier == null) {
            throwProcessingException("AdminImpl.requiredparameter", new Object[] {}); //$NON-NLS-1$
        }
        
        if ( groupIdentifier.equals(AdminObject.WILDCARD) ) {
            throw new AdminProcessingException(AdminServerPlugin.Util.getString("ServerSecurityAdminImpl.Cant_use_wildcard")); //$NON-NLS-1$
        }
        Collection roleNames = null;
        try {
            roleNames = getAuthorizationServiceProxy().getRoleNamesForPrincipal(
                                                    validateSession(),
                                                    new MetaMatrixPrincipalName(groupIdentifier, Principal.TYPE_GROUP));
        } catch (InvalidSessionException e) {
        	throw new AdminComponentException(e);
        } catch (AuthorizationMgmtException e) {
        	throw new AdminComponentException(e);
        } catch (AuthorizationException e) {
        	throw new AdminComponentException(e);
        }
        Collection roles = new ArrayList();
        Iterator roleNameItr = roleNames.iterator();
        while ( roleNameItr.hasNext() ) {
             String roleName = (String)roleNameItr.next();
             roles.add(new MMRole(new String[] {roleName}));
        }
        return roles;
    }
       
    /**
     * @see com.metamatrix.admin.api.server.ServerSecurityAdmin#importDataRoles(java.lang.String, java.lang.String, char[], org.teiid.adminapi.AdminOptions)
     */
    public String importDataRoles(String vdbName, String vdbVersion, char[] xmlContents, AdminOptions options) 
        throws AdminException{
        
        if (vdbName == null) {
            throw new AdminProcessingException(AdminServerPlugin.Util.getString("ServerSecurityAdminImpl.vdbName_can_not_be_null")); //$NON-NLS-1$
        }
        
        if (vdbVersion == null) {
            throw new AdminProcessingException(AdminServerPlugin.Util.getString("ServerSecurityAdminImpl.vdbVersion_can_not_be_null")); //$NON-NLS-1$
        }
        
        if (options == null) {
            
            options = new AdminOptions(AdminOptions.OnConflict.IGNORE);
        }

        return super.importDataRoles(vdbName, vdbVersion, xmlContents, options);
    }

    /**
     * @see com.metamatrix.admin.api.server.ServerSecurityAdmin#exportDataRoles(java.lang.String, java.lang.String)
     */
    public char[] exportDataRoles(String vdbName, String vdbVersion) 
        throws AdminException {
        
        if (vdbName == null) {
            throw new AdminProcessingException(AdminServerPlugin.Util.getString("ServerSecurityAdminImpl.vdbName_can_not_be_null")); //$NON-NLS-1$
        }
        
        if (vdbVersion == null) {
            throw new AdminProcessingException(AdminServerPlugin.Util.getString("ServerSecurityAdminImpl.vdbVersion_can_not_be_null")); //$NON-NLS-1$
        }
        return super.exportDataRoles(vdbName, vdbVersion);
    }
    
    @Override
    public List<String> getDomainNames() throws AdminException {
    	try {
			return this.getMembershipServiceProxy().getDomainNames();
		} catch (MembershipServiceException e) {
			throw new AdminComponentException(e);
		}
    }
    
    @Override
    public Collection<Group> getGroupsForDomain(String domainName)
    		throws AdminException {
        if (domainName == null) {
            throwProcessingException("AdminImpl.requiredparameter", new Object[] {}); //$NON-NLS-1$
        }
        try {
        	Collection<String> groupNames = this.getMembershipServiceProxy().getGroupsForDomain(domainName);
        	List<Group> result = new ArrayList<Group>(groupNames.size());
        	for (String groupName : groupNames) {
        		result.add(new MMGroup(new String[] {groupName}));
			}
        	return result;
        } catch (MembershipServiceException e) {
        	throw new AdminComponentException(e);
        }
    }
}