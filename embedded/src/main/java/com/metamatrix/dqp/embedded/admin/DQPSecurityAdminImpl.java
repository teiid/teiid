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

package com.metamatrix.dqp.embedded.admin;

import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import javax.xml.parsers.ParserConfigurationException;

import org.teiid.adminapi.AdminComponentException;
import org.teiid.adminapi.AdminException;
import org.teiid.adminapi.AdminObject;
import org.teiid.adminapi.AdminOptions;
import org.teiid.adminapi.AdminProcessingException;
import org.teiid.adminapi.Group;
import org.teiid.adminapi.Principal;
import org.teiid.adminapi.SecurityAdmin;
import org.xml.sax.SAXException;

import com.metamatrix.admin.api.exception.security.InvalidSessionException;
import com.metamatrix.admin.api.exception.security.MetaMatrixSecurityException;
import com.metamatrix.admin.objects.MMGroup;
import com.metamatrix.admin.objects.MMRole;
import com.metamatrix.api.exception.security.AuthorizationException;
import com.metamatrix.api.exception.security.AuthorizationMgmtException;
import com.metamatrix.api.exception.security.MembershipServiceException;
import com.metamatrix.dqp.embedded.DQPEmbeddedPlugin;
import com.metamatrix.jdbc.EmbeddedConnectionFactoryImpl;
import com.metamatrix.platform.admin.api.EntitlementMigrationReport;
import com.metamatrix.platform.security.api.AuthorizationPolicy;
import com.metamatrix.platform.security.api.AuthorizationPolicyFactory;
import com.metamatrix.platform.security.api.AuthorizationRealm;
import com.metamatrix.platform.security.api.Credentials;
import com.metamatrix.platform.security.api.MetaMatrixPrincipalName;
import com.metamatrix.platform.security.api.SessionToken;


/** 
 * @since 4.3
 */
public class DQPSecurityAdminImpl  extends BaseAdmin implements SecurityAdmin {

    public DQPSecurityAdminImpl(EmbeddedConnectionFactoryImpl manager) {
        super(manager);
    }
    
    /** 
     * @see com.metamatrix.admin.api.server.ServerSecurityAdmin#addRoleToGroup(java.lang.String, java.lang.String)
     * @since 4.3
     */
    public void assignRoleToGroup(String roleIdentifier, String groupIdentifier) throws AdminException {
    	throw new AdminComponentException(DQPEmbeddedPlugin.Util.getString("ServerSecurityAdminImpl.not_implemented")); //$NON-NLS-1$
    }
    
    /** 
     * @see com.metamatrix.admin.api.server.ServerSecurityAdmin#removeRoleFromGroup(java.lang.String, java.lang.String)
     * @since 4.3
     */
    public void removeRoleFromGroup(String roleIdentifier, String groupIdentifier) throws AdminException {
    	throw new AdminComponentException(DQPEmbeddedPlugin.Util.getString("ServerSecurityAdminImpl.not_implemented")); //$NON-NLS-1$
    }
    
    public boolean authenticateUser(String username, char[] credentials, Serializable trustePayload, String applicationName) throws AdminException {
        try {
			return getMembershipService().authenticateUser(username, new Credentials(credentials), trustePayload, applicationName).isAuthenticated();
		} catch (MembershipServiceException e) {
			throw new AdminComponentException(e);
		} 
    }
    

    /** 
     * @see com.metamatrix.admin.api.server.ServerSecurityAdmin#getGroupsForUser(java.lang.String, boolean)
     * @since 4.3
     */
    public Collection<Group> getGroupsForUser(String userIdentifier) throws AdminException {
        if (userIdentifier == null) {
            throwProcessingException("AdminImpl.requiredparameter", new Object[] {}); //$NON-NLS-1$
        }
        
        if ( userIdentifier.equals(AdminObject.WILDCARD) ) {
            throw new AdminProcessingException(DQPEmbeddedPlugin.Util.getString("ServerSecurityAdminImpl.Cant_use_wildcard")); //$NON-NLS-1$
        }
        Collection groups = new ArrayList();
        // Get all memberships - explicit and implicit
        Set allMemberships = null;
        try {
            allMemberships = getMembershipService().getGroupsForUser(userIdentifier);
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
            allGroups = getMembershipService().getGroupNames();
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
            throw new AdminProcessingException(DQPEmbeddedPlugin.Util.getString("ServerSecurityAdminImpl.Cant_use_wildcard")); //$NON-NLS-1$
        }
        Collection roleNames = null;
        try {
            roleNames = getAuthorizationService().getRoleNamesForPrincipal(new MetaMatrixPrincipalName(groupIdentifier, Principal.TYPE_GROUP));
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
            throw new AdminProcessingException(DQPEmbeddedPlugin.Util.getString("ServerSecurityAdminImpl.vdbName_can_not_be_null")); //$NON-NLS-1$
        }
        
        if (vdbVersion == null) {
            throw new AdminProcessingException(DQPEmbeddedPlugin.Util.getString("ServerSecurityAdminImpl.vdbVersion_can_not_be_null")); //$NON-NLS-1$
        }
        
        if (options == null) {
            
            options = new AdminOptions(AdminOptions.OnConflict.IGNORE);
        }

        try {
        	EntitlementMigrationReport rpt = new EntitlementMigrationReport("from file", vdbName + " " + vdbVersion); //$NON-NLS-1$ //$NON-NLS-2$
        	
            Collection<AuthorizationPolicy> roles = AuthorizationPolicyFactory.buildPolicies(vdbName, vdbVersion, xmlContents);

            AuthorizationRealm realm = new AuthorizationRealm(vdbName, vdbVersion);
            
            getAuthorizationService().updatePoliciesInRealm(realm, roles);

            return rpt.toString();
        } catch (AuthorizationMgmtException e) {
        	throw new AdminProcessingException(e);
   		} catch (SAXException e) {
   			throw new AdminComponentException(e);
   		} catch (IOException e) {
   			throw new AdminComponentException(e);
   		} catch (ParserConfigurationException e) {
   			throw new AdminComponentException(e);
        }
    }

    /**
     * @see com.metamatrix.admin.api.server.ServerSecurityAdmin#exportDataRoles(java.lang.String, java.lang.String)
     */
    public char[] exportDataRoles(String vdbName, String vdbVersion) throws AdminException {
        
        if (vdbName == null) {
            throw new AdminProcessingException(DQPEmbeddedPlugin.Util.getString("ServerSecurityAdminImpl.vdbName_can_not_be_null")); //$NON-NLS-1$
        }
        
        if (vdbVersion == null) {
            throw new AdminProcessingException(DQPEmbeddedPlugin.Util.getString("ServerSecurityAdminImpl.vdbVersion_can_not_be_null")); //$NON-NLS-1$
        }
	     
        Collection roles = null;
		try {
			roles = getAuthorizationService().getPoliciesInRealm(new AuthorizationRealm(vdbName, vdbVersion));
			if (roles != null && !roles.isEmpty()) {
				return AuthorizationPolicyFactory.exportPolicies(roles);
			}
			return null;
		} catch (AuthorizationMgmtException e) {
			throw new AdminProcessingException(e);
		} catch (AuthorizationException e) {
			throw new AdminProcessingException(e);
		} catch (IOException e) {
			throw new AdminComponentException(e);
		}
	}
    
    @Override
    public List<String> getDomainNames() throws AdminException {
    	try {
			return this.getMembershipService().getDomainNames();
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
        	Collection<String> groupNames = this.getMembershipService().getGroupsForDomain(domainName);
        	List<Group> result = new ArrayList<Group>(groupNames.size());
        	for (String groupName : groupNames) {
        		result.add(new MMGroup(new String[] {groupName}));
			}
        	return result;
        } catch (MembershipServiceException e) {
        	throw new AdminComponentException(e);
        }
    }
    
     void throwProcessingException(String key, Object[] objects) throws AdminException {
        throw new AdminProcessingException(DQPEmbeddedPlugin.Util.getString(key, objects));
    }    
 
}
