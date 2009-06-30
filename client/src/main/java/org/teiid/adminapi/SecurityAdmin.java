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

package org.teiid.adminapi;

import java.io.Serializable;
import java.util.Collection;
import java.util.List;

import com.metamatrix.admin.RolesAllowed;


/**
 * This interface defines the methods available for security administration
 * in the Teiid system.
 *
 * @since 4.3
 */
@RolesAllowed(value=AdminRoles.RoleName.ADMIN_SYSTEM)
public interface SecurityAdmin {
    /**
     * Get the Collection of administrative role names possessed by the given group, if any.
     *
     * @param groupIdentifier
     *            The unique identifier for the {@link Group}. This is group name. A user is a {@link Principal} and a
     *            Principal name is considered to be unique throughout the MetaMatrix system across all Membership domains.
     *             The {@link AdminObject#WILDCARD WILDCARD} cannot be used here.
     * @return The Collection of {@link Role}s.
     * @throws AdminException
     *             if there's a system error.
     * @since 4.3
     */
    Collection<Role> getRolesForGroup(String groupIdentifier) throws AdminException;
    
    /**
     * Get the group memberships for the given user. 
     *
     * @param userIdentifier
     *            The unique identifier for the user. This is generally a user name. A user is a {@link Principal} and a
     *            Principal name is considered to be unique throughout the MetaMatrix system across all Membership domains.
     *             The {@link AdminObject#WILDCARD WILDCARD} cannot be used here.
     * @return The collection of groups in which the given user has membership.
     * @throws AdminException
     *             if there's a system error.
     * @since 4.3
     */
    Collection<Group> getGroupsForUser(String userIdentifier) throws AdminException;
    
    
    /**
     * Get the group denoted by the given <code>groupIdentifier</code>.
     *
     * @param groupIdentifier
     *            The unique identifier for the {@link Group}. This is generally a group name. A group is a {@link Principal} and
     *            a Principal name is considered to be unique throughout the MetaMatrix system across all Membership domains. <br>
     *            Note that by supplying the {@link AdminObject#WILDCARD WILDCARD} identifier, all all users in the system will
     *            retrieved.</br>
     * @return The Collection of groups.
     * @throws AdminException
     *             if there's a system error.
     * @since 4.3
     */
    Collection<Group> getGroups(String groupIdentifier) throws AdminException;

    /**
     * Assign to the given {@link Group} the given Administrative Role.
     *
     * @param roleIdentifier
     *            one of {@link AdminRoles}.
     * @param groupIdentifier
     *            the unique identifier for the Principal. The {@link AdminObject#WILDCARD WILDCARD} cannot be used here.
     * @throws AdminException
     *             if there's a system error.
     * @since 4.3
     */
    void assignRoleToGroup(String roleIdentifier,
                               String groupIdentifier) throws AdminException;

    /**
     * Remove an administrative role from the given {@link Group}.
     *
     * @param roleIdentifier
     *            one of {@link AdminRoles}
     * @param groupIdentifier
     *            the unique identifier for the Principal. The {@link AdminObject#WILDCARD WILDCARD} cannot be used here.
     * @throws AdminException
     *             if there's a system error.
     * @since 4.3
     */
    void removeRoleFromGroup(String roleIdentifier,
                                 String groupIdentifier) throws AdminException;
    
    /**
     * Import the data Roles for given vdb and version into the connected server
     * @param vdbName - target name of the VDB, the roles to be imported under
     * @param vdbVersion - target version of the vdb, the roles to be imported under
     * @param data - character data array containing the XML file which defines the roles 
     * @param options - options to overwrite in case the matching roles already exist.
     * @return a report of the import
     * @throws AdminException
     */
    String importDataRoles(String vdbName, String vdbVersion, char[] data, AdminOptions options)  
        throws AdminException;
    
    /**
     * Export the data roles defined for the given vdb from the current system
     * @param vdbName - Name of the vdb
     * @param vdbVersion - version of the vdb
     * @return - char[] stream containing the XML contents of the roles.
     * @throws AdminException
     */
    char[] exportDataRoles(String vdbName, String vdbVersion) throws AdminException;
    
    /**
     * Authenticate a user with the specified user name and credentials
     * for use with the specified application. The application name may also
     * be used by the Membership Service to determine the appropriate authentication
     * mechanism.
     * @param username the user name that is to be authenticated
     * @param credential
     * @param trustePayload
     * @param applicationName the name of the application for which the user
     * is authenticating
     * @return true if the authentication is successful
     * @throws AdminException
     */
    boolean authenticateUser(String username, char[] credentials, Serializable trustePayload, String applicationName) throws AdminException;
    
    /**
     * Returns the active authorization provider domain names, in authentication order.
     * @return List<String>
     * @throws AdminException
     */
	List<String> getDomainNames( ) throws AdminException;

	/**
	 * Return the {@link Group}s for a given domain.  The domain name must be an specified
	 * exactly.  See {@link #getActiveDomainNames()} for possible domain names.
	 * @param domainName
	 * @return
	 * @throws AdminException
	 */
	Collection<Group> getGroupsForDomain(String domainName) throws AdminException;
}
