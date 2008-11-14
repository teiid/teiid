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

package com.metamatrix.admin.api.server;

import java.util.Collection;

import com.metamatrix.admin.api.core.CoreSecurityAdmin;
import com.metamatrix.admin.api.exception.AdminException;
import com.metamatrix.admin.api.objects.AdminObject;
import com.metamatrix.admin.api.objects.AdminOptions;
import com.metamatrix.admin.api.objects.Group;
import com.metamatrix.admin.api.objects.Principal;
import com.metamatrix.admin.api.objects.Role;


/**
 * Interface that exposes MetaMatrix security system for administration.
 * <p>
 * Clients should <i>not</i> code directly to this interface but should instead use {@link ServerAdmin}.
 * </p>
 *
 * @since 4.3
 */
public interface ServerSecurityAdmin extends CoreSecurityAdmin {

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
    Collection getRolesForGroup(String groupIdentifier) throws AdminException;
    
        /**
     * Get the Collection of administrative role names possessed by the given user, if any.
     *
     * @param userIdentifier
     *            The unique identifier for the user. This is generally a user name. A user is a {@link Principal} and a
     *            Principal name is considered to be unique throughout the MetaMatrix system across all Membership domains.
     *             The {@link AdminObject#WILDCARD WILDCARD} cannot be used here.
     * @return The Collection of <code>String</code> role names.
     * @throws AdminException
     *             if there's a system error.
     * @since 4.3
     */
    Collection getRolesForUser(String userIdentifier) throws AdminException;

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
    Collection getGroupsForUser(String userIdentifier) throws AdminException;
    
    
    /**
     * Get the group denoted by the given <code>groupIdentifier</code>.
     *
     * @param groupIdentifier
     *            The unique identifier for the {@link Group}. This is generally a group name. A group is a {@link Principal} and
     *            a Principal name is considered to be unique throughout the MetaMatrix system across all Membership domains. <br>
     *            Note that by supplying the {@link AdminObject#WILDCARD WILDCARD} identifier, all all users in the system will
     *            retrieved.</br>
     * @return The Collection of users.
     * @throws AdminException
     *             if there's a system error.
     * @since 4.3
     */
    Collection getGroups(String groupIdentifier) throws AdminException;

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
     * Export the data roles defined for the given vdb fromthe current system
     * @param vdbName - Name of the vdb
     * @param vdbVersion - version of the vdb
     * @return - char[] stream containing the XML contents of the roles.
     * @throws AdminException
     */
    char[] exportDataRoles(String vdbName, String vdbVersion) throws AdminException;
}
