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

package com.metamatrix.admin.api.server;

import org.teiid.adminapi.AdminObject;
import org.teiid.adminapi.AdminRoles;

import com.metamatrix.admin.RolesAllowed;

/**
 * <p>This is the primary facade interface to MetaMatrix server administrative functionality.  The
 * general design is to provide getters to retrieve lightweight data transfer
 * objects ({@link AdminObject}) which are not "live" and do not communicate back
 * to the server.</p>
 *
 * <p>Generally, all objects have an identifier and each identifier form is
 * specific to the object type.  The identifiers taken by the methods in this interface
 * may take generic identifiers (such as {@link AdminObject#WILDCARD}) to specify a set of objects
 * to work on.  The identifier forms and uniqueness constraints are specified in the
 * javadoc for each particular object.
 * </p>
 *
 * @see AdminObject
 *
 * @since 4.3
 */
public interface ServerAdmin extends ServerMonitoringAdmin, ServerConfigAdmin, ServerRuntimeStateAdmin, ServerSecurityAdmin, org.teiid.adminapi.Admin {

    /**
     * Closes ServerAdmin connection to the server.
     * @since 4.3
     */
	@RolesAllowed(value=AdminRoles.RoleName.ANONYMOUS)
    void close();    
}
