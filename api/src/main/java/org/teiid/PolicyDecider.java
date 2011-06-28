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

package org.teiid;

import java.util.Set;

import org.teiid.adminapi.DataPolicy.Context;
import org.teiid.adminapi.DataPolicy.PermissionType;

/**
 * A policy decider that reports authorization decisions for further action.  
 * A decider may be called many times for a single user command.  Typically there will be 1 call for every
 * command/subquery/temp table access/function call.
 */
public interface PolicyDecider {
	
	/**
	 * Called by the system hasRole function to determine role membership.
	 * @param roleName
	 * @param context
	 * @return true if the user has the given role name, otherwise false
	 */
	boolean hasRole(String roleName, CommandContext context);

	/**
	 * Returns the set of resources not allowed to be accessed by the current user.
	 * Resource names are given based upon the FQNs (NOTE these are non-SQL names - identifiers are not quoted).
	 * @param action
	 * @param resources
	 * @param context in which the action is performed.  
	 *   For example you can have a context of {@link Context#UPDATE} for a {@link PermissionType#READ} for columns used in an UPDATE condition.   
	 * @param commandContext
	 * @return the set of inaccessible resources, never null
	 */
	Set<String> getInaccessibleResources(PermissionType action,
			Set<String> resources, Context context,
			CommandContext commandContext);

	/**
	 * Checks if the given temp table is accessible.  Typically as long as temp tables can be created, all operations are allowed.
	 * Resource names are given based upon the FQNs (NOTE these are non-SQL names - identifiers are not quoted).
	 * @param action
	 * @param resource
	 * @param context in which the action is performed.  
	 *   For example you can have a context of {@link Context#UPDATE} for a {@link PermissionType#READ} for columns used in an UPDATE condition.   
	 * @param commandContext
	 * @return true if the access is allowed, otherwise false
	 */
	boolean isTempAccessable(PermissionType action, String resource,
			Context context, CommandContext commandContext);
	
	/**
	 * Determines if an authorization check should proceed
	 * @param commandContext
	 * @return
	 */
	boolean validateCommand(CommandContext commandContext);

}
