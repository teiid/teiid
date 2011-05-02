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

import java.util.List;

public interface DataPolicy {
	
	public enum PermissionType {CREATE, READ, UPDATE, DELETE, ALTER, EXECUTE};
	
	/**
	 * Get the Name of the Data Policy
	 * @return
	 */
	String getName();
	
	/**
	 * Get the description of the Data Policy
	 * @return
	 */
	String getDescription();
	
	/**
	 * Get the List of Permissions for this Data Policy.
	 * @return
	 */
	List<DataPermission> getPermissions();
	
	/**
	 * Mapped Container Role names for this Data Policy
	 * @return
	 */
	List<String> getMappedRoleNames();
	
	/**
	 * If the policy applies to any authenticated user
	 * @return
	 */
	boolean isAnyAuthenticated();
	
	/**
	 * If the policy allows for temporary table usage
	 * @return
	 */
	Boolean isAllowCreateTemporaryTables();
	
	interface DataPermission {
		/**
		 * Get the Resource Name that Data Permission representing
		 * @return
		 */
		String getResourceName();
		
		/**
		 * Is "CREATE" allowed?
		 * @return
		 */
		Boolean getAllowCreate();
		
		/**
		 * Is "SELECT" allowed?
		 * @return
		 */
		Boolean getAllowRead();
		
		/**
		 * Is "INSERT/UPDATE" allowed?
		 * @return
		 */
		Boolean getAllowUpdate();
		
		/**
		 * Is "DELETE" allowed?
		 * @return
		 */
		Boolean getAllowDelete();

		/**
		 * Is "ALTER" allowed?
		 * @return
		 */
		Boolean getAllowAlter();

		/**
		 * Is "EXECUTE" allowed?
		 * @return
		 */
		Boolean getAllowExecute();		
	}
}
