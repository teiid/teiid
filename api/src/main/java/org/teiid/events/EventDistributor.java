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

package org.teiid.events;

import java.util.List;

/**
 * Distributes events across the Teiid cluster
 */
public interface EventDistributor {
	
	/**
	 * Update the given materialized view row using the internal mat view name #MAT_VIEWFQN.
	 * The tuple is expected to be in table order, which has the primary key first.
	 * Deletes need to only send the key, not the entire row contents.
	 * 
	 * @param vdbName
	 * @param vdbVersion
	 * @param matViewFqn
	 * @param tuple
	 * @param delete
	 */
	void updateMatViewRow(String vdbName, int vdbVersion, String matViewFqn, List<?> tuple, boolean delete);
	
	/**
	 * Notify that the metadata has been changed for the given fqns.
	 * A fqn has the form schema.entityname.
	 * This typically implies that the costing metadata has changed, but may also indicate
	 * a view definition has changed.
	 * @param vdbName
	 * @param vdbVersion
	 * @param fqns
	 */
	void schemaModification(String vdbName, int vdbVersion, String... fqns);
	
	/**
	 * Notify that the table data has changed.
	 * A table fqn has the form schema.tablename.
	 * @param vdbName
	 * @param vdbVersion
	 * @param tableFqns
	 */
	void dataModification(String vdbName, int vdbVersion, String... tableFqns);
}
