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

import org.teiid.metadata.ColumnStats;
import org.teiid.metadata.TableStats;

/**
 * Distributes events across the Teiid cluster
 */
public interface EventDistributor {
	
	/**
	 * Update the given materialized view row.
	 * The tuple is expected to be in table order, which has the primary key first.
	 * Deletes need to only send the key, not the entire row contents.
	 * 
	 * @param vdbName
	 * @param vdbVersion
	 * @param schema
	 * @param viewName
	 * @param tuple
	 * @param delete
	 */
	void updateMatViewRow(String vdbName, int vdbVersion, String schema, String viewName, List<?> tuple, boolean delete);
	
	/**
	 * Notify that the metadata has been changed for the given table or view.
	 * @param vdbName
	 * @param vdbVersion
	 * @param schema
	 * @param objectNames
	 */
	void schemaModification(String vdbName, int vdbVersion, String schema, String... objectNames);
	
	/**
	 * Notify that the table data has changed.
	 * @param vdbName
	 * @param vdbVersion
 	 * @param schema
	 * @param tableNames
	 */
	void dataModification(String vdbName, int vdbVersion, String schema, String... tableNames);

	void setColumnStats(String vdbName, int vdbVersion, String schemaName,
			String tableName, String columnName, ColumnStats stats);

	void setTableStats(String vdbName, int vdbVersion, String schemaName,
			String tableName, TableStats stats);
	
}
