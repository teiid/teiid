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


/** 
 * This object holds the statistics for a ConnectionPool that is being utilized by a Connector.
 * As per how many available connections
 * processed etc.
 */
public interface ConnectionPoolStatistics extends AdminObject, Serializable {

	/**
	 * The maximum number of connections that are available
	 * @return
	 */
	long getAvailableConnectionCount();
	
	/**
	 * The number of connections that are currently in the pool
	 * @return
	 */
	int getConnectionCount();

	/**
	 * The number of connections that have been created since the connector was last started
	 * @return
	 */
	int getConnectionCreatedCount();
	
	/**
	 * The number of connections that have been destroyed since the connector was last started
	 * @return
	 */
	int getConnectionDestroyedCount();
	

	/**
	 * The number of connections that are currently in use
	 * @return
	 */
	long getInUseConnectionCount();
	
	/**
	 * The most connections that have been simultaneously in use since this connector was started
	 * @return
	 */
	long getMaxConnectionsInUseCount();
	
	/**
	 * Max size
	 * @return
	 */
	int getMaxSize();
	
	/**
	 * Min Size
	 * @return
	 */
	int getMinSize();
}
