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
package org.teiid.adminapi.impl;

import org.jboss.managed.api.annotation.ManagementProperty;
import org.teiid.adminapi.ConnectionPoolStatistics;

public class ConnectionPoolStatisticsMetadata extends AdminObjectImpl implements ConnectionPoolStatistics {

	private static final long serialVersionUID = 4420845831075045579L;
	private long availableConnectionCount;
	private int connectionCount;
	private int connectionCreatedCount;
	private int connectionDestroyedCount;
	private long inUseConnectionCount;
	private long maxConnectionsInUseCount;
	private int maxSize;
	private int minSize;
	
	
	@Override
	@ManagementProperty(description="The maximum number of connections that are available", readOnly=true)
	public long getAvailableConnectionCount() {
		return availableConnectionCount;
	}

	@Override
	@ManagementProperty(description="The number of connections that are currently in the pool", readOnly=true)
	public int getConnectionCount() {
		return connectionCount;
	}

	@Override
	@ManagementProperty(description="The number of connections that have been created since the connector was last started", readOnly=true)
	public int getConnectionCreatedCount() {
		return connectionCreatedCount;
	}

	@Override
	@ManagementProperty(description="The number of connections that have been destroyed since the connector was last started", readOnly=true)
	public int getConnectionDestroyedCount() {
		return connectionDestroyedCount;
	}

	@Override
	@ManagementProperty(description="The number of connections that are currently in use", readOnly=true)
	public long getInUseConnectionCount() {
		return inUseConnectionCount;
	}

	@Override
	@ManagementProperty(description="The most connections that have been simultaneously in use since this connector was started", readOnly=true)
	public long getMaxConnectionsInUseCount() {
		return maxConnectionsInUseCount;
	}

	@Override
	@ManagementProperty(description="Max configured size", readOnly=true)
	public int getMaxSize() {
		return maxSize;
	}

	@Override
	@ManagementProperty(description="Min Configured Size", readOnly=true)
	public int getMinSize() {
		return minSize;
	}

	public void setMaxSize(int maxSize) {
		this.maxSize = maxSize;
	}

	public void setAvailableConnectionCount(long availableConnectionCount) {
		this.availableConnectionCount = availableConnectionCount;
	}

	public void setConnectionCount(int connectionCount) {
		this.connectionCount = connectionCount;
	}

	public void setConnectionCreatedCount(int connectionCreatedCount) {
		this.connectionCreatedCount = connectionCreatedCount;
	}

	public void setConnectionDestroyedCount(int connectionDestroyedCount) {
		this.connectionDestroyedCount = connectionDestroyedCount;
	}

	public void setInUseConnectionCount(long inUseConnectionCount) {
		this.inUseConnectionCount = inUseConnectionCount;
	}

	public void setMaxConnectionsInUseCount(long maxConnectionsInUseCount) {
		this.maxConnectionsInUseCount = maxConnectionsInUseCount;
	}

	public void setMinSize(int minSize) {
		this.minSize = minSize;
	}
	
    public String toString() {
    	StringBuilder str = new StringBuilder();
        str.append("ConnectionPoolStatisticsMetadata:"); //$NON-NLS-1$
        str.append("  availableConnectionCount = " + availableConnectionCount); //$NON-NLS-1$
        str.append("; connectionCount = " + connectionCount); //$NON-NLS-1$
        str.append("; connectionCreatedCount = " + connectionCreatedCount); //$NON-NLS-1$
        str.append("; connectionDestroyedCount = " + connectionDestroyedCount);     //$NON-NLS-1$
        str.append("; inUseConnectionCount = " + inUseConnectionCount);     //$NON-NLS-1$
        str.append("; maxConnectionsInUseCount = " + maxConnectionsInUseCount);     //$NON-NLS-1$
        str.append("; maxSize = " + maxSize);     //$NON-NLS-1$
        str.append("; minSize = " + minSize);     //$NON-NLS-1$
        return str.toString();
    }   	
}
