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

import org.teiid.adminapi.VDB;


/**
 * A listener interface than can be registered with {@link EventDistributor} that will notify 
 * the events occurring in the Teiid engine
 */
public interface EventListener {
	
	/**
	 * Invoked when VDB is deployed
	 * @param vdbName
	 * @param vdbVersion
	 */
	void vdbDeployed(String vdbName, int vdbVersion);
	
	/**
	 * Invoked when VDB undeployed
	 * @param vdbName
	 * @param vdbVersion
	 */
	void vdbUndeployed(String vdbName, int vdbVersion);
	
	/**
	 * VDB and all its metadata has been loaded and in ACTIVE state.
	 * @param vdb
	 */
	void vdbLoaded(VDB vdb);
	
	/**
	 * VDB failed to load and in FAILED state; Note this can be called multiple times for given VDB
	 * @param vdb
	 */
	void vdbLoadFailed(VDB vdb);	
}
