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
package org.teiid.connector.api;

import java.io.Serializable;

/**
 * Cache Scope
 * 
 * REQUEST - Items placed in this scope are retained until the end of the top level request. The items to be placed
 * does not need to implement {@link Serializable}, however recommended. These items are not replicated across the cluster.
 * SERVICE - Items from this scope are available to the identified connector
 * 
 * All the items placed in the below scopes must be {@link Serializable}, as they are replicated across cluster.
 *  
 * SESSION - Items placed in the scope retained until the particular User's session of top level request is alive. 
 * VDB - Items placed with this scope retained until the life of the VDB; 
 * 
 * GLOBAL - Items placed in this will available to all until the Query Service is recycled. 
 */
public enum CacheScope {
	REQUEST, 
	SERVICE,
	SESSION, 
	VDB, 
	GLOBAL;
}
