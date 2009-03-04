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

package com.metamatrix.server;

import com.metamatrix.api.exception.MetaMatrixComponentException;

/**
 * Server life-cycle management interface for the host controller
 */
public interface HostManagement {

	/**
	 * Start all servers in the all the hosts
	 */
	void startAllServersInCluster();
	
	/**
	 * Kill all the servers at all the hosts
	 */
	void killAllServersInCluster() ;
	
	/**
	 * Kill and re-start all the servers in the cluster
	 */
	void bounceAllServersInCluster();
	
	/**
	 * Start all servers on the given host; 
	 * @param hostName - name of the host; can not be null
	 */
	void startServers(String hostName) throws MetaMatrixComponentException;
	
	/**
	 * Kill all servers on a this host
	 * @param hostName - name of the host; can not be null
	 * @param stopNow - true if need to be stopped immediately
	 */
	void killServers(String hostName, boolean stopNow) throws MetaMatrixComponentException ;
	
	/**
	 * Kill and restart all the servers on this host
	 * @param hostName - name of the host; can not be null
	 */
	void bounceServers(String hostName) throws MetaMatrixComponentException;
	
	/**
	 * Start a server process on this host with specified processName
	 * @param hostName - name of the host; can not be null
	 * @param processName - virtual machine name
	 */
	void startServer(String hostName, String processName) throws MetaMatrixComponentException ;
	
	/**
	 * Kill the server process on this host with specified processName
	 * @param hostName - name of the host; can not be null
	 * @param processName
	 */
	void killServer(String hostName, String processName, boolean stopNow) throws MetaMatrixComponentException;
	
	/**
	 * Ping the server process on this host with given processName.
	 * @param hostName - name of the host; can not be null
	 * @param processName
	 * @return true if available; false otherwise
	 */
	boolean pingServer(String hostName, String processName);
	
	/**
	 * Ping the local host controller 
	 * @param hostName - name of the host; can not be null
	 * @return true if available; false otherwise
	 */
	boolean ping(String hostName);
	
	/**
	 * Kill the host controller; this will also kill all the server processes
	 * on this host.
	 * @param hostName - name of the host; can not be null
	 */
	void shutdown(String hostname) throws MetaMatrixComponentException;
	
	/**
	 * shutdown all the host controllers and all the servers processes
	 */
	void shutdownCluster() throws MetaMatrixComponentException;
}
