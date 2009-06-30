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

package com.metamatrix.platform.vm.api.controller;

import java.net.InetAddress;

import org.teiid.adminapi.AdminException;

import com.metamatrix.common.config.api.ServiceComponentDefnID;
import com.metamatrix.common.log.LogConfiguration;
import com.metamatrix.platform.service.api.ServiceID;
import com.metamatrix.platform.vm.controller.ProcessStatistics;

public interface ProcessManagement {
    
	/**
	 * Starts the VM by invoking all the deployed services
	 */
	public void start();
	
	
    /**
     * Shut down all services waiting for work to complete.
     * Essential services will also be shutdown.
     */
    void shutdown(boolean now);
    
	/**
	 *  Start the service identified by the ServiceComponentID
	 *  If synch is true then wait for service to start before returning.
	 *  Any exceptions will then be thrown to the caller.
	 *  If synch is false then start service asynchronously.
	 */
	public void startDeployedService(ServiceComponentDefnID id);

	/**
	 * Start a previously stopped service
	 */
	void startService(ServiceID serviceID);


	/**
	 * Kill service once work is complete
	 */
	void stopService(ServiceID id, boolean now, boolean shutdown);

    /**
     * Check the state of a service
     */
    void checkService(ServiceID serviceID);

    /**
     * Set the current log configuration.
     */
    void setCurrentLogConfiguration(LogConfiguration logConfiguration);

	/**
	 * Get the time the VM was initialized.
	 */
    long getStartTime();

	/**
	 * Get the address of the host this VM is running on.
	 */
    InetAddress getAddress();

	/**
	 * Get the name for this controller.
	 */
    String getName();

	/**
	 * Method called from registries to determine if VMController is alive.
	 */
	void ping();

    /**
     * Returns true if system is being shutdown.
     */
    boolean isShuttingDown();

    /**
     * Return information about VM.
     * totalMemory, freeMemory, threadCount
     */
    ProcessStatistics getVMStatistics();

    /**
     * dumps stack trace to log file.
     */
    String dumpThreads();

    /**
     * Export the server logs to a byte[].  The bytes contain the contents of a .zip file containing the logs. 
     * This will export all logs on the host that contains this VMController.
     * @return the logs, as a byte[].
     * @throws AdminException
     * @since 4.3
     */
    byte[] exportLogs();
}

