/*
 * JBoss, Home of Professional Open Source.
 * Copyright (C) 2008 Red Hat, Inc.
 * Copyright (C) 2000-2007 MetaMatrix, Inc.
 * Licensed to Red Hat, Inc. under one or more contributor 
 * license agreements.  See the copyright.txt file in the
 * distribution for a full listing of individual contributors.
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
import java.util.Date;

import com.metamatrix.admin.api.exception.AdminException;
import com.metamatrix.api.exception.MultipleException;
import com.metamatrix.common.config.api.ServiceComponentDefnID;
import com.metamatrix.common.log.LogConfiguration;
import com.metamatrix.platform.service.api.ServiceID;
import com.metamatrix.platform.vm.controller.VMControllerID;
import com.metamatrix.platform.vm.controller.VMStatistics;

public interface VMControllerInterface {
    
	/**
	 * Starts the VM by invoking all the deployed services
	 */
	public void startVM();
	
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
	 * Kill all services (waiting for work to complete) and then kill the vm.
	 */
	void stopVM();

	/**
	 * Kill all services now, do not wait for work to complete
	 */
	void stopVMNow();

	/**
	 * Kill service once work is complete
	 */
	void stopService(ServiceID id);

	/**
	 * Kill service now!!!
	 */
	void stopServiceNow(ServiceID id);

    /**
     * Kill all services once work is complete
     */
    void stopAllServices() throws MultipleException;

    /**
     * Kill all services now
     */
    void stopAllServicesNow() throws MultipleException;
    
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
    Date getStartTime();

	/**
	 * Get the address of the host this VM is running on.
	 */
    InetAddress getAddress();

	/**
	 * Get the ID for this controller.
	 */
    VMControllerID getID() ;

	/**
	 * Get the name for this controller.
	 */
    String getName();

	/**
	 * Method called from registries to determine if VMController is alive.
	 */
	void ping();

    /**
     * Shut down all services waiting for work to complete.
     * Essential services will also be shutdown.
     */
    void shutdown();

    /**
     * Shut down all services without waiting for work to complete.
     * Essential services will also be shutdown.
     */
    void shutdownNow();

    /**
     * Shut down service waiting for work to complete.
     * Essential services will also be shutdown.
     */
    void shutdownService(ServiceID serviceID) ;

    /**
     * Shut down all services without waiting for work to complete.
     * Essential services will also be shutdown.
     */
    void shutdownServiceNow(ServiceID serviceID);

    /**
     * Returns true if system is being shutdown.
     */
    boolean isShuttingDown();

    /**
     * Return information about VM.
     * totalMemory, freeMemory, threadCount
     */
    VMStatistics getVMStatistics();

    /**
     * dumps stack trace to log file.
     */
    void dumpThreads();

    /**
     * Run GC on vm.
     */
    void runGC() ;

    
    /**
     * Export the server logs to a byte[].  The bytes contain the contents of a .zip file containing the logs. 
     * This will export all logs on the host that contains this VMController.
     * @return the logs, as a byte[].
     * @throws AdminException
     * @since 4.3
     */
    byte[] exportLogs();
}

