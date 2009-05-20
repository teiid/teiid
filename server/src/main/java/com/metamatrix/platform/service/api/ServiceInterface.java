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

package com.metamatrix.platform.service.api;

import java.util.Collection;
import java.util.Date;
import java.util.Properties;

import com.metamatrix.common.application.ClassLoaderManager;
import com.metamatrix.common.comm.ClientServiceRegistry;
import com.metamatrix.common.config.api.DeployedComponentID;
import com.metamatrix.common.queue.WorkerPoolStats;
import com.metamatrix.common.stats.ConnectionPoolStats;
import com.metamatrix.platform.service.controller.ServiceData;

public interface ServiceInterface {

    /** Time to wait for queues to clear before giving up (1 min)*/
    public static final int WAIT_TO_DIE_TIME = 1000 * 60;

    /**
     * Instruct the service to initialize and begin processing.The
     * service must notify any lifecycle listeners of its initialization
     * using the unique service instance name returned.
     * @param deployedComponentID Unique identifier of this deployed component.
     * @param clManager
     * @return The unique name of this service instance.
     */
    void init(ServiceID id, DeployedComponentID deployedComponentID, Properties props, ClientServiceRegistry listenerRegistry, ClassLoaderManager clManager);


    /*
     * Instruct the service to stop processing, free resources, and die. The
     * service must notify any lifecycle listeners of its death using the
     * unique service instance name returned by init().
     */
    void die();

    /*
     * Instruct the service to stop processing immediately and die. The
     * service must notify any lifecycle listeners of its death using the
     * unique service instance name returned by init().
     */
    void dieNow();

    
    /* Checks the state of the service.  Based on its underlying implementation,
     * the state may change if a problem is detected (for example if an underlying datasource is down). 
     */
    void checkState();
    

    /**
     * Retreive the properties object used to initialize the service.
     */
    Properties getProperties();

    /**
     * Get the time that the service was initialized.
     */
    Date getStartTime();

    /**
     * Determine which host the service instance is running on.
     * @return Host the service is running on
     */
    String getHostname();

    /**
     * Get process name on which this service is running on
     * @return
     */
    String getProcessName();
    
    /**
     * Determine if the service is alive and well.
     */
    boolean isAlive();

    /**
     * Get service type
     */
    String getServiceType();

    int getCurrentState();

    Date getStateChangeTime();

    ServiceID getID();

    /**
     * Returns a list of QueueStats objects that represent the queues in
     * this service.
     * If there are no queues, null is returned.
     */
    Collection getQueueStatistics();

    /**
     * Returns a QueueStats object that represent the queue in
     * this service.
     * If there is no queue with the given name, null is returned.
     */
    WorkerPoolStats getQueueStatistics(String name);
    
    
    /**
     * Returns a list of ConnectionPoolStat objects for the specific 
     * ConnectorBinding service
     * If there are no ConnectionPoolStats, null is returned.
     */
    Collection<ConnectionPoolStats> getConnectionPoolStats();
    
    
    /**
     * There are reflective based calls on this
     */
    void setInitException(Throwable t);

    Throwable getInitException();
    
    /**
     * There are reflective based calls on this
     */    
    void updateState(int state);
    
    ServiceData getServiceData();
}
