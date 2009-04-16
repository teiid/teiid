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

package com.metamatrix.platform.admin.api;

import java.util.Collection;
import java.util.Date;
import java.util.List;

import com.metamatrix.admin.api.exception.security.InvalidSessionException;
import com.metamatrix.api.exception.MetaMatrixComponentException;
import com.metamatrix.api.exception.MultipleException;
import com.metamatrix.api.exception.security.AuthorizationException;
import com.metamatrix.common.config.api.Configuration;
import com.metamatrix.common.log.LogConfiguration;
import com.metamatrix.common.queue.WorkerPoolStats;
import com.metamatrix.platform.admin.api.runtime.SystemState;
import com.metamatrix.platform.service.api.ServiceID;
import com.metamatrix.platform.vm.controller.ProcessStatistics;

public interface RuntimeStateAdminAPI extends SubSystemAdminAPI {

    
    /**
     * Return TRUE if the system is started; i.e. at leat one of every essential services in a product is running. Authorization,
     * Configuration, Membership and Session services are considered to be essential.
     * 
     * @return Boolean - TRUE if system is started, FALSE if not.
     * @throws AuthorizationException
     *             if caller is not authorized to perform this method.
     * @throws InvalidSessionException
     *             if the <code>callerSessionID</code> is not valid or is expired.
     * @throws MetaMatrixComponentException
     *             if an error occurred in communicating with a component.
     */
    boolean isSystemStarted() throws AuthorizationException,
                             InvalidSessionException,
                             MetaMatrixComponentException;

    /**
     * Stop service once work is complete.
     * 
     * @param callerSessionID
     *            ID of the caller's current session.
     * @param serviceID
     *            ID of service instance.
     * @throws AuthorizationException
     *             if caller is not authorized to perform this method.
     * @throws InvalidSessionException
     *             if the <code>callerSessionID</code> is not valid or is expired.
     * @throws MetaMatrixComponentException
     *             if an error occurred in communicating with a component.
     */
    void stopService(ServiceID serviceID) throws AuthorizationException,
                                         InvalidSessionException,
                                         MetaMatrixComponentException;

    /**
     * Stop service now.
     * 
     * @param callerSessionID
     *            ID of the caller's current session.
     * @param serviceID
     *            ID of service instance.
     * @throws AuthorizationException
     *             if caller is not authorized to perform this method.
     * @throws InvalidSessionException
     *             if the <code>callerSessionID</code> is not valid or is expired.
     * @throws MetaMatrixComponentException
     *             if an error occurred in communicating with a component.
     */
    void stopServiceNow(ServiceID serviceID) throws AuthorizationException,
                                            InvalidSessionException,
                                            MetaMatrixComponentException;

    /**
     * Stop process once work is complete.
     * 
     * @param callerSessionID
     *            ID of the caller's current session.
     * @param processID
     *            <code>VMControllerID</code>.
     * @throws AuthorizationException
     *             if caller is not authorized to perform this method.
     * @throws InvalidSessionException
     *             if the <code>callerSessionID</code> is not valid or is expired.
     * @throws MetaMatrixComponentException
     *             if an error occurred in communicating with a component.
     */
    void stopProcess(String hostname, String processName, boolean now) throws AuthorizationException,
                                              InvalidSessionException,
                                              MetaMatrixComponentException;

    /**
     * Gracefully shutdown server waiting for work to complete.
     * 
     * @param callerSessionID
     *            ID of the caller's current session.
     * @throws AuthorizationException
     *             if caller is not authorized to perform this method.
     * @throws InvalidSessionException
     *             if the <code>callerSessionID</code> is not valid or is expired.
     * @throws MetaMatrixComponentException
     *             if an error occurred in communicating with a component.
     */
    void shutdownServer() throws AuthorizationException,
                         InvalidSessionException,
                         MetaMatrixComponentException;

    /**
     * Return the running state of the system.
     * 
     * @return SysteState object that represents the system.
     * @throws AuthorizationException
     *             if caller is not authorized to perform this method.
     * @throws InvalidSessionException
     *             if the <code>callerSessionID</code> is not valid or is expired.
     * @throws MetaMatrixComponentException
     *             if an error occurred in communicating with a component.
     */
    SystemState getSystemState() throws AuthorizationException,
                                InvalidSessionException,
                                MetaMatrixComponentException;

    /**
     * Restart a failed or stopped service.
     * 
     * @param serviceID
     *            ID of service instance.
     * @throws AuthorizationException
     *             if caller is not authorized to perform this method.
     * @throws InvalidSessionException
     *             if the <code>callerSessionID</code> is not valid or is expired.
     * @throws MetaMatrixComponentException
     *             if an error occurred in communicating with a component.
     */
    void restartService(ServiceID serviceID) throws AuthorizationException,
                                            InvalidSessionException,
                                            MetaMatrixComponentException;

    /**
     * Start up all processes and services for the host.
     * 
     * @param host
     *            Host to start
     * @throws AuthorizationException
     *             if caller is not authorized to perform this method.
     * @throws InvalidSessionException
     *             if the <code>callerSessionID</code> is not valid or is expired.
     * @throws MetaMatrixComponentException
     *             if an error occurred in communicating with a component.
     */
    void startHost(String host) throws AuthorizationException,
                               InvalidSessionException,
                               MetaMatrixComponentException;

    /**
     * Start up all process and services.
     * 
     * @param host
     *            Host processes belongs to.
     * @param process
     *            Processes to start
     * @throws AuthorizationException
     *             if caller is not authorized to perform this method.
     * @throws InvalidSessionException
     *             if the <code>callerSessionID</code> is not valid or is expired.
     * @throws MetaMatrixComponentException
     *             if an error occurred in communicating with a component.
     */
    void startProcess(String host,
                      String process) throws AuthorizationException,
                                     InvalidSessionException,
                                     MetaMatrixComponentException;

    /**
     * Stop host processes/services once work is complete.
     * 
     * @param host
     *            Name of host.
     * @throws AuthorizationException
     *             if caller is not authorized to perform this method.
     * @throws InvalidSessionException
     *             if the <code>callerSessionID</code> is not valid or is expired.
     * @throws MetaMatrixComponentException
     *             if an error occurred in communicating with a component.
     */
    void stopHost(String host) throws AuthorizationException,
                              InvalidSessionException,
                              MetaMatrixComponentException,
                              MultipleException;

    /**
     * Stop host processes/services now.
     * 
     * @param host
     *            Name of host.
     * @throws AuthorizationException
     *             if caller is not authorized to perform this method.
     * @throws InvalidSessionException
     *             if the <code>callerSessionID</code> is not valid or is expired.
     * @throws MetaMatrixComponentException
     *             if an error occurred in communicating with a component.
     */
    void stopHostNow(String host) throws AuthorizationException,
                                 InvalidSessionException,
                                 MetaMatrixComponentException,
                                 MultipleException;

    /**
     * Start up all services in psc.
     * 
     * @param pscID
     *            PSC to start.
     * @throws AuthorizationException
     *             if caller is not authorized to perform this method.
     * @throws InvalidSessionException
     *             if the <code>callerSessionID</code> is not valid or is expired.
     * @throws MetaMatrixComponentException
     *             if an error occurred in communicating with a component.
     */
//    void startPSC(PscID pscID) throws AuthorizationException,
//                              InvalidSessionException,
//                              MetaMatrixComponentException,
//                              MultipleException;
//
    /**
     * Stop up all services in psc, waiting for work to complete.
     * 
     * @param pscID
     *            PSC to stop.
     * @throws AuthorizationException
     *             if caller is not authorized to perform this method.
     * @throws InvalidSessionException
     *             if the <code>callerSessionID</code> is not valid or is expired.
     * @throws MetaMatrixComponentException
     *             if an error occurred in communicating with a component.
     */
//    void stopPSC(PscID pscID) throws AuthorizationException,
//                             InvalidSessionException,
//                             MetaMatrixComponentException,
//                             MultipleException;

    /**
     * Stop up all services in psc now.
     * 
     * @param pscID
     *            PSC to stop.
     * @throws AuthorizationException
     *             if caller is not authorized to perform this method.
     * @throws InvalidSessionException
     *             if the <code>callerSessionID</code> is not valid or is expired.
     * @throws MetaMatrixComponentException
     *             if an error occurred in communicating with a component.
     */
//    void stopPSCNow(PscID pscID) throws AuthorizationException,
//                                InvalidSessionException,
//                                MetaMatrixComponentException,
//                                MultipleException;

    /**
     * Synchronize running services with runtime configuration.
     * 
     * @throws AuthorizationException
     *             if caller is not authorized to perform this method.
     * @throws InvalidSessionException
     *             if the <code>callerSessionID</code> is not valid or is expired.
     * @throws MetaMatrixComponentException
     *             if an error occurred in communicating with a component.
     */
    void synchronizeServer() throws AuthorizationException,
                            InvalidSessionException,
                            MetaMatrixComponentException,
                            MultipleException;

    /**
     * Returns a Date object representing the time the server was started. If the server is not started a null is returned.
     * 
     * @return Date
     * @throws AuthorizationException
     *             if caller is not authorized to perform this method.
     * @throws InvalidSessionException
     *             if the <code>callerSessionID</code> is not valid or is expired.
     * @throws MetaMatrixComponentException
     *             if an error occurred in communicating with a component.
     */
    Date getServerStartTime() throws AuthorizationException,
                             InvalidSessionException,
                             MetaMatrixComponentException;

    /**
     * Sets the <code>LogConfiguration</code> on the given <code>Configuration</code>. If the configuration is
     * <code>operational</code>, then the log configuration is set on the <code>Logmanager</code> running in each VM.
     * 
     * @param config
     *            The configuration for which to set the log configuration.
     * @param logConfig
     *            The log configuration with which to affect the log properties.
     * @param actions
     *            The <code>Actions</code> from the <code>ConfigurationObjectEditor</code> used to affect the configuration
     *            database.
     * @throws AuthorizationException
     *             if caller is not authorized to perform this method.
     * @throws InvalidSessionException
     *             if the <code>callerSessionID</code> is not valid or is expired.
     * @throws MetaMatrixComponentException
     *             if an error occurred in communicating with a component.
     */
    void setLoggingConfiguration(Configuration config,
                                 LogConfiguration logConfig,
                                 List actions) throws AuthorizationException,
                                              InvalidSessionException,
                                              MetaMatrixComponentException;

    /**
     * Return Collection of QueueStats for service.
     * 
     * @param serviceID
     *            ID of the service.
     * @return Collection of QueueStats objects.
     * @throws AuthorizationException
     *             if caller is not authorized to perform this method.
     * @throws InvalidSessionException
     *             if the <code>callerSessionID</code> is not valid or is expired.
     * @throws MetaMatrixComponentException
     *             if an error occurred in communicating with a component.
     */

    Collection getServiceQueueStatistics(ServiceID serviceID) throws AuthorizationException,
                                                             InvalidSessionException,
                                                             MetaMatrixComponentException;

    /**
     * Return QueueStats object for queue.
     * 
     * @param serviceID
     *            ID of the service.
     * @param queueName
     *            Name of queue.
     * @return QueueStats object.
     * @throws AuthorizationException
     *             if caller is not authorized to perform this method.
     * @throws InvalidSessionException
     *             if the <code>callerSessionID</code> is not valid or is expired.
     * @throws MetaMatrixComponentException
     *             if an error occurred in communicating with a component.
     */
    WorkerPoolStats getServiceQueueStatistics(ServiceID serviceID,
                                              String queueName) throws AuthorizationException,
                                                               InvalidSessionException,
                                                               MetaMatrixComponentException;

    /**
     * Return VMStatistics object for Process.
     * 
     * @param VMControllerID
     *            ID of the process.
     * @return VMStatistics.
     * @throws AuthorizationException
     *             if caller is not authorized to perform this method.
     * @throws InvalidSessionException
     *             if the <code>callerSessionID</code> is not valid or is expired.
     * @throws MetaMatrixComponentException
     *             if an error occurred in communicating with a component.
     */
    ProcessStatistics getProcessStatistics(String hostName, String processName) throws AuthorizationException,
                                                     InvalidSessionException,
                                                     MetaMatrixComponentException;

    /**
     * Return the PscID by Name
     * 
     * @param hostName
     *            name of the host process for the PSC
     * @param processName
     *            name of the process for the PSC
     * @param pscName
     *            name of the PSC
     * @return <PscID>
     * @throws ServiceNotBoundException
     * @throws InvalidSessionException
     *             if the <code>callerSessionID</code> is not valid or is expired.
     * @throws AuthorizationException
     * @throws MetaMatrixComponentException
     * @since 4.2.1
     */
//    PscID getPscIDByName(String hostName,
//                         String processName,
//                         String pscName) throws InvalidSessionException,
//                                        AuthorizationException,
//                                        MetaMatrixComponentException;

    /**
     * Return the ServiceID by Name
     * 
     * @param hostName
     *            name of the host to that the service is configured
     * @param processName
     *            process name of the service is configured
     * @param serviceName
     *            name of the service to be retrieved
     * @return <code>ServiceID</code>
     * @throws AuthorizationException
     *             if caller is not authorized to perform this method.
     * @throws InvalidSessionException
     *             if the <code>callerSessionID</code> is not valid or is expired.
     * @throws MetaMatrixComponentException
     *             if an error occurred in communicating with a component.
     * @since 4.2
     */
    ServiceID getServiceIDByName(String hostName,
                                 String processName,
                                 String serviceName) throws AuthorizationException,
                                                    InvalidSessionException,
                                                    MetaMatrixComponentException;

    /**
     * Returns a list that contains all of the VMControllerBindings for all the VMControllers running in the MetaMatrix system.
     * 
     * @return <code>List</code> of <code>VMControllerBinding</code>s
     * @throws InvalidSessionException
     *             if the <code>callerSessionID</code> is not valid or is expired.
     * @throws AuthorizationException
     * @throws MetaMatrixComponentException
     * @since 4.2.1
     */
    List getVMControllerBindings() throws InvalidSessionException,
                                  AuthorizationException,
                                  MetaMatrixComponentException;

    /**
     * Get the log entries that match the specified criteria. 
     * @param startTime
     * @param endTime  If null, will ignore this criterion.
     * @param levels List of Integers
     * @param levels List of Strings.  If null, will ignore this criterion and return entries with any context.
     * @param maxRows
     * @return List of LogEntry objects
     * @since 4.3
     */
    List getLogEntries(Date startTime,
                                Date endTime,
                                List levels,
                                List contexts,
                                int maxRows) throws AuthorizationException,
                                InvalidSessionException,
                                MetaMatrixComponentException;

}
