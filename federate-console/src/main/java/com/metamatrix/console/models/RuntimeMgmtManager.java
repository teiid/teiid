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

package com.metamatrix.console.models;

import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;

import com.metamatrix.console.connections.ConnectionInfo;
import com.metamatrix.console.ui.util.property.PropertyProvider;
import com.metamatrix.console.util.ExternalException;
import com.metamatrix.platform.admin.api.RuntimeStateAdminAPI;
import com.metamatrix.platform.admin.api.runtime.HostData;
import com.metamatrix.platform.admin.api.runtime.PSCData;
import com.metamatrix.platform.admin.api.runtime.ProcessData;
import com.metamatrix.platform.admin.api.runtime.ServiceData;
import com.metamatrix.platform.admin.api.runtime.SystemState;
import com.metamatrix.platform.service.api.ServiceInterface;

public final class RuntimeMgmtManager
    extends Manager {

    ///////////////////////////////////////////////////////////////////////////
    // FIELDS
    ///////////////////////////////////////////////////////////////////////////

    //key=ProcessData, value=Integer[2] {registered psc count, not reg psc count}
    private HashMap procPscMap = new HashMap();

    private PropertyProvider propProvider =
        new PropertyProvider("com/metamatrix/console/data/runtime_mgr"); //$NON-NLS-1$
    

    
    

    ///////////////////////////////////////////////////////////////////////////
    // CONSTRUCTORS
    ///////////////////////////////////////////////////////////////////////////

    public RuntimeMgmtManager(ConnectionInfo connection)
        	throws ExternalException {
		super(connection);
        refreshImpl();
    }

    ///////////////////////////////////////////////////////////////////////////
    // METHODS
    ///////////////////////////////////////////////////////////////////////////
        
    
    public void bounceServer() throws ExternalException {
        refreshImpl();
        try {
            ConnectionInfo connection = getConnection();

            // get QueryManager listeners.
            List qmListeners = ModelManager.getQueryManager(connection).getEventListeners();

            getAPI().bounceServer();
            
            ModelManager.removeConnection(connection);
            connection.relogin();
            refreshImpl();
            
            //Reinitialize QueryManager listener. 
            QueryManager qm = ModelManager.getQueryManager(connection);
            Iterator iter = qmListeners.iterator();
            while (iter.hasNext()) {
            	qm.addManagerListener((ManagerListener) iter.next());
            }
        } catch (Exception theException) {
            theException.printStackTrace();
            throw new ExternalException(
                formatErrorMsg("bounceServer", theException), //$NON-NLS-1$
                theException);
        }
    }
    
    private String formatErrorMsg(
        String theMethodName,
        Exception theException) {

        return formatErrorMsg(theMethodName, null, theException);
    }

    private String formatErrorMsg(
        String theMethodName,
        String theDetails,
        Exception theException) {

        return theException.getMessage() +
               " < RuntimeMgmtManager." + theMethodName + //$NON-NLS-1$
               ((theDetails == null) ? "" : ":" + theDetails) + //$NON-NLS-1$ //$NON-NLS-2$
               " >"; //$NON-NLS-1$
    }

    public Integer[] getPscCounts(ProcessData theProcess) {
        Integer[] counts = (Integer[])procPscMap.get(theProcess);
        if (counts == null) {
            int notRegistered = 0;
            int registered = 0;
            Collection pscs = theProcess.getPSCs();
            if ((pscs != null) && !pscs.isEmpty()) {
                Iterator itr = pscs.iterator();
                PSCData psc = (PSCData)itr.next();
                if (psc.isRegistered()) {
                    registered++;
                }
                else {
                    notRegistered++;
                }
            }
            counts = new Integer[] {new Integer(registered),
                                    new Integer(notRegistered)};
            procPscMap.put(theProcess, counts);
        }
        return counts;
    }

    public SystemState getServerState()
        throws ExternalException {
        refreshImpl();
        try {
            return getAPI().getSystemState();
        }
        catch (Exception theException) {
            theException.printStackTrace();
            throw new ExternalException(
                formatErrorMsg("getServerState", theException), //$NON-NLS-1$
                theException);
        }
    }

    private boolean isNotRegistered(ServiceData theService) {
        return (theService.getCurrentState() ==
                ServiceInterface.STATE_NOT_REGISTERED);
    }
    
    /**
     * called to determine is a service is in the system state, regardless if running.
     *  
     * @param serviceName
     * @return
     * @throws ExternalException
     * @since 4.3
     */
    public boolean isServiceInSystemState(String serviceName) throws ExternalException {

//        List hostNames = null;
        try {
                SystemState state = getServerState();
                Collection hosts = state.getHosts();
                // HOST_LOOP:
                if ((hosts != null) && (!hosts.isEmpty())) {
//                    hostNames = new ArrayList(hosts.size());
//                    boolean matchedHost = false;
                    //
                    // loop through hosts
                    //
                    Iterator hostItr = hosts.iterator();
                    while (hostItr.hasNext()) {
 //                       matchedHost = false;
                        HostData host = (HostData)hostItr.next();
                        //
                        // loop through processes
                        //
                        Collection procs = host.getProcesses();
                        if ((procs != null) && (!procs.isEmpty())) {
                            Iterator procItr = procs.iterator();
                            while (procItr.hasNext()) {
                                ProcessData process =
                                    (ProcessData)procItr.next();
                                //
                                // loop through PSCs
                                //
                                Collection pscs = process.getPSCs();
                                if ((pscs != null) && (!pscs.isEmpty())) {
                                    Iterator pscItr = pscs.iterator();
                                    while (pscItr.hasNext()) {
                                        PSCData psc = (PSCData)pscItr.next();
                                        //
                                        // loop through services
                                        //
                                        Collection services = psc.getServices();
                                        if ((services != null) &&
                                            (!services.isEmpty())) {
                                            Iterator serviceItr =
                                                services.iterator();
                                            while (serviceItr.hasNext()) {
                                                ServiceData service =
                                                    (ServiceData)serviceItr.next();
                                                if (service.getComponentDefnID().getName().equalsIgnoreCase(serviceName)) {
                                                    if (service.getCurrentState() == ServiceInterface.STATE_OPEN) {
//                                                        hostNames.add(host.getName());
                                                        return true;
                                                    }
                                                }
                                            }
                                        }
                                    } // end while PSCs
                                } // end if PSCs
                                
                            } // end while Processes
                            
                        } // end if Processes
                        
                    } // end while Hosts
                    
                } // end if Hosts

        } catch (Exception theException) {
            theException.printStackTrace();
            throw new ExternalException(
                formatErrorMsg("getHostNamesWhereServiceiSRunning", theException), //$NON-NLS-1$
                theException);
        }
        return false;

        
    }

    public void refreshImpl()
        throws ExternalException {

        procPscMap.clear();
    }

    public void shutdownServer()
        throws ExternalException {
        refreshImpl();
        try {
        	getAPI().shutdownServer();
        }
        catch (Exception theException) {
            theException.printStackTrace();
            throw new ExternalException(
                formatErrorMsg("shutdownServer", theException), //$NON-NLS-1$
                theException);
        }
    }

    public void startHost(HostData theHost)
        throws ExternalException {
        refreshImpl();
        try {
        	getAPI().startHost(theHost.getName());
        }
        catch (Exception theException) {
            theException.printStackTrace();
            throw new ExternalException(
                formatErrorMsg("startHost", "host=" + theHost, theException), //$NON-NLS-1$ //$NON-NLS-2$
                theException);
        }
    }

    public void startProcess(ProcessData theProcess)
        throws ExternalException {
        refreshImpl();
        try {
        	getAPI().startProcess(theProcess.getHostName(), theProcess.getName());
        }
        catch (Exception theException) {
            theException.printStackTrace();
            throw new ExternalException(
                formatErrorMsg("startProcess", //$NON-NLS-1$
                               "process=" + theProcess, //$NON-NLS-1$
                               theException),
                theException);
        }
    }

    public void startPsc(PSCData thePsc)
        throws ExternalException {
        refreshImpl();
        try {
        	getAPI().startPSC(thePsc.getPscID());
        }
        catch (Exception theException) {
            theException.printStackTrace();
            throw new ExternalException(
                formatErrorMsg("startPsc", "psc=" + thePsc, theException), //$NON-NLS-1$ //$NON-NLS-2$
                theException);
        }
    }

    public void startService(ServiceData theService)
        throws ExternalException {
        refreshImpl();
        try {
        	getAPI().restartService(theService.getServiceID());
        }
        catch (Exception theException) {
            theException.printStackTrace();
            throw new ExternalException(
                formatErrorMsg("startService", //$NON-NLS-1$
                               "service=" + theService, //$NON-NLS-1$
                               theException),
                theException);
        }
    }

    public void stopHost(HostData theHost)
        throws ExternalException {
        refreshImpl();
        try {
        	getAPI().stopHost(theHost.getName());
        }
        catch (Exception theException) {
            theException.printStackTrace();
            //don't rethrow: stopping the host you're connected to can cause an error 
        }
    }

    public void stopHostNow(HostData theHost)
        throws ExternalException {
        refreshImpl();
        try {
        	getAPI().stopHostNow(theHost.getName());
        }
        catch (Exception theException) {
            theException.printStackTrace();
            //don't rethrow: stopping the host you're connected to can cause an error 
        }
    }

    public void stopProcess(ProcessData theProcess)
        throws ExternalException {
        refreshImpl();
        try {
        	getAPI().stopProcess(theProcess.getProcessID());
        }
        catch (Exception theException) {
            theException.printStackTrace();
            //don't rethrow: stopping the host you're connected to can cause an error 
        }
    }

    public void stopProcessNow(ProcessData theProcess)
        throws ExternalException {
        refreshImpl();
        try {
        	getAPI().stopProcessNow(theProcess.getProcessID());
        }
        catch (Exception theException) {
            theException.printStackTrace();
            //don't rethrow: stopping the host you're connected to can cause an error 
        }
    }

    public void stopPsc(PSCData thePsc)
        throws ExternalException {
        refreshImpl();
        try {
        	getAPI().stopPSC(thePsc.getPscID());
        }
        catch (Exception theException) {
            theException.printStackTrace();
            throw new ExternalException(
                formatErrorMsg("stopPsc", "psc=" + thePsc, theException), //$NON-NLS-1$ //$NON-NLS-2$
                theException);
        }
    }

    public void stopPscNow(PSCData thePsc)
        throws ExternalException {
        refreshImpl();
        try {
        	getAPI().stopPSCNow(thePsc.getPscID());
        }
        catch (Exception theException) {
            theException.printStackTrace();
            throw new ExternalException(
                formatErrorMsg("stopPscNow", "psc=" + thePsc, theException), //$NON-NLS-1$ //$NON-NLS-2$
                theException);
        }
    }

    public void stopService(ServiceData theService)
        throws ExternalException {
        refreshImpl();
        try {
        	getAPI().stopService(theService.getServiceID());
        }
        catch (Exception theException) {
            theException.printStackTrace();
            throw new ExternalException(
                formatErrorMsg("stopService", //$NON-NLS-1$
                               "service=" + theService, //$NON-NLS-1$
                               theException),
                theException);
        }
    }

    public void stopServiceNow(ServiceData theService)
        throws ExternalException {
        refreshImpl();
        try {
        	getAPI().stopServiceNow(theService.getServiceID());
        }
        catch (Exception theException) {
            theException.printStackTrace();
            throw new ExternalException(
                formatErrorMsg("stopServiceNow", //$NON-NLS-1$
                               "service=" + theService, //$NON-NLS-1$
                               theException),
                theException);
        }
    }

   
    public synchronized void synchronizeServer()
        throws ExternalException {
        refreshImpl();
        try {
        	getAPI().synchronizeServer();
            // get a list of services
            // wait until all services have a state that is registered
            // or until a certain number of iterations have occurred
            int synchLoop = 0;
            final int LOOPS = propProvider.getInt("synch.maxduration.sec", 20); //$NON-NLS-1$
            final int DURATION =
                1000 * propProvider.getInt("synch.sleep.sec", 1); //$NON-NLS-1$
            boolean synching = true;
            while (synching && (synchLoop < LOOPS)) {
                synchLoop++;
                synching = false;
                SystemState state = getServerState();
                Collection hosts = state.getHosts();
                HOST_LOOP:
                if ((hosts != null) && (!hosts.isEmpty())) {
                    //
                    // loop through hosts
                    //
                    Iterator hostItr = hosts.iterator();
                    while (hostItr.hasNext()) {
                        HostData host = (HostData)hostItr.next();
                        //
                        // loop through processes
                        //
                        Collection procs = host.getProcesses();
                        if ((procs != null) && (!procs.isEmpty())) {
                            Iterator procItr = procs.iterator();
                            while (procItr.hasNext()) {
                                ProcessData process =
                                    (ProcessData)procItr.next();
                                //
                                // loop through PSCs
                                //
                                Collection pscs = process.getPSCs();
                                if ((pscs != null) && (!pscs.isEmpty())) {
                                    Iterator pscItr = pscs.iterator();
                                    while (pscItr.hasNext()) {
                                        PSCData psc = (PSCData)pscItr.next();
                                        //
                                        // loop through services
                                        //
                                        Collection services = psc.getServices();
                                        if ((services != null) &&
                                            (!services.isEmpty())) {
                                            Iterator serviceItr =
                                                services.iterator();
                                            while (serviceItr.hasNext()) {
                                                ServiceData service =
                                                    (ServiceData)serviceItr.next();
                                                if (isNotRegistered(service)) {
                                                    synching = true;
                                                    break HOST_LOOP;
                                                }
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
                if (synching) {
                    try {
                        Thread.sleep(DURATION);
                    }
                    catch (InterruptedException theException) {
                    }
                }
            }
        }
        catch (Exception theException) {
            theException.printStackTrace();
            throw new ExternalException(
                formatErrorMsg("synchronizeServer", theException), //$NON-NLS-1$
                theException);
        }
    }
    
    /**
     * Get a connection to the RuntimeStateAdminAPI 
     * @param reconnect If true, reconnect to the server if not connected.
     * @return
     * @since 4.3
     */
    private RuntimeStateAdminAPI getAPI(boolean reconnect) {
    	return ModelManager.getRuntimeStateAPI(getConnection(reconnect));
    }
    
    /**
     * Get a connection to the RuntimeStateAdminAPI 
     * Reconnect to the server if not connected.
     * @return
     */
    private RuntimeStateAdminAPI getAPI() {
        return getAPI(true);
    }
        
}
