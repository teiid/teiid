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

package com.metamatrix.console.ui.views.runtime.model;

import java.util.ArrayList;
import java.util.List;

import com.metamatrix.platform.admin.api.runtime.HostData;
import com.metamatrix.platform.admin.api.runtime.ProcessData;
import com.metamatrix.platform.admin.api.runtime.ServiceData;
import com.metamatrix.platform.service.api.ServiceState;

public class HostStatistics
    implements StatisticsConstants {

    ///////////////////////////////////////////////////////////////////////////
    // FIELDS
    ///////////////////////////////////////////////////////////////////////////

    private HostData host;
    ArrayList synchedProcs = new ArrayList();
    ArrayList notRegProcs = new ArrayList();
    ArrayList notDeployedProcs = new ArrayList();
    private Object[] processStats = new Object[NUM_PROCESS_STATS];

    ArrayList runningServs = new ArrayList();
    ArrayList synchedServs = new ArrayList();
    ArrayList notRegServs = new ArrayList();
    ArrayList notDeployedServs = new ArrayList();
    ArrayList failedServs = new ArrayList();
    ArrayList stoppedServs = new ArrayList();
    ArrayList initFailedServs = new ArrayList();
    ArrayList notInitServs = new ArrayList();
    ArrayList dataSourceUnavailableServs = new ArrayList();
    private Object[] serviceStats = new Object[NUM_SERV_STATS];

    ///////////////////////////////////////////////////////////////////////////
    // CONSTRUCTORS
    ///////////////////////////////////////////////////////////////////////////

    public HostStatistics(HostData theHost) {
        host = theHost;
    }

    ///////////////////////////////////////////////////////////////////////////
    // METHODS
    ///////////////////////////////////////////////////////////////////////////

    public void addProcess(ProcessData theProcess) {
        if (theProcess.isDeployed()) {
            if (theProcess.isRegistered()) {
                synchedProcs.add(theProcess);
            }
            else {
                notRegProcs.add(theProcess);
            }
        }
        else {
            if (theProcess.isRegistered()) {
                notDeployedProcs.add(theProcess);
            }
            else {
                synchedProcs.add(theProcess);
            }
        }
    }
    public void addService(ServiceData theService) {
        boolean registered = theService.isRegistered();
        if (theService.isDeployed()) {
            if (registered) {
                synchedServs.add(theService);
            } else {
                notRegServs.add(theService);
            }
        } else {
            if (registered) {
                notDeployedServs.add(theService);
            } else {
                synchedServs.add(theService);
            }
        }
        if (registered) {
            int state = theService.getCurrentState();
            if (state == ServiceState.STATE_OPEN) {
                runningServs.add(theService);
            } else if (state == ServiceState.STATE_CLOSED) {
                stoppedServs.add(theService);
            } else if (state == ServiceState.STATE_FAILED) {
                failedServs.add(theService);
            } else if (state == ServiceState.STATE_INIT_FAILED) {
                initFailedServs.add(theService);
            } else if (state == ServiceState.STATE_NOT_INITIALIZED) {
                notInitServs.add(theService);
            } else if (state == ServiceState.STATE_DATA_SOURCE_UNAVAILABLE) {
                dataSourceUnavailableServs.add(theService);
            }
        }
    }

    public List getFailedServices() {
        return failedServs;
    }

    public List getInitFailedServices() {
        return initFailedServs;
    }

    public List getNotDeployedProcesses() {
        return notDeployedProcs;
    }

    public List getNotDeployedServices() {
        return notDeployedServs;
    }

    public List getNotInitServices() {
        return notInitServs;
    }

    public List getNotRegisteredProcesses() {
        return notRegProcs;
    }

    public List getNotRegisteredServices() {
        return notRegServs;
    }

    public List getDataSourceUnavailableServices() {
        return dataSourceUnavailableServs;
    }
    
    public List getProcesses() {
        List clone = (List)synchedProcs.clone();
        clone.addAll(notRegProcs);
        clone.addAll(notDeployedProcs);
        return clone;
    }

    public Object[] getProcessStats() {
        processStats[PROC_HOST_INDEX] = host;
        processStats[SYNCHED_PROCS_INDEX] =
            new Integer(synchedProcs.size());
        processStats[NOT_REGISTERED_PROCS_INDEX] =
            new Integer(notRegProcs.size());
        processStats[NOT_DEPLOYED_PROCS_INDEX] =
            new Integer(notDeployedProcs.size());
        processStats[TOTAL_PROCS_INDEX] =
            new Integer(
                ((Integer)processStats[SYNCHED_PROCS_INDEX]).intValue() +
                ((Integer)processStats[NOT_REGISTERED_PROCS_INDEX]).intValue() +
                ((Integer)processStats[NOT_DEPLOYED_PROCS_INDEX]).intValue());
        return processStats;

    }

    public List getRunningServices() {
        return runningServs;
    }

    public List getServices() {
        List clone = (List)synchedServs.clone();
        clone.addAll(notRegServs);
        clone.addAll(notDeployedServs);
        return clone;
    }

    public Object[] getServiceStats() {
        serviceStats[SERV_HOST_INDEX] = host;
        serviceStats[RUNNING_INDEX] =
            new Integer(runningServs.size());
        serviceStats[SYNCHED_SERVS_INDEX] =
            new Integer(synchedServs.size());
        serviceStats[NOT_REGISTERED_SERVS_INDEX] =
            new Integer(notRegServs.size());
        serviceStats[NOT_DEPLOYED_SERVS_INDEX] =
            new Integer(notDeployedServs.size());
        serviceStats[FAILED_INDEX] =
            new Integer(failedServs.size());
        serviceStats[STOPPED_INDEX] =
            new Integer(stoppedServs.size());
        serviceStats[INIT_FAILED_INDEX] =
            new Integer(initFailedServs.size());
        serviceStats[NOT_INIT_INDEX] =
            new Integer(notInitServs.size());
        serviceStats[DATA_SOURCE_UNAVAILABLE_INDEX] =
            new Integer(dataSourceUnavailableServs.size());
        serviceStats[TOTAL_SERVS_INDEX] =
            new Integer(
                ((Integer)serviceStats[RUNNING_INDEX]).intValue() +
                ((Integer)serviceStats[FAILED_INDEX]).intValue() +
                ((Integer)serviceStats[STOPPED_INDEX]).intValue() +
                ((Integer)serviceStats[INIT_FAILED_INDEX]).intValue() +                
                ((Integer)serviceStats[NOT_INIT_INDEX]).intValue() +
                ((Integer)serviceStats[DATA_SOURCE_UNAVAILABLE_INDEX]).intValue());
        return serviceStats;
    }

    public List getStoppedServices() {
        return stoppedServs;
    }


    public List getSynchedProcesses() {
        return synchedProcs;
    }

    public List getSynchedServices() {
        return synchedServs;
    }

}
