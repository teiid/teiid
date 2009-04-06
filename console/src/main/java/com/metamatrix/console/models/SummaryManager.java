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

package com.metamatrix.console.models;
/*
 * (c) Copyright 2000-2002 MetaMatrix, Inc.
 * All Rights Reserved.
 */

import java.util.Collection;
import java.util.Date;
import java.util.HashSet;
import java.util.Iterator;

import com.metamatrix.admin.api.objects.Session;
import com.metamatrix.common.config.api.Host;
import com.metamatrix.common.config.api.HostID;
import com.metamatrix.common.log.LogManager;
import com.metamatrix.console.connections.ConnectionInfo;
import com.metamatrix.console.ui.views.summary.SummaryHostInfo;
import com.metamatrix.console.ui.views.summary.SummaryInfoProvider;
import com.metamatrix.console.util.LogContexts;
import com.metamatrix.platform.admin.api.RuntimeStateAdminAPI;
import com.metamatrix.platform.admin.api.runtime.HostData;
import com.metamatrix.platform.admin.api.runtime.SystemState;

/**
 * Subclass of Manager to manage data for the 'Summary' tab (SummaryPanel).
 */
public class SummaryManager extends TimedManager implements SummaryInfoProvider {
    private Date systemStartUpTime = null;

//***** Constructor / initialization

    public SummaryManager(ConnectionInfo connection) {
        super(connection);
        super.init();
    }

    public void init() {
        super.init();
    }

    public String getSysURL() {
    	return getConnection().getURL();
    }
    
    public String getSystemName() throws Exception {
        return getConnection().getClusterName();
    }


    public String getSystemURL() throws Exception {
        String serverName = ""; //$NON-NLS-1$
        return serverName;
    }

    public int getSystemState() throws Exception {
        RuntimeStateAdminAPI api = ModelManager.getRuntimeStateAPI(getConnection());
        int state = SummaryInfoProvider.GREEN;
        if (!api.isSystemStarted()) {
            state = SummaryInfoProvider.RED;
        } else if (api.getSystemState().hasFailedService()) {
            state = SummaryInfoProvider.YELLOW;
        }
        return state;
    }

    public Date getSystemStartUpTime() throws Exception {
        //if (systemStartUpTime == null) {
            RuntimeStateAdminAPI runtimeState = ModelManager.getRuntimeStateAPI(
            		getConnection());
            systemStartUpTime = runtimeState.getServerStartTime();
        //}
        return systemStartUpTime;
    }

    public int getActiveSessionCount() throws Exception {
        return ModelManager.getSessionManager(this.getConnection()).getSessions().size();
    }

    public SummaryHostInfo[] getHostInfo() throws Exception {
        SystemState state = ModelManager.getRuntimeStateAPI(getConnection())
        		.getSystemState();
        Collection /*<HostData>*/ hostDataColl = state.getHosts();
        Iterator it = hostDataColl.iterator();
        HostData[] hostData = new HostData[hostDataColl.size()];
        for (int i = 0; it.hasNext(); i++) {
            hostData[i] = (HostData)it.next();
        }
        Collection hosts = getHosts();
        SummaryHostInfo[] info = new SummaryHostInfo[hosts.size()];
        it = hosts.iterator();
        for (int i = 0; it.hasNext(); i++) {
            Host curHost = (Host)it.next();
            info[i] = new SummaryHostInfo(curHost.getName(), hostStatus(curHost.getName(), hostData));
        }
        return info;
    }

    public Collection getHosts() throws Exception {
        Collection hosts = new HashSet();
        Iterator itDeployedHostIDs = ModelManager.getConfigurationAPI(
        		getConnection()).getCurrentConfiguration().getHostIDs()
        		.iterator();

        Host host = null;
        HostID hostID = null;
        while( itDeployedHostIDs.hasNext() ) {
            hostID = (HostID)itDeployedHostIDs.next();
            host = ModelManager.getConfigurationAPI(getConnection()).getHost(hostID);
            hosts.add(host);
        }
        return hosts;
    }

/******************************
/    Private internal methods
******************************/

    private int hostStatus(String hostName, HostData[] hostData) {
        int status;
        int loc = -1;
        int i = 0;
        while ((i < hostData.length) && (loc < 0)) {
            if (hostName.equalsIgnoreCase(hostData[i].getName())) {
                loc = i;
            } else {
                i++;
            }
        }
        if (loc < 0) {
            LogManager.logError(LogContexts.SUMMARY,
                    "Host Identifier " + hostName + " found in configuration but " + //$NON-NLS-1$ //$NON-NLS-2$
                    "not found in runtime state."); //$NON-NLS-1$
            status = SummaryHostInfo.NOT_RUNNING;
        } else {
            if (hostData[loc].isRegistered()) {
                status = SummaryHostInfo.RUNNING;
            } else {
                status = SummaryHostInfo.NOT_RUNNING;
            }
        }
        return status;
    }

	@Override
	public Date getLastSessionStartUp() throws Exception {
		Collection<Session> sessions = ModelManager.getSessionManager(this.getConnection()).getSessions();
		Date start = null;
		for (Session session : sessions) {
			Date createDate = session.getCreatedDate();
			if (start == null || start.compareTo(createDate) > 0) {
				start = createDate;
			}
		}
		return start;
	}
}
