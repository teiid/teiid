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

package com.metamatrix.common.comm.platform.socket.client;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.teiid.adminapi.Admin;
import org.teiid.adminapi.AdminException;
import org.teiid.adminapi.ProcessObject;

import com.metamatrix.common.api.HostInfo;
import com.metamatrix.common.api.MMURL;
import com.metamatrix.platform.security.api.LogonResult;

/**
 * Will discover hosts based upon an anon admin api call.
 */
public class AdminApiServerDiscovery extends UrlServerDiscovery {
	
	/**
	 * If the FIREWALL_HOST property is set, then this host name will be used instead of the process
	 * names returned by the AdminApi
	 */
	public static final String USE_URL_HOST = "AdminApiServerDiscovery.useUrlHost"; //$NON-NLS-1$
	
	public static final int DISCOVERY_TIMEOUT = 120000;
	
	static class ClusterInfo {
		volatile long lastDiscoveryTime;
		volatile List<HostInfo> knownHosts = new ArrayList<HostInfo>();
	}
	
	private static Map<String, ClusterInfo> clusterInfo = Collections.synchronizedMap(new HashMap<String, ClusterInfo>());
	
	private boolean useUrlHost;
	
	@Override
	public void init(MMURL url, Properties p) {
		super.init(url, p);
		//TODO: this could be on a per cluster basis
		useUrlHost = Boolean.valueOf(p.getProperty(USE_URL_HOST)).booleanValue();
	}
	
	@Override
	public List<HostInfo> getKnownHosts(LogonResult result,
			SocketServerInstance instance) {
		if (result == null) {
			return super.getKnownHosts(result, instance);
		}
		ClusterInfo info = clusterInfo.get(result.getClusterName());
		if (info == null) {
			info = new ClusterInfo();
		}
		synchronized (info) {
			if (instance != null 
					&& (info.lastDiscoveryTime < System.currentTimeMillis() - DISCOVERY_TIMEOUT || info.knownHosts.isEmpty())) {
				Admin serverAdmin = instance.getService(Admin.class);
				try {
					Collection<ProcessObject> processes = serverAdmin.getProcesses("*");
					info.knownHosts.clear();
					for (ProcessObject processObject : processes) {
						if (!processObject.isEnabled() || !processObject.isRunning()) {
							continue;
						}
						info.knownHosts.add(new HostInfo(useUrlHost?instance.getHostInfo().getHostName():processObject.getInetAddress().getHostName(), processObject.getPort()));
					}
					info.lastDiscoveryTime = System.currentTimeMillis();
				} catch (AdminException e) {
					//ignore - will get an update on the next successful connection
				}
			}
			if (info.knownHosts.size() == 0) {
				return super.getKnownHosts(result, instance);
			}
			return new ArrayList<HostInfo>(info.knownHosts);
		}
	}
}
