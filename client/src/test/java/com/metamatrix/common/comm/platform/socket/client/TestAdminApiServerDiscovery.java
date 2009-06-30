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

import java.net.InetAddress;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import junit.framework.TestCase;

import org.mockito.Mockito;
import org.teiid.adminapi.Admin;
import org.teiid.adminapi.ProcessObject;

import com.metamatrix.common.api.HostInfo;
import com.metamatrix.common.api.MMURL;
import com.metamatrix.platform.security.api.LogonResult;

public class TestAdminApiServerDiscovery extends TestCase {

	public void testFirewallHost() throws Exception {
		AdminApiServerDiscovery discovery = new AdminApiServerDiscovery();
		Properties p = new Properties();
		p.setProperty(AdminApiServerDiscovery.USE_URL_HOST, Boolean.TRUE.toString());
		MMURL mmurl = new MMURL("foo", 1, false); //$NON-NLS-1$
		discovery.init(mmurl, p);
		HostInfo knownHost = mmurl.getHostInfo().get(0);
		//we will start off using the url host
		assertEquals(1, discovery.getKnownHosts(null, null).size()); 
		
		SocketServerInstance instance = Mockito.mock(SocketServerInstance.class);
		Admin serverAdmin = Mockito.mock(Admin.class);
		
		List<ProcessObject> processes = new ArrayList<ProcessObject>();
		ProcessObject p1 = Mockito.mock(ProcessObject.class);
		Mockito.stub(p1.getPort()).toReturn(5);
		processes.add(p1);
		ProcessObject p2 = Mockito.mock(ProcessObject.class);
		Mockito.stub(p2.isEnabled()).toReturn(true);
		Mockito.stub(p2.isRunning()).toReturn(true);
		Mockito.stub(p2.getPort()).toReturn(6);
		Mockito.stub(p2.getInetAddress()).toReturn(InetAddress.getByName("0.0.0.0")); //$NON-NLS-1$
		processes.add(p2);
		Mockito.stub(serverAdmin.getProcesses("*")).toReturn(processes); //$NON-NLS-1$
		Mockito.stub(instance.getService(Admin.class)).toReturn(serverAdmin);
		Mockito.stub(instance.getHostInfo()).toReturn(knownHost);
		
		discovery.connectionSuccessful(knownHost);
		List<HostInfo> knownHosts = discovery.getKnownHosts(new LogonResult(), instance);
		
		assertEquals(1, knownHosts.size());
		HostInfo h = knownHosts.get(0);
		//the returned host should have the url name, but the process port
		assertEquals("foo", h.getHostName()); //$NON-NLS-1$
		assertEquals(6, h.getPortNumber());
	}
	
}
