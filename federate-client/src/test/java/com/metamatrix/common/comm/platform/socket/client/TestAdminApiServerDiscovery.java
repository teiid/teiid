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

package com.metamatrix.common.comm.platform.socket.client;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import junit.framework.TestCase;

import org.mockito.Mockito;

import com.metamatrix.admin.api.objects.ProcessObject;
import com.metamatrix.admin.api.server.ServerAdmin;
import com.metamatrix.common.api.HostInfo;
import com.metamatrix.common.api.MMURL;

public class TestAdminApiServerDiscovery extends TestCase {

	public void testFirewallHost() throws Exception {
		AdminApiServerDiscovery discovery = new AdminApiServerDiscovery();
		Properties p = new Properties();
		p.setProperty(AdminApiServerDiscovery.USE_URL_HOST, Boolean.TRUE.toString());
		MMURL mmurl = new MMURL("foo", 1, false);
		discovery.init(mmurl, p);
		
		//we will start off using the url host
		assertEquals(1, discovery.getKnownHosts().size()); 
		
		SocketServerInstance instance = Mockito.mock(SocketServerInstance.class);
		ServerAdmin serverAdmin = Mockito.mock(ServerAdmin.class);
		
		List<ProcessObject> processes = new ArrayList<ProcessObject>();
		ProcessObject p1 = Mockito.mock(ProcessObject.class);
		Mockito.stub(p1.isEnabled()).toReturn(false);
		Mockito.stub(p1.getPort()).toReturn(5);
		processes.add(p1);
		ProcessObject p2 = Mockito.mock(ProcessObject.class);
		Mockito.stub(p2.isEnabled()).toReturn(true);
		Mockito.stub(p2.getPort()).toReturn(6);
		processes.add(p2);
		
		Mockito.stub(serverAdmin.getProcesses("*")).toReturn(processes);
		Mockito.stub(instance.getService(ServerAdmin.class)).toReturn(serverAdmin);
		discovery.connectionSuccessful(discovery.getKnownHosts().get(0), instance);
		
		List<HostInfo> knownHosts = discovery.getKnownHosts();
		assertEquals(1, knownHosts.size());
		HostInfo h = knownHosts.get(0);
		assertEquals("foo", h.getHostName());
		assertEquals(6, h.getPortNumber());
	}
	
}
