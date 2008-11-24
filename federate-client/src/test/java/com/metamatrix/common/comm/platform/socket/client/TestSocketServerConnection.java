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

/**
 * 
 */
package com.metamatrix.common.comm.platform.socket.client;

import java.util.Properties;

import junit.framework.TestCase;

import com.metamatrix.common.api.MMURL_Properties;
import com.metamatrix.common.util.MetaMatrixProductNames;

/**
 * <code>TestCase</case> for <code>SocketServerConnection</code>
 * @see SocketServerConnection
 * @since Westport
 */
public class TestSocketServerConnection extends TestCase {
	

	/**
	 * Validate that the client host name and IP address property in 
	 * the connection properties object is set after a <code>SocketServerConnection</code> 
	 * is established. 
	 * 
	 * <p>The expected results contains the host name and IP address 
	 * of the local machine as returned by <code>NetUtils</code>. 
	 * These values are not put into the initial connection object 
	 * and it is up to <code>SocketServerConnection</code> to place 
	 * the values into the connection properties object during the 
	 * connection process.</p>
	 * @throws Throwable 
	 *  
	 * @since Westport    
	 */
	public void testSocketServerConnection_PropertiesClientHost() throws Throwable {
		Properties p = new Properties();
		
		p.setProperty(MMURL_Properties.CONNECTION.PRODUCT_NAME, MetaMatrixProductNames.Platform.PRODUCT_NAME);

		SocketServerConnectionFactory.updateConnectionProperties(p);
       
		assertTrue(p.containsKey(MMURL_Properties.CONNECTION.CLIENT_HOSTNAME));
		assertTrue(p.containsKey(MMURL_Properties.CONNECTION.CLIENT_IP_ADDRESS));
		assertEquals(Boolean.TRUE.toString(), p.getProperty(MMURL_Properties.CONNECTION.AUTO_FAILOVER));
	}

}
