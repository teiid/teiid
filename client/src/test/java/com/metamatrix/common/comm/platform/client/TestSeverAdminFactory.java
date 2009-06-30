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

package com.metamatrix.common.comm.platform.client;

import static org.mockito.Matchers.anyObject;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.stub;
import static org.mockito.Mockito.stubVoid;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import java.util.Properties;

import org.junit.Test;
import org.teiid.adminapi.Admin;
import org.teiid.adminapi.AdminComponentException;

import com.metamatrix.common.comm.api.ServerConnection;
import com.metamatrix.common.comm.api.ServerConnectionFactory;
import com.metamatrix.common.comm.exception.SingleInstanceCommunicationException;

public class TestSeverAdminFactory {
	
	@Test public void testBounce() throws Exception {
		ServerConnectionFactory scf = mock(ServerConnectionFactory.class);
		ServerConnection sc = mock(ServerConnection.class);
		Admin sa = mock(Admin.class);
		stubVoid(sa).toThrow(new AdminComponentException(new SingleInstanceCommunicationException())).on().restart();
		stub(sc.getService(Admin.class)).toReturn(sa);
		stub(scf.createConnection((Properties)anyObject())).toReturn(sc);
		
		ServerAdminFactory saf = new ServerAdminFactory(scf, 1);
		Admin admin = saf.createAdmin("foo", "bar".toCharArray(), "mm://test:1"); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
		admin.restart();
		
		//verify that the actual bounce was called
		verify(sa, times(1)).restart();
		
		//here's the test we issue to see that the system is up after the bounce
		verify(sa, times(1)).getSystem(); 
	}

}
