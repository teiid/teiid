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

package org.teiid.resource.adapter.ws;

import javax.resource.ResourceException;
import javax.xml.transform.stream.StreamSource;
import javax.xml.ws.WebServiceException;
import javax.xml.ws.Service.Mode;
import javax.xml.ws.http.HTTPBinding;

import org.junit.Test;

public class TestWSAdapter {
	
	@Test(expected=WebServiceException.class) public void testMissingBinding() throws ResourceException {
		WSManagedConnectionFactory wsmcf = new WSManagedConnectionFactory();
		
		WSConnectionImpl conn = (WSConnectionImpl)wsmcf.createConnectionFactory().getConnection();
		conn.createDispatch(HTTPBinding.HTTP_BINDING, null, StreamSource.class, Mode.PAYLOAD);
	}

}
