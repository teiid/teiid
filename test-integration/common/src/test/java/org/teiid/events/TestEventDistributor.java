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
package org.teiid.events;

import org.junit.Test;
import org.mockito.Mockito;
import org.teiid.adminapi.VDB;
import org.teiid.core.util.UnitTestUtil;
import org.teiid.jdbc.FakeServer;

@SuppressWarnings("nls")
public class TestEventDistributor {
	private static final String VDB = "PartsSupplier"; //$NON-NLS-1$
	
	@Test
	public void testEvents() throws Exception {
		FakeServer server = null;
		try {
			server = new FakeServer(true);
			EventListener events = Mockito.mock(EventListener.class);
			server.getEventDistributor().register(events);
			
	    	server.deployVDB(VDB, UnitTestUtil.getTestDataPath() + "/PartsSupplier.vdb");
	    	
	    	Mockito.verify(events).vdbDeployed(VDB, 1);
	    	Mockito.verify(events).vdbLoaded((VDB)Mockito.any());
	    	
	    	server.undeployVDB(VDB);

	    	Mockito.verify(events).vdbDeployed(VDB, 1);
	    	Mockito.verify(events).vdbLoaded((VDB)Mockito.any());
	    	Mockito.verify(events).vdbUndeployed(VDB, 1);
		} finally { 
			if (server != null) {
				server.stop();
			}
		}
	}

}
