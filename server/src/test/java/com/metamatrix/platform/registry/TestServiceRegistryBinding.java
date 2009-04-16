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

package com.metamatrix.platform.registry;

import java.util.Date;
import java.util.Properties;

import junit.framework.TestCase;

import com.metamatrix.admin.server.FakeConfiguration;
import com.metamatrix.common.config.api.DeployedComponent;
import com.metamatrix.common.messaging.NoOpMessageBus;
import com.metamatrix.platform.service.api.ServiceID;
import com.metamatrix.platform.service.api.ServiceInterface;
import com.metamatrix.platform.service.api.ServiceState;
import com.metamatrix.platform.service.api.exception.ServiceClosedException;
import com.metamatrix.platform.service.controller.AbstractService;
import com.metamatrix.server.query.service.QueryService;

public class TestServiceRegistryBinding extends TestCase {

	private interface FakeServiceInterface extends ServiceInterface {
		public int doSomething(int arg);
	}
	
	private static class FakeServiceImpl extends AbstractService implements FakeServiceInterface {

		public FakeServiceImpl() {
			this.updateState(ServiceState.STATE_OPEN);
		}
		
		@Override
		protected void closeService() throws Exception {
		}

		@Override
		protected void initService(Properties props) throws Exception {
		}

		@Override
		protected void killService() {
		}

		@Override
		protected void waitForServiceToClear() throws Exception {
		}

		public int doSomething(int arg) {
			return arg;
		}
		
	}
	
	public void testStateCheckingProxy() throws Exception {
		FakeServiceImpl service = new FakeServiceImpl();
		
    	ProcessRegistryBinding vmBinding2  = FakeRegistryUtil.buildVMRegistryBinding("2.2.2.2", "process2");             //$NON-NLS-1$ //$NON-NLS-2$ 
    	ServiceID sid1 = new ServiceID(5, vmBinding2.getHostName(), vmBinding2.getProcessName());
    	ServiceRegistryBinding binding = new ServiceRegistryBinding(sid1, service, QueryService.SERVICE_NAME,
                                                                    "dqp2", "QueryService", //$NON-NLS-1$ //$NON-NLS-2$
                                                                    "dqp2", "2.2.2.2",(DeployedComponent)new FakeConfiguration().deployedComponents.get(4),  //$NON-NLS-1$ //$NON-NLS-2$ 
                                                                    ServiceState.STATE_CLOSED,
                                                                    new Date(),  
                                                                    false, new NoOpMessageBus()); 
		
		assertEquals(1, ((FakeServiceInterface)binding.getService()).doSomething(1));

		service.die();
		
		//ensure that check state is not called through the proxy
		((FakeServiceInterface)binding.getService()).die();
		
		try {
			((FakeServiceInterface)binding.getService()).doSomething(1);
			fail("expected exception"); //$NON-NLS-1$
		} catch (ServiceClosedException e) {
			//expected
		}
	}
	
}
