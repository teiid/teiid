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

import java.lang.reflect.Proxy;
import java.util.Properties;

import junit.framework.TestCase;

import com.metamatrix.platform.service.api.ServiceInterface;
import com.metamatrix.platform.service.api.ServiceState;
import com.metamatrix.platform.service.api.exception.ServiceClosedException;
import com.metamatrix.platform.service.controller.AbstractService;

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
		
    	FakeServiceInterface fakeServiceInterface = (FakeServiceInterface) Proxy
				.newProxyInstance(Thread.currentThread()
						.getContextClassLoader(),
						new Class[] { FakeServiceInterface.class },
						new ServiceRegistryBinding.StateAwareProxy(service));
		
		assertEquals(1, fakeServiceInterface.doSomething(1));

		service.die();
		
		//ensure that check state is not called through the proxy
		fakeServiceInterface.die();
		
		try {
			fakeServiceInterface.doSomething(1);
			fail("expected exception"); //$NON-NLS-1$
		} catch (ServiceClosedException e) {
			//expected
		}
	}
	
}
