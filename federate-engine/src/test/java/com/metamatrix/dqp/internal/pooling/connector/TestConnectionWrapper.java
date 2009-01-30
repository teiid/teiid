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

package com.metamatrix.dqp.internal.pooling.connector;

import junit.framework.TestCase;

import org.mockito.Mockito;

import com.metamatrix.data.api.Connection;
import com.metamatrix.data.pool.PoolAwareConnection;

public class TestConnectionWrapper extends TestCase {

	public void testIdleTime() throws Exception {
		ConnectionWrapper wrapper = new ConnectionWrapper(Mockito.mock(Connection.class), Mockito.mock(ConnectionPool.class), 1);
		long time = wrapper.getTimeReturnedToPool();
		Thread.sleep(5);
		wrapper.release();
		assertTrue(wrapper.getTimeReturnedToPool() - time > 0);
	}
	
	public void testIsAliveTestInterval() throws Exception {
		PoolAwareConnection connection = Mockito.mock(PoolAwareConnection.class);
		Mockito.stub(connection.isAlive()).toReturn(Boolean.TRUE);
		ConnectionWrapper wrapper = new ConnectionWrapper(connection, Mockito.mock(ConnectionPool.class), 1);
		wrapper.setTestInterval(-1); //trigger an actual call.
		for (int i = 0; i < 10; i++) {
			wrapper.isAlive();
			wrapper.setTestInterval(60000);
		}
		Mockito.verify(connection, Mockito.times(1)).isAlive();
	}
}
