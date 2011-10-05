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

package org.teiid.jdbc;

import static org.junit.Assert.*;

import org.junit.Test;
import org.teiid.jdbc.EnhancedTimer.Task;

@SuppressWarnings("nls")
public class TestEnhancedTimer {
	
	private final class SimpleCancelTask implements Runnable {
		@Override
		public void run() {
		}
	}

	@Test public void testRemove() {
		EnhancedTimer ct = new EnhancedTimer("foo");
		SimpleCancelTask sct = new SimpleCancelTask();
		Task tt = ct.add(sct, 20000);
		Task tt1 = ct.add(sct, 20000);
		assertTrue(tt.compareTo(tt1) < 0);
		Task tt2 = ct.add(sct, 10000);
		assertEquals(3, ct.getQueueSize());
		tt.cancel();
		tt1.cancel();
		tt2.cancel();
		assertEquals(0, ct.getQueueSize());
	}
	
}
