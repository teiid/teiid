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
import org.teiid.jdbc.CancellationTimer.CancelTask;

@SuppressWarnings("nls")
public class TestCancellationTimer {
	
	private final class SimpleCancelTask extends CancelTask {
		private SimpleCancelTask(long delay) {
			super(delay);
		}

		@Override
		public void run() {
		}
	}

	@Test public void testRemove() {
		CancellationTimer ct = new CancellationTimer("foo");
		SimpleCancelTask sct = new SimpleCancelTask(20000); 
		ct.add(sct);
		SimpleCancelTask sct1 = new SimpleCancelTask(30000); 
		ct.add(sct1);
		SimpleCancelTask sct2 = new SimpleCancelTask(10000); 
		ct.add(sct2);
		assertEquals(3, ct.getQueueSize());
		ct.remove(sct2);
		ct.remove(sct1);
		ct.remove(sct);
		assertEquals(0, ct.getQueueSize());
	}
	
}
