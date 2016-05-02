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

package org.teiid.translator.jdbc.derby;

import static org.junit.Assert.*;

import org.junit.Test;

public class TestDerbyCapabilities {
	
	@Test public void testLimitSupport() {
		DerbyExecutionFactory derbyCapabilities = new DerbyExecutionFactory();
		assertFalse(derbyCapabilities.supportsRowLimit());
		derbyCapabilities.setDatabaseVersion(DerbyExecutionFactory.TEN_5.toString());
		assertTrue(derbyCapabilities.supportsRowLimit());
	}
	
	@Test public void testFunctionSupport() {
		DerbyExecutionFactory derbyCapabilities = new DerbyExecutionFactory();
		assertEquals(27, derbyCapabilities.getSupportedFunctions().size());
		derbyCapabilities.setDatabaseVersion(DerbyExecutionFactory.TEN_4.toString());
		assertEquals(44, derbyCapabilities.getSupportedFunctions().size());
	}

}
