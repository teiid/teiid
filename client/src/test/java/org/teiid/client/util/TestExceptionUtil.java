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

package org.teiid.client.util;

import static org.junit.Assert.*;

import org.junit.Test;
import org.teiid.core.TeiidException;
import org.teiid.jdbc.JDBCPlugin;

@SuppressWarnings("nls")
public class TestExceptionUtil {

	@Test public void testSanitize() {
		TeiidException te = new TeiidException(JDBCPlugin.Event.TEIID20000, "you don't want to see this");
		te.initCause(new Exception("or this"));
		
		Throwable t = ExceptionUtil.sanitize(te, true);
		assertTrue(t.getStackTrace().length != 0);
		assertNotNull(t.getCause());
		assertEquals("TEIID20000", t.getMessage());
		assertEquals("java.lang.Exception", t.getCause().getMessage());
		
		t = ExceptionUtil.sanitize(te, false);
		assertEquals(0, t.getStackTrace().length);
		assertEquals("TEIID20000", t.getMessage());
	}
	
}
