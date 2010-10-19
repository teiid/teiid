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

package org.teiid.net.socket;

import java.lang.reflect.Method;

import junit.framework.TestCase;

import org.teiid.adminapi.Admin;
import org.teiid.adminapi.AdminException;
import org.teiid.client.DQP;
import org.teiid.client.util.ExceptionUtil;
import org.teiid.client.xa.XATransactionException;
import org.teiid.core.TeiidComponentException;
import org.teiid.core.TeiidRuntimeException;

@SuppressWarnings("nls")
public class TestSocketServiceRegistry extends TestCase {

	interface Foo{
		void somemethod();
	}
	
	public void testExceptionConversionNoException() throws Exception {
				
		Method m = Foo.class.getMethod("somemethod", new Class[] {});
		
		Throwable t = ExceptionUtil.convertException(m, new TeiidComponentException());
		
		assertTrue(t instanceof TeiidRuntimeException);
	}
	
	public void testAdminExceptionConversion() throws Exception {
		
		Method m = Admin.class.getMethod("getCacheStats", new Class[] {String.class});
		
		Throwable t = ExceptionUtil.convertException(m, new TeiidComponentException());
		
		assertTrue(t instanceof AdminException);
	}
	
	public void testComponentExceptionConversion() throws Exception {
		
		Method m = DQP.class.getMethod("getMetadata", new Class[] {Long.TYPE});
		
		Throwable t = ExceptionUtil.convertException(m, new NullPointerException());
		
		assertTrue(t instanceof TeiidComponentException);
	}
	
	public void testXATransactionExceptionConversion() throws Exception {
		
		Method m = DQP.class.getMethod("recover", new Class[] {Integer.TYPE});
		
		Throwable t = ExceptionUtil.convertException(m, new TeiidComponentException());
		
		assertTrue(t instanceof XATransactionException);
	}
	
}
