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

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.sql.SQLException;
import java.sql.Wrapper;

import org.teiid.jdbc.WrapperImpl;

import junit.framework.TestCase;

public class TestWrapperImpl extends TestCase {
	
	interface Foo extends Wrapper {
		void callMe();
	}
	
	static class FooImpl extends WrapperImpl implements Foo {
		
		boolean wasCalled;
		
		public void callMe() {
			wasCalled = true;
		}
		
	}
	
	public void testProxy() throws SQLException {
		
		final FooImpl fooImpl = new FooImpl(); 
		
		Foo proxy = (Foo)Proxy.newProxyInstance(TestWrapperImpl.class.getClassLoader(), new Class[] {Foo.class}, new InvocationHandler() {

			public Object invoke(Object arg0, Method arg1, Object[] arg2)
					throws Throwable {
				if (arg1.getName().equals("callMe")) {
					return null;
				}
				try {
					return arg1.invoke(fooImpl, arg2);
				} catch (InvocationTargetException e) {
					throw e.getTargetException();
				}
			}
			
		});
		
		proxy.callMe();
		
		assertFalse(fooImpl.wasCalled);
		
		proxy.unwrap(Foo.class).callMe();
		
		assertTrue(fooImpl.wasCalled);
		
		try {
			proxy.unwrap(String.class);
			fail("expected exception");
		} catch (SQLException e) {
			assertEquals("Wrapped object is not an instance of class java.lang.String", e.getMessage());
		}
	}

}
