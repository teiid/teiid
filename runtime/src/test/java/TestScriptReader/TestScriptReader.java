/*
 * JBoss, Home of Professional Open Source.
 * Copyright (C) 2008 Red Hat, Inc.
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

package TestScriptReader;

import static org.junit.Assert.*;

import org.junit.Test;
import org.teiid.odbc.ScriptReader;

@SuppressWarnings("nls")
public class TestScriptReader {

	@Test public void testRewrite() throws Exception {
		ScriptReader sr = new ScriptReader("select 'a'::b from foo");
		sr.setRewrite(true);
		String result = sr.readStatement();
		assertEquals("select cast('a' AS b) from foo", result);
	}
	
	@Test public void testRewriteComplexLiteral() throws Exception {
		ScriptReader sr = new ScriptReader("select 'a''c'::b");
		sr.setRewrite(true);
		String result = sr.readStatement();
		assertEquals("select cast('a''c' AS b)", result);
	}
	
	@Test public void testRewrite1() throws Exception {
		ScriptReader sr = new ScriptReader("select a~b, a!~~c from foo");
		sr.setRewrite(true);
		String result = sr.readStatement();
		assertEquals("select a LIKE_REGEX b, a NOT LIKE c from foo", result);
	}
	
	@Test public void testRewrite2() throws Exception {
		ScriptReader sr = new ScriptReader("select a~");
		sr.setRewrite(true);
		String result = sr.readStatement();
		assertEquals("select a LIKE_REGEX ", result);
	}
	
	@Test public void testRewrite3() throws Exception {
		ScriptReader sr = new ScriptReader("select a::b");
		sr.setRewrite(true);
		String result = sr.readStatement();
		assertEquals("select a", result);
	}
	
	@Test public void testDelimited() throws Exception {
		ScriptReader sr = new ScriptReader("set foo 'bar'; set foo1 'bar1'");
		String result = sr.readStatement();
		assertEquals("set foo 'bar'", result);
		result = sr.readStatement();
		assertEquals(" set foo1 'bar1'", result);
	}
	
	@Test public void testRegClassCast() throws Exception {
		ScriptReader sr = new ScriptReader("where oid='\"a\"'::regclass");
		sr.setRewrite(true);;
		String result = sr.readStatement();
		assertEquals("where oid=(SELECT oid FROM pg_class WHERE upper(relname) = 'A')", result);
	}
	
	@Test public void testExtraDelim() throws Exception {
		ScriptReader sr = new ScriptReader("BEGIN;declare \"SQL_CUR0x1e4ba50\" cursor with hold for select * from pg_proc;;fetch 1 in \"SQL_CUR0x1e4ba50\"");
		String result = sr.readStatement();
		assertEquals("BEGIN", result);
		result = sr.readStatement();
		assertEquals("declare \"SQL_CUR0x1e4ba50\" cursor with hold for select * from pg_proc", result);
		result = sr.readStatement();
		assertEquals("fetch 1 in \"SQL_CUR0x1e4ba50\"", result);
		assertNull(sr.readStatement());
	}
	
}
