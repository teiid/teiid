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

package org.teiid.query.eval;

import static org.junit.Assert.*;

import java.lang.reflect.Method;
import java.util.Map;

import javax.script.CompiledScript;
import javax.script.ScriptException;
import javax.script.SimpleScriptContext;

import org.junit.Test;

@SuppressWarnings("nls")
public class TestTeiidScriptEngine {

	@Test public void testGetMethods() throws ScriptException {
		TeiidScriptEngine tse = new TeiidScriptEngine();
		Map<String, Method> map = tse.getMethodMap(Object.class);
		assertEquals(map, tse.getMethodMap(Object.class));
		assertEquals(4, map.size());
	}
	
	@Test public void testArraySyntax() throws Exception {
		TeiidScriptEngine tse = new TeiidScriptEngine();
		CompiledScript cs = tse.compile("root.1.2");
		SimpleScriptContext ssc = new SimpleScriptContext();
		ssc.setAttribute("root", new Object[] {new Object[] {"x", "y"}}, SimpleScriptContext.ENGINE_SCOPE);
		assertEquals("y", cs.eval(ssc));
	}
	
}
