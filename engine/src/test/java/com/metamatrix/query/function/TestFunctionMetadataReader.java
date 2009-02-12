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

package com.metamatrix.query.function;

import java.io.File;
import java.io.FileInputStream;
import java.util.List;

import com.metamatrix.core.util.UnitTestUtil;
import com.metamatrix.query.function.metadata.FunctionMetadataReader;
import com.metamatrix.query.function.metadata.FunctionMethod;
import com.metamatrix.query.function.metadata.FunctionParameter;

import junit.framework.TestCase;

public class TestFunctionMetadataReader extends TestCase {

	public void testLoadFunctionMethods() throws Exception {
		
		List<FunctionMethod>  fns = FunctionMetadataReader.loadFunctionMethods(new FileInputStream(UnitTestUtil.getTestDataPath()+File.separator+"udf.xmi"));
		
		assertEquals(3, fns.size());
		
		for (FunctionMethod m:fns) {
			if (m.getName().equals("GetSystemProperty")) {
				assertEquals("MyFunctions", m.getCategory());
				assertEquals("com.metamatrix.dqp.embedded.udf.MyFunctions", m.getInvocationClass());
				assertEquals("getProperty", m.getInvocationMethod());
				assertEquals(FunctionMethod.CAN_PUSHDOWN, m.getPushdown());
				assertEquals(FunctionMethod.DETERMINISTIC, m.getDeterministic());
				assertNull(m.getDescription());
				
				assertEquals(1, m.getInputParameterCount());
				FunctionParameter in = m.getInputParameters()[0];
				assertEquals("prop", in.getName());
				assertEquals("string", in.getType());
				assertNull(in.getDescription());
				
				assertNotNull(m.getOutputParameter());
				assertEquals(FunctionParameter.OUTPUT_PARAMETER_NAME, m.getOutputParameter().getName());
				assertEquals("string", m.getOutputParameter().getType());
				
			}
			else if (m.getName().equals("getpushdown")) {
				assertEquals("MyFunctions", m.getCategory());
				assertEquals("com.metamatrix.dqp.embedded.udf.MyFunctions", m.getInvocationClass());
				assertEquals("getPropertyNoArgs", m.getInvocationMethod());
				assertEquals(FunctionMethod.CANNOT_PUSHDOWN, m.getPushdown());
				assertEquals(FunctionMethod.DETERMINISTIC, m.getDeterministic());
				assertNull(m.getDescription());
				
				assertEquals(0, m.getInputParameterCount());
				
				assertNotNull(m.getOutputParameter());
				assertEquals(FunctionParameter.OUTPUT_PARAMETER_NAME, m.getOutputParameter().getName());
				assertEquals("string", m.getOutputParameter().getType());				
			}
			else if (m.getName().equals("getxyz")) {
				assertEquals("MyFunctions", m.getCategory());
				assertEquals("com.metamatrix.dqp.embedded.udf.MyFunctions", m.getInvocationClass());
				assertEquals("getPropertyNoArgs", m.getInvocationMethod());
				assertEquals(FunctionMethod.CAN_PUSHDOWN, m.getPushdown());
				assertEquals(FunctionMethod.NONDETERMINISTIC, m.getDeterministic());
				assertNull(m.getDescription());
				
				assertEquals(0, m.getInputParameterCount());
				
				assertNotNull(m.getOutputParameter());
				assertEquals(FunctionParameter.OUTPUT_PARAMETER_NAME, m.getOutputParameter().getName());
				assertEquals("string", m.getOutputParameter().getType());
			}
			else {
				fail("unknown method");
			}
		}
		
	}

}
