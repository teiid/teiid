/*
 * Copyright Red Hat, Inc. and/or its affiliates
 * and other contributors as indicated by the @author tags and
 * the COPYRIGHT.txt file distributed with this work.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.teiid.query.function;

import java.io.File;
import java.io.FileInputStream;
import java.util.List;

import junit.framework.TestCase;

import org.teiid.core.util.UnitTestUtil;
import org.teiid.metadata.FunctionMethod;
import org.teiid.metadata.FunctionParameter;
import org.teiid.metadata.FunctionMethod.PushDown;
import org.teiid.metadata.FunctionMethod.Determinism;
import org.teiid.query.function.metadata.FunctionMetadataReader;

@SuppressWarnings("nls")
public class TestFunctionMetadataReader extends TestCase {

	public void testLoadFunctionMethods() throws Exception {
		
		List<FunctionMethod>  fns = FunctionMetadataReader.loadFunctionMethods(new FileInputStream(UnitTestUtil.getTestDataPath()+File.separator+"udf.xmi"));
		
		assertEquals(3, fns.size());
		
		for (FunctionMethod m:fns) {
			if (m.getName().equals("GetSystemProperty")) {
				assertEquals("MyFunctions", m.getCategory());
				assertEquals("com.metamatrix.dqp.embedded.udf.MyFunctions", m.getInvocationClass());
				assertEquals("getProperty", m.getInvocationMethod());
				assertEquals(PushDown.CAN_PUSHDOWN, m.getPushdown());
				assertEquals(Determinism.DETERMINISTIC, m.getDeterminism());
				assertNull(m.getDescription());
				
				assertEquals(1, m.getInputParameterCount());
				FunctionParameter in = m.getInputParameters().get(0);
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
				assertEquals(PushDown.CANNOT_PUSHDOWN, m.getPushdown());
				assertEquals(Determinism.DETERMINISTIC, m.getDeterminism());
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
				assertEquals(PushDown.CAN_PUSHDOWN, m.getPushdown());
				assertEquals(Determinism.NONDETERMINISTIC, m.getDeterminism());
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
