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

package org.teiid.translator.file;

import static org.junit.Assert.*;

import java.io.File;
import java.util.Arrays;

import org.junit.Test;
import org.mockito.Mockito;
import org.teiid.core.util.UnitTestUtil;
import org.teiid.language.Argument;
import org.teiid.language.Call;
import org.teiid.language.Literal;
import org.teiid.language.Argument.Direction;
import org.teiid.translator.FileConnection;
import org.teiid.translator.ProcedureExecution;
import org.teiid.translator.TypeFacility;

@SuppressWarnings("nls")
public class TestFileExecutionFactory {

	@Test public void testGetTextFiles() throws Exception {
		FileExecutionFactory fef = new FileExecutionFactory();
		FileConnection fc = Mockito.mock(FileConnection.class);
		Mockito.stub(fc.getFile("*.txt")).toReturn(new File(UnitTestUtil.getTestDataPath(), "*.txt"));
		Call call = fef.getLanguageFactory().createCall("getTextFiles", Arrays.asList(new Argument(Direction.IN, new Literal("*.txt", TypeFacility.RUNTIME_TYPES.STRING), TypeFacility.RUNTIME_TYPES.STRING, null)), null);
		ProcedureExecution pe = fef.createProcedureExecution(call, null, null, fc);
		pe.execute();
		int count = 0;
		while (true) {
			if (pe.next() == null) {
				break;
			}
			count++;
		}
		assertEquals(2, count);
	}
	
}
