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
import java.sql.Timestamp;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;

import org.junit.Test;
import org.mockito.Mockito;
import org.teiid.core.util.UnitTestUtil;
import org.teiid.language.Argument;
import org.teiid.language.Argument.Direction;
import org.teiid.language.Call;
import org.teiid.language.Literal;
import org.teiid.metadata.MetadataFactory;
import org.teiid.metadata.Procedure;
import org.teiid.query.metadata.SystemMetadata;
import org.teiid.translator.FileConnection;
import org.teiid.translator.ProcedureExecution;
import org.teiid.translator.TypeFacility;

@SuppressWarnings("nls")
public class TestFileExecutionFactory {

	@Test public void testGetTextFiles() throws Exception {
		FileExecutionFactory fef = new FileExecutionFactory();
		MetadataFactory mf = new MetadataFactory("vdb", 1, "text", SystemMetadata.getInstance().getRuntimeTypeMap(), new Properties(), null);
		fef.getMetadata(mf, null);
		Procedure p = mf.getSchema().getProcedure("getTextFiles");
		FileConnection fc = Mockito.mock(FileConnection.class);
		Mockito.stub(fc.getFile("*.txt")).toReturn(new File(UnitTestUtil.getTestDataPath(), "*.txt"));
		Call call = fef.getLanguageFactory().createCall("getTextFiles", Arrays.asList(new Argument(Direction.IN, new Literal("*.txt", TypeFacility.RUNTIME_TYPES.STRING), TypeFacility.RUNTIME_TYPES.STRING, null)), p);
		ProcedureExecution pe = fef.createProcedureExecution(call, null, null, fc);
		pe.execute();
		int count = 0;
		while (true) {
		    List<?> val = pe.next();
			if (val == null) {
				break;
			}
			assertEquals(5, val.size());
			assertTrue(val.get(3) instanceof Timestamp);
			assertEquals(Long.valueOf(0), val.get(4));
			count++;
		}
		assertEquals(2, count);
		
		
		call = fef.getLanguageFactory().createCall("getTextFiles", Arrays.asList(new Argument(Direction.IN, new Literal("*1*", TypeFacility.RUNTIME_TYPES.STRING), TypeFacility.RUNTIME_TYPES.STRING, null)), p);
		pe = fef.createProcedureExecution(call, null, null, fc);
		Mockito.stub(fc.getFile("*1*")).toReturn(new File(UnitTestUtil.getTestDataPath(), "*1*"));
		pe.execute();
		count = 0;
		while (true) {
			if (pe.next() == null) {
				break;
			}
			count++;
		}
		assertEquals(1, count);
	}
	
}
