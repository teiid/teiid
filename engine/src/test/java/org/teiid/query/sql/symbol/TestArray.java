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

package org.teiid.query.sql.symbol;

import static org.junit.Assert.*;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import org.junit.Test;
import org.teiid.client.BatchSerializer;
import org.teiid.common.buffer.TupleBuffer;
import org.teiid.core.types.DataTypeManager;
import org.teiid.core.util.UnitTestUtil;
import org.teiid.query.sql.visitor.SQLStringVisitor;

@SuppressWarnings("nls")
public class TestArray {

	@Test public void testArrayValueCompare() {
		ArrayValue a1 = new ArrayValue(new Object[] {1, 2, 3});
		
		UnitTestUtil.helpTestEquivalence(0, a1, a1);
		
		ArrayValue a2 = new ArrayValue(new Object[] {1, 2});
		
		UnitTestUtil.helpTestEquivalence(1, a1, a2);
	}
	
	@Test public void testArrayValueToString() {
		ArrayValue a1 = new ArrayValue(new Object[] {1, "x'2", 3});
		
		assertEquals("(1, 'x''2', 3)", SQLStringVisitor.getSQLString(new Constant(a1)));
	}
	
	@Test public void testArrayClone() {
		Array array = new Array(DataTypeManager.DefaultDataClasses.OBJECT, Arrays.asList((Expression)new ElementSymbol("e1")));
		
		Array a1 = array.clone();
		
		assertNotSame(a1, array);
		assertNotSame(a1.getExpressions().get(0), array.getExpressions().get(0));
	}
	
	@Test public void testArrayValueSerialization() throws Exception {
		ArrayValue a1 = new ArrayValue(new Integer[] {1, 2, 3});
		String[] types = TupleBuffer.getTypeNames(Arrays.asList(new Array(Integer.class, null)));
		ByteArrayOutputStream baos = new ByteArrayOutputStream();
		ObjectOutputStream oos = new ObjectOutputStream(baos);
		BatchSerializer.writeBatch(oos, types, Collections.singletonList(Arrays.asList((a1))));
		oos.close();
		ByteArrayInputStream bais = new ByteArrayInputStream(baos.toByteArray());
		ObjectInputStream ois = new ObjectInputStream(bais);
		List<List<Object>> batch = BatchSerializer.readBatch(ois, types);
		assertEquals(a1, batch.get(0).get(0));
	}
	
}
