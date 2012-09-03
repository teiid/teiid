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
import java.sql.SQLException;
import java.util.Arrays;
import java.util.List;

import org.junit.Test;
import org.teiid.client.BatchSerializer;
import org.teiid.common.buffer.TupleBuffer;
import org.teiid.core.types.ArrayImpl;
import org.teiid.core.types.DataTypeManager;
import org.teiid.core.util.UnitTestUtil;
import org.teiid.query.sql.visitor.SQLStringVisitor;

@SuppressWarnings("nls")
public class TestArray {

	@Test public void testArrayValueCompare() {
		ArrayImpl a1 = new ArrayImpl(new Object[] {1, 2, 3});
		
		UnitTestUtil.helpTestEquivalence(0, a1, a1);
		
		ArrayImpl a2 = new ArrayImpl(new Object[] {1, 2});
		
		UnitTestUtil.helpTestEquivalence(1, a1, a2);
	}
	
	@Test public void testArrayValueToString() {
		ArrayImpl a1 = new ArrayImpl(new Object[] {1, "x'2", 3});
		
		assertEquals("(1, 'x''2', 3)", SQLStringVisitor.getSQLString(new Constant(a1)));
	}
	
	@Test public void testArrayClone() {
		Array array = new Array(DataTypeManager.DefaultDataClasses.OBJECT, Arrays.asList((Expression)new ElementSymbol("e1")));
		
		Array a1 = array.clone();
		
		assertNotSame(a1, array);
		assertEquals(a1, array);
		assertNotSame(a1.getExpressions().get(0), array.getExpressions().get(0));
	}
	
	@SuppressWarnings("unchecked")
	@Test public void testArrayValueSerialization() throws Exception {
		ArrayImpl a1 = new ArrayImpl(new Integer[] {1, null, 3});
		ArrayImpl a2 = new ArrayImpl(null);
		String[] types = TupleBuffer.getTypeNames(Arrays.asList(new Array(Integer.class, null)));
		ByteArrayOutputStream baos = new ByteArrayOutputStream();
		ObjectOutputStream oos = new ObjectOutputStream(baos);
		BatchSerializer.writeBatch(oos, types, Arrays.asList(Arrays.asList(a1), Arrays.asList(a2)));
		oos.close();
		ByteArrayInputStream bais = new ByteArrayInputStream(baos.toByteArray());
		ObjectInputStream ois = new ObjectInputStream(bais);
		List<List<Object>> batch = BatchSerializer.readBatch(ois, types);
		assertEquals(a1, batch.get(0).get(0));
		try {
			((java.sql.Array)batch.get(1).get(0)).getArray();
			fail();
		} catch (SQLException e) {
			
		}
	}
	
	@Test public void testZeroBasedArray() throws Exception {
		ArrayImpl a1 = new ArrayImpl(new Integer[] {1, 2, 3});
		a1.setZeroBased(true);
		assertEquals(2, java.lang.reflect.Array.get(a1.getArray(1, 1), 0));
	}
	
	/**
	 * This is for compatibility with array_get
	 * @throws Exception
	 */
	@Test(expected=IndexOutOfBoundsException.class) public void testIndexOutOfBounds() throws Exception {
		ArrayImpl a1 = new ArrayImpl(new Integer[] {1, 2, 3});
		a1.getArray(-1, 1);
	}
}
