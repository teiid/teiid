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

import java.util.Arrays;

import org.junit.Test;
import org.teiid.core.types.DataTypeManager;
import org.teiid.core.util.UnitTestUtil;

@SuppressWarnings("nls")
public class TestArray {

	@Test public void testArrayValueCompare() {
		ArrayValue a1 = new ArrayValue(new Object[] {1, 2, 3});
		
		UnitTestUtil.helpTestEquivalence(0, a1, a1);
		
		ArrayValue a2 = new ArrayValue(new Object[] {1, 2});
		
		UnitTestUtil.helpTestEquivalence(1, a1, a2);
	}
	
	@Test public void testArrayClone() {
		Array array = new Array(DataTypeManager.DefaultDataClasses.OBJECT, Arrays.asList((Expression)new ElementSymbol("e1")));
		
		Array a1 = array.clone();
		
		assertNotSame(a1, array);
		assertNotSame(a1.getExpressions().get(0), array.getExpressions().get(0));
	}
	
}
