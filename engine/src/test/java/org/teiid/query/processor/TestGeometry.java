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

package org.teiid.query.processor;

import static org.junit.Assert.*;

import java.sql.Blob;
import java.sql.Clob;

import org.junit.Test;
import org.teiid.core.types.BinaryType;
import org.teiid.core.types.ClobType;
import org.teiid.core.types.GeometryType;
import org.teiid.query.eval.Evaluator;
import org.teiid.query.resolver.TestFunctionResolving;
import org.teiid.query.sql.symbol.Expression;

@SuppressWarnings("nls")
public class TestGeometry {

	@Test public void testRoundTrip() throws Exception {
		Expression ex = TestFunctionResolving.getExpression("ST_GeomFromText('POLYGON ((40 0, 50 50, 0 50, 0 0, 40 0))')");
		GeometryType geom = (GeometryType) Evaluator.evaluate(ex);
		byte[] bytes = geom.getBytes(1, (int) geom.length());
		
		Expression ex1 = TestFunctionResolving.getExpression("ST_GeomFromBinary(X'"+new BinaryType(bytes)+"')");
		GeometryType geom1 = (GeometryType) Evaluator.evaluate(ex1);
		assertEquals(geom, geom1);
	}
	
	@Test public void testAsText() throws Exception {
		Expression ex2 = TestFunctionResolving.getExpression("st_astext(ST_GeomFromText('POLYGON ((40 0, 50 50, 0 50, 0 0, 40 0))'))");
		Clob val = (Clob) Evaluator.evaluate(ex2);
		assertEquals("POLYGON ((40 0, 50 50, 0 50, 0 0, 40 0))", ClobType.getString(val));
	}
	
	@Test public void testAsBinary() throws Exception {
		Expression ex = TestFunctionResolving.getExpression("st_asbinary(ST_GeomFromText('POLYGON ((40 0, 50 50, 0 50, 0 0, 40 0))'))");
		assertTrue(Evaluator.evaluate(ex) instanceof Blob);
	}
	
	@Test public void testContains() throws Exception {
		Expression ex = TestFunctionResolving.getExpression("st_contains(ST_GeomFromText('POLYGON ((40 0, 50 50, 0 50, 0 0, 40 0))'), ST_GeomFromText('POLYGON ((40 0, 50 50, 0 50, 0 0, 40 0))'))");
		Boolean b = (Boolean) Evaluator.evaluate(ex);
		assertTrue(b);
		
		ex = TestFunctionResolving.getExpression("st_contains(ST_GeomFromText('POLYGON ((40 0, 50 50, 0 50, 0 0, 40 0))'), ST_GeomFromText('POLYGON ((43 0, 50 50, 0 50, 0 0, 43 0))'))");
		b = (Boolean) Evaluator.evaluate(ex);
		assertFalse(b);
	}
	
	@Test public void testIntersects() throws Exception {
		Expression ex = TestFunctionResolving.getExpression("st_intersects(ST_GeomFromText('POLYGON ((40 0, 50 50, 0 50, 0 0, 40 0))'), ST_GeomFromText('POLYGON ((40 0, 50 50, 0 50, 0 0, 40 0))'))");
		Boolean b = (Boolean) Evaluator.evaluate(ex);
		assertTrue(b);
		
		ex = TestFunctionResolving.getExpression("st_intersects(ST_GeomFromText('POLYGON ((100 100, 200 200, 75 75, 100 100))'), ST_GeomFromText('POLYGON ((43 0, 50 50, 0 50, 0 0, 43 0))'))");
		b = (Boolean) Evaluator.evaluate(ex);
		assertFalse(b);
	}

}
