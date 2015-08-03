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

import java.io.ByteArrayOutputStream;
import java.sql.Blob;
import java.sql.Clob;

import org.junit.Test;
import org.teiid.api.exception.query.ExpressionEvaluationException;
import org.teiid.core.types.BinaryType;
import org.teiid.core.types.ClobImpl;
import org.teiid.core.types.ClobType;
import org.teiid.core.types.GeometryType;
import org.teiid.query.eval.Evaluator;
import org.teiid.query.function.GeometryTransformUtils;
import org.teiid.query.function.GeometryUtils;
import org.teiid.query.resolver.TestFunctionResolving;
import org.teiid.query.sql.symbol.Expression;

import com.vividsolutions.jts.geom.Coordinate;
import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.geom.GeometryFactory;
import com.vividsolutions.jts.geom.Point;
import com.vividsolutions.jts.io.OutputStreamOutStream;
import com.vividsolutions.jts.io.WKBWriter;
import com.vividsolutions.jts.io.WKTReader;
import static org.teiid.query.resolver.TestFunctionResolving.assertEval;

@SuppressWarnings("nls")
public class TestGeometry {

	@Test public void testRoundTrip() throws Exception {
		Expression ex = TestFunctionResolving.getExpression("ST_GeomFromText('POLYGON ((40 0, 50 50, 0 50, 0 0, 40 0))')");
		GeometryType geom = (GeometryType) Evaluator.evaluate(ex);
		assertEquals(0, geom.getSrid());
		byte[] bytes = geom.getBytes(1, (int) geom.length());
		
		Expression ex1 = TestFunctionResolving.getExpression("ST_GeomFromBinary(X'"+new BinaryType(bytes)+"', 8307)");
		GeometryType geom1 = (GeometryType) Evaluator.evaluate(ex1);
		assertEquals(8307, geom1.getSrid());
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

    @Test public void testAsGeoJson() throws Exception {        
        assertEval(
                "ST_AsGeoJson(ST_GeomFromText('POINT (-48.23456 20.12345)'))",
                "{\"coordinates\":[-48.23456,20.12345],\"type\":\"Point\"}"
        );
        assertEval(
                "ST_AsGeoJson(ST_GeomFromText('POLYGON ((40 0, 50 50, 0 50, 0 0, 40 0))'))",
                "{\"coordinates\":[[[40.0,0.0],[50.0,50.0],[0.0,50.0],[0.0,0.0],[40.0,0.0]]],\"type\":\"Polygon\"}"
        );
    }
        
    @Test public void testFromGeoJson() throws Exception {
        assertEval(
                "ST_AsText(ST_GeomFromGeoJSON('{\"coordinates\":[-48.23456,20.12345],\"type\":\"Point\"}'))",
                "POINT (-48.23456 20.12345)"
        );
        assertEval(
                "ST_AsText(ST_GeomFromGeoJSON('{\"coordinates\":[[[40.0,0.0],[50.0,50.0],[0.0,50.0],[0.0,0.0],[40.0,0.0]]],\"type\":\"Polygon\"}'))",
                "POLYGON ((40 0, 50 50, 0 50, 0 0, 40 0))"
        );
    }
        
    @Test public void testAsGml() throws Exception {        
        assertEval(
                "ST_AsGML(ST_GeomFromText('POINT (-48.23456 20.12345)'))",
                "<gml:Point>\n" +
                "  <gml:coordinates>\n" +
                "    -48.23456,20.12345 \n" +
                "  </gml:coordinates>\n" +
                "</gml:Point>\n"
        );
        assertEval(
                "ST_AsGML(ST_GeomFromText('POLYGON ((40 0, 50 50, 0 50, 0 0, 40 0))', 4326))",
                "<gml:Polygon>\n" +
                "  <gml:outerBoundaryIs>\n" +
                "    <gml:LinearRing>\n" +
                "      <gml:coordinates>\n" +
                "        40.0,0.0 50.0,50.0 0.0,50.0 0.0,0.0 40.0,0.0 \n" +
                "      </gml:coordinates>\n" +
                "    </gml:LinearRing>\n" +
                "  </gml:outerBoundaryIs>\n" +
                "</gml:Polygon>\n"
        );
    }
    
    
    @Test public void testFromGml() throws Exception {        
        assertEval(
                "ST_AsText(ST_GeomFromGML('" +
                "<gml:Point>\n" +
                "  <gml:coordinates>\n" +
                "    -48.23456,20.12345 \n" +
                "  </gml:coordinates>\n" +
                "</gml:Point>'))",
                "POINT (-48.23456 20.12345)"
        );
        assertEval("ST_AsText(ST_GeomFromGML('" +
                "<gml:Polygon>\n" +
                "  <gml:outerBoundaryIs>\n" +
                "    <gml:LinearRing>\n" +
                "      <gml:coordinates>\n" +
                "        40.0,0.0 50.0,50.0 0.0,50.0 0.0,0.0 40.0,0.0 \n" +
                "      </gml:coordinates>\n" +
                "    </gml:LinearRing>\n" +
                "  </gml:outerBoundaryIs>\n" +
                "</gml:Polygon>'))",
                "POLYGON ((40 0, 50 50, 0 50, 0 0, 40 0))"
        );
    }

    @Test
    public void testAsEwkt() throws Exception {
        assertEval("ST_AsEWKT(ST_GeomFromText('POLYGON((0 0,0 1,1 1,1 0,0 0))', 4326))",
                   "SRID=4326;POLYGON ((0 0, 0 1, 1 1, 1 0, 0 0))");
        assertEval("ST_AsEWKT(ST_GeomFromText('POLYGON((0 0,0 1,1 1,1 0,0 0))'))",
                   "POLYGON ((0 0, 0 1, 1 1, 1 0, 0 0))");
    }
    
    @Test
    public void testAsKml() throws Exception {
        assertEval("ST_AsKML(ST_GeomFromText('POLYGON((0 0,0 1,1 1,1 0,0 0))', 4326))",
                   "<Polygon>\n" + 
                   "  <outerBoundaryIs>\n"+
                   "    <LinearRing>\n"+
                   "      <coordinates>\n"+
                   "        0.0,0.0 0.0,1.0 1.0,1.0 1.0,0.0 0.0,0.0 \n"+
                   "      </coordinates>\n"+
                   "    </LinearRing>\n"+
                   "  </outerBoundaryIs>\n"+
                   "</Polygon>\n");
    }
    
    @Test(expected=ExpressionEvaluationException.class)
    public void testAsKmlException() throws Exception {
        assertEval("ST_AsKML(ST_GeomFromText('POLYGON((0 0,0 1,1 1,1 0,0 0))'))", null);
    }
    
    @Test public void testEquals() throws Exception {
    	Expression ex = TestFunctionResolving.getExpression("ST_Equals (ST_GeomFromText('LINESTRING(-1 2, 0 3)'), ST_GeomFromText('LINESTRING(0 3, -1 2)'))");
		Boolean b = (Boolean) Evaluator.evaluate(ex);
		assertTrue(b);
		
		ex = TestFunctionResolving.getExpression("ST_Equals (ST_GeomFromText('LINESTRING(0 0, 0 1, 0 3)'), ST_GeomFromText('LINESTRING(0 3, 0 0)'))");
		b = (Boolean) Evaluator.evaluate(ex);
		assertTrue(b);
		
		ex = TestFunctionResolving.getExpression("ST_Equals (ST_GeomFromText('LINESTRING(0 1, 0 3)'), ST_GeomFromText('LINESTRING(0 3, 0 0)'))");
		b = (Boolean) Evaluator.evaluate(ex);
		assertFalse(b);
    }
    
    @Test(expected=ExpressionEvaluationException.class) public void testEwkb() throws Exception {
    	WKBWriter writer = new WKBWriter(3, true);
    	GeometryFactory gf = new GeometryFactory();
    	Point point = gf.createPoint(new Coordinate(0, 0, 0));
    	point.setSRID(100);
    	ByteArrayOutputStream baos = new ByteArrayOutputStream();
    	writer.write(point, new OutputStreamOutStream(baos));
    	
		Expression ex1 = TestFunctionResolving.getExpression("ST_GeomFromBinary(X'"+new BinaryType(baos.toByteArray())+"', 8307)");
		Evaluator.evaluate(ex1);
    }
    
    @Test
    public void testGmlParseSrid() throws Exception {
        String gml = "<gml:Polygon srsName=\"SDO:8307\" xmlns:gml=\"http://www.opengis.net/gml\"><gml:outerBoundaryIs><gml:LinearRing><gml:coordinates decimal=\".\" cs=\",\" ts=\" \">5,1 8,1 8,6 5,7 5,1 </gml:coordinates></gml:LinearRing></gml:outerBoundaryIs></gml:Polygon>";
        GeometryType gt = GeometryUtils.geometryFromGml(new ClobType(ClobImpl.createClob(gml.toCharArray())), null);
        assertEquals(8307, gt.getSrid());
        
        //oracle will leave of the int with unknown
        gml = "<gml:Polygon srsName=\"SDO:\" xmlns:gml=\"http://www.opengis.net/gml\"><gml:outerBoundaryIs><gml:LinearRing><gml:coordinates decimal=\".\" cs=\",\" ts=\" \">5,1 8,1 8,6 5,7 5,1 </gml:coordinates></gml:LinearRing></gml:outerBoundaryIs></gml:Polygon>";
        gt = GeometryUtils.geometryFromGml(new ClobType(ClobImpl.createClob(gml.toCharArray())), null);
        assertEquals(GeometryType.UNKNOWN_SRID, gt.getSrid());
    }

    @Test public void testTransform() throws Exception {
        Geometry g0 = new WKTReader().read("POINT(426418.89 4957737.37)");
        Geometry g1 = GeometryTransformUtils.transform(g0,
                "+proj=utm +zone=32 +datum=WGS84 +units=m +no_defs", // EPSG:32632
                "+proj=longlat +datum=WGS84 +no_defs" // EPSG:4326
        );
        assertEquals("POINT (8.07013599546795 44.76924401481436)", g1.toText());
    }
}
