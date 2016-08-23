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
import static org.teiid.query.processor.TestProcessor.*;
import static org.teiid.query.resolver.TestFunctionResolving.*;

import java.io.ByteArrayOutputStream;
import java.sql.Blob;
import java.sql.Clob;
import java.util.Arrays;
import java.util.List;

import org.junit.Test;
import org.teiid.api.exception.query.ExpressionEvaluationException;
import org.teiid.client.util.ExceptionUtil;
import org.teiid.core.types.BinaryType;
import org.teiid.core.types.ClobImpl;
import org.teiid.core.types.ClobType;
import org.teiid.core.types.GeometryType;
import org.teiid.query.eval.Evaluator;
import org.teiid.query.function.GeometryTransformUtils;
import org.teiid.query.function.GeometryUtils;
import org.teiid.query.resolver.TestFunctionResolving;
import org.teiid.query.sql.symbol.Expression;
import org.teiid.query.unittest.RealMetadataFactory;

import com.vividsolutions.jts.geom.Coordinate;
import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.geom.GeometryFactory;
import com.vividsolutions.jts.geom.Point;
import com.vividsolutions.jts.io.OutputStreamOutStream;
import com.vividsolutions.jts.io.WKBWriter;
import com.vividsolutions.jts.io.WKTReader;

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
	
	@Test public void testTextError() throws Exception {
		Expression ex = TestFunctionResolving.getExpression("ST_GeomFromText('''hello''')");
		try {
			Evaluator.evaluate(ex);
			fail();
		} catch (ExpressionEvaluationException e) {
			assertNull(ExceptionUtil.getExceptionOfType(e, NullPointerException.class));
		}
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
                "{\"type\":\"Point\",\"coordinates\":[-48.23456,20.12345]}"
        );
        assertEval(
                "ST_AsGeoJson(ST_GeomFromText('POLYGON ((40 0, 50 50, 0 50, 0 0, 40 0))'))",
                "{\"type\":\"Polygon\",\"coordinates\":[[[40.0,0.0],[50.0,50.0],[0.0,50.0],[0.0,0.0],[40.0,0.0]]]}"
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
    
    @Test(expected=ExpressionEvaluationException.class) public void testEwkbZCooridinate() throws Exception {
    	WKBWriter writer = new WKBWriter(3, true);
    	GeometryFactory gf = new GeometryFactory();
    	Point point = gf.createPoint(new Coordinate(0, 0, 0));
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
    
    @Test public void testGeoJsonCollection() throws Exception {
    	Expression ex = TestFunctionResolving.getExpression("ST_AsGeoJson(ST_GeomFromText('GEOMETRYCOLLECTION(POINT(4 6),LINESTRING(4 6,7 10))'))");
		ClobType c = (ClobType) Evaluator.evaluate(ex);
		assertEquals("{\"type\":\"GeometryCollection\",\"geometries\":[{\"type\":\"Point\",\"coordinates\":[4.0,6.0]},{\"type\":\"LineString\",\"coordinates\":[[4.0,6.0],[7.0,10.0]]}]}", ClobType.getString(c));
    }
    
    @Test(expected=ExpressionEvaluationException.class) public void testEwktNotExpected() throws Exception {
    	Expression ex = TestFunctionResolving.getExpression("ST_GeomFromText('POINT(0 0 0)'))");
    	Evaluator.evaluate(ex);
    }
    
    @Test public void testEnvelope() throws Exception {
    	Expression ex = TestFunctionResolving.getExpression("ST_AsText(ST_Envelope(ST_GEOMFROMTEXT('LINESTRING(0 0, 1 3)')))");
		ClobType c = (ClobType) Evaluator.evaluate(ex);
		assertEquals("POLYGON ((0 0, 0 3, 1 3, 1 0, 0 0))", ClobType.getString(c));
    }
    
    @Test public void testEwkt() throws Exception {
    	Expression ex = TestFunctionResolving.getExpression("st_asewkt(ST_GeomFromEwkt('POINT(0 0 0)')))");
    	assertEquals("POINT (0 0)", ClobType.getString((ClobType)Evaluator.evaluate(ex)));
    }
    
    @Test public void testEwktWithSRID() throws Exception {
    	Expression ex = TestFunctionResolving.getExpression("st_asewkt(ST_GeomFromEwkt('SRID=4326;POINT(0 0)')))");
    	Evaluator.evaluate(ex);
    	
    	//whitespace
    	ex = TestFunctionResolving.getExpression("st_asewkt(ST_GeomFromEwkt('   SRID=4326;POINT(0 0)')))");
    	Evaluator.evaluate(ex);
    	
    	//mixed case
    	ex = TestFunctionResolving.getExpression("st_asewkt(ST_GeomFromEwkt('SrID=4326;POINT(0 0)')))");
    	assertEquals("SRID=4326;POINT (0 0)", ClobType.getString((ClobType)Evaluator.evaluate(ex)));
    }
    
    @Test public void testAsFromEwkb() throws Exception {
    	Expression ex = TestFunctionResolving.getExpression("st_asewkt(st_geomfromewkb(st_asewkb(ST_GeomFromEwkt('SrID=4326;POINT(0 0)'))))");
    	assertEquals("SRID=4326;POINT (0 0)", ClobType.getString((ClobType)Evaluator.evaluate(ex)));
    }
    
    @Test public void testSimplify() throws Exception {
		Expression ex = TestFunctionResolving.getExpression("ST_AsText(ST_SIMPLIFY(ST_GeomFromText('LINESTRING(1 1,2 2,2 3.5,1 3,1 2,2 1)'), 1))");
		assertEquals("LINESTRING (1 1, 2 3.5, 2 1)", ClobType.getString((ClobType)Evaluator.evaluate(ex)));
	}
    
    @Test public void testWithin() throws Exception {
		Expression ex = TestFunctionResolving.getExpression("ST_WITHIN(ST_GeomFromText('POINT(0 0)'), ST_GeomFromText('POINT(0 0)'))");
		assertTrue((Boolean)Evaluator.evaluate(ex));
		
		ex = TestFunctionResolving.getExpression("ST_WITHIN(ST_GeomFromText('POINT(0 1)'), ST_GeomFromText('POINT(0 0)'))");
		assertFalse((Boolean)Evaluator.evaluate(ex));
	}
    
    @Test public void testDWithin() throws Exception {
		Expression ex = TestFunctionResolving.getExpression("ST_DWITHIN(ST_GeomFromText('POINT(0 0)'), ST_GeomFromText('POINT(0 .5)'), 1)");
		assertTrue((Boolean)Evaluator.evaluate(ex));
		
		ex = TestFunctionResolving.getExpression("ST_DWITHIN(ST_GeomFromText('POINT(0 1)'), ST_GeomFromText('POINT(0 0)'), 1)");
		assertFalse((Boolean)Evaluator.evaluate(ex));
	}
    
    @Test public void testBoundingBoxIntesects() throws Exception {
		Expression ex = TestFunctionResolving.getExpression("ST_GEOMFROMTEXT('LINESTRING(0 0, 1 3)') && ST_GEOMFROMTEXT('POINT(0 1)')");
		assertTrue((Boolean)Evaluator.evaluate(ex));
	}
    
    @Test public void testExtent() throws Exception {
       final String sql = "SELECT st_astext(st_extent(g)) from (select ST_GEOMFROMTEXT('LINESTRING(0 0, 1 3)') as g union all select ST_GEOMFROMTEXT('POINT(5 5)')) as x"; //$NON-NLS-1$
        
       List<?>[] expected = new List[] { 
           Arrays.asList("POLYGON ((0 0, 0 5, 5 5, 5 0, 0 0))")
       };    
    
       ProcessorPlan plan = helpGetPlan(sql, RealMetadataFactory.example1Cached());

       helpProcess(plan, new HardcodedDataManager(), expected);
	}
    
    @Test public void testForce2d() throws Exception {
		Expression ex = TestFunctionResolving.getExpression("st_astext(st_force_2d(ST_GEOMFROMTEXT('LINESTRING(0 0, 1 3)')))");
		assertEquals("LINESTRING (0 0, 1 3)", ClobType.getString((ClobType)Evaluator.evaluate(ex)));
	}
    
    @Test public void testHasArc() throws Exception {
		Expression ex = TestFunctionResolving.getExpression("(st_hasarc(ST_GEOMFROMTEXT('LINESTRING(0 0, 1 3)')))");
		assertFalse((Boolean)Evaluator.evaluate(ex));
	}
    
    @Test public void testEndPoint() throws Exception {
		Expression ex = TestFunctionResolving.getExpression("ST_AsText(st_endpoint(ST_GEOMFROMTEXT('LINESTRING(0 0, 1 3)'))))");
		assertEquals("POINT (1 3)", ClobType.getString((ClobType)Evaluator.evaluate(ex)));
	}
    
    @Test public void testStartPoint() throws Exception {
        Expression ex = TestFunctionResolving.getExpression("ST_AsText(st_startpoint(ST_GEOMFROMTEXT('LINESTRING(0 0, 1 3)'))))");
        assertEquals("POINT (0 0)", ClobType.getString((ClobType)Evaluator.evaluate(ex)));
    }
    
    @Test public void testCoordDims() throws Exception {
		Expression ex = TestFunctionResolving.getExpression("ST_CoordDim(ST_GEOMFROMTEXT('LINESTRING EMPTY'))");
		assertEquals(2, Evaluator.evaluate(ex));
	}
    
    @Test public void testOrderingEquals() throws Exception {
		Expression ex = TestFunctionResolving.getExpression("ST_OrderingEquals(ST_GeomFromText('LINESTRING(0 0, 10 10)'),ST_GeomFromText('LINESTRING(0 0, 0 0, 10 10)'))");
		assertFalse((Boolean)Evaluator.evaluate(ex));
	}
    
    @Test public void testPointN() throws Exception {
		Expression ex = TestFunctionResolving.getExpression("ST_AsText(ST_PointN(ST_GeomFromText('LINESTRING(1 2, 3 2, 1 2)'),4))");
		assertNull(Evaluator.evaluate(ex));
	}
    
    @Test public void testPolygon() throws Exception {
        Expression ex = TestFunctionResolving.getExpression("ST_ASEWKT(ST_Polygon(ST_GeomFromText('LINESTRING(75.15 29.53,77 29,77.6 29.5, 75.15 29.53)'), 4326))");
        assertEquals("SRID=4326;POLYGON ((75.15 29.53, 77 29, 77.6 29.5, 75.15 29.53))", ClobType.getString((ClobType)Evaluator.evaluate(ex)));
    }
    
    @Test public void testRelate() throws Exception {
        Expression ex = TestFunctionResolving.getExpression("ST_Relate(ST_GeomFromText('POINT(1 2)'), ST_Buffer(ST_GeomFromText('POINT(1 2)'),2))");
        assertEquals("0FFFFF212", Evaluator.evaluate(ex));
    }
    
    @Test public void testRelatePattern() throws Exception {
        Expression ex = TestFunctionResolving.getExpression("ST_Relate(ST_GeomFromText('POINT(1 2)'), ST_Buffer(ST_GeomFromText('POINT(1 2)'),2), '*FF*FF212')");
        assertEquals(true, Evaluator.evaluate(ex));
    }
    
    @Test public void testX() throws Exception {
        Expression ex = TestFunctionResolving.getExpression("ST_X(ST_GeomFromText('POINT(1 2)'))");
        assertEquals(1.0, Evaluator.evaluate(ex));
    }
    
    @Test public void testY() throws Exception {
        Expression ex = TestFunctionResolving.getExpression("ST_Y(ST_GeomFromText('POINT(1 2)'))");
        assertEquals(2.0, Evaluator.evaluate(ex));
    }
    
    @Test public void testZ() throws Exception {
        Expression ex = TestFunctionResolving.getExpression("ST_Z(ST_GeomFromText('POINT(1 2)'))");
        assertNull(Evaluator.evaluate(ex));
    }
    
}
