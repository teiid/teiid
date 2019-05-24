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

package org.teiid.geo;

import static org.junit.Assert.*;
import static org.teiid.query.resolver.TestFunctionResolving.*;

import org.junit.Test;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.io.WKTReader;
import org.teiid.core.types.ClobType;
import org.teiid.query.eval.Evaluator;
import org.teiid.query.resolver.TestFunctionResolving;
import org.teiid.query.sql.symbol.Expression;

@SuppressWarnings("nls")
public class TestOptionalGeospatial {

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

    @Test public void testGeoJson() throws Exception {
        Expression ex = TestFunctionResolving.getExpression("ST_AsText(ST_GeomFromGeoJson(ST_AsGeoJSON(ST_GeomFromText('MULTIPOINT ((10 40), (40 30), (20 20), (30 10))'))))");
        assertEquals("MULTIPOINT ((10 40), (40 30), (20 20), (30 10))", ClobType.getString((ClobType)Evaluator.evaluate(ex)));

        ex = TestFunctionResolving.getExpression("ST_AsText(ST_GeomFromGeoJson(ST_AsGeoJSON(ST_GeomFromText('MULTILINESTRING ((10 10, 20 20, 10 40),(40 40, 30 30, 40 20, 30 10))'))))");
        assertEquals("MULTILINESTRING ((10 10, 20 20, 10 40), (40 40, 30 30, 40 20, 30 10))", ClobType.getString((ClobType)Evaluator.evaluate(ex)));

        ex = TestFunctionResolving.getExpression("ST_AsText(ST_GeomFromGeoJson(ST_AsGeoJSON(ST_GeomFromText('LINESTRING (30 10, 10 30, 40 40)'))))");
        assertEquals("LINESTRING (30 10, 10 30, 40 40)", ClobType.getString((ClobType)Evaluator.evaluate(ex)));
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

}
