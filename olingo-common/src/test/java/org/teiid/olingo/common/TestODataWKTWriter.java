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

package org.teiid.olingo.common;

import static org.junit.Assert.assertEquals;

import org.apache.olingo.commons.api.edm.EdmPrimitiveTypeException;
import org.apache.olingo.commons.core.edm.primitivetype.EdmGeometryPolygon;
import org.junit.Test;
import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.GeometryFactory;
import org.locationtech.jts.geom.LinearRing;
import org.locationtech.jts.geom.Polygon;

@SuppressWarnings("nls")
public class TestODataWKTWriter {

    @Test
    public void testPolygon() throws EdmPrimitiveTypeException {
        ODataWKTWriter writer = new ODataWKTWriter();
        GeometryFactory factory = new GeometryFactory();
        Polygon polygon = factory
                .createPolygon(
                        factory.createLinearRing(new Coordinate[] {
                                new Coordinate(0, 0), new Coordinate(3, 0),
                                new Coordinate(3, 3),
                                new Coordinate(0, 3), new Coordinate(0, 0) }),
                        new LinearRing[] { factory.createLinearRing(
                                new Coordinate[] { new Coordinate(1, 1),
                                        new Coordinate(2, 1),
                                        new Coordinate(2, 2),
                                        new Coordinate(1, 2),
                                        new Coordinate(1, 1) }) });
        //hole first, no space between rings
        assertEquals("Polygon((1.0 1.0,2.0 1.0,2.0 2.0,1.0 2.0,1.0 1.0),"
                + "(0.0 0.0,3.0 0.0,3.0 3.0,0.0 3.0,0.0 0.0))", writer.write(polygon));

        String uriValue = ODataTypeManager.geometryToODataValueString(polygon, true);

        org.apache.olingo.commons.api.edm.geo.Polygon olingoPolygon = (org.apache.olingo.commons.api.edm.geo.Polygon) EdmGeometryPolygon.getInstance().valueOfString(uriValue, null, null, null, null, null, EdmGeometryPolygon.getInstance().getDefaultType());

        assertEquals(0, olingoPolygon.getExterior().iterator().next().getX(), 0);
        assertEquals(1, olingoPolygon.getInterior(0).iterator().next().getX(), 0);
    }

}
