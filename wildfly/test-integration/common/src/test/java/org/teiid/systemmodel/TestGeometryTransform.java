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

package org.teiid.systemmodel;

import static org.junit.Assert.*;

import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.osgeo.proj4j.Proj4jException;
import org.teiid.core.types.ClobType;
import org.teiid.core.util.UnitTestUtil;
import org.teiid.jdbc.AbstractQueryTest;
import org.teiid.jdbc.FakeServer;
import org.teiid.jdbc.TeiidSQLException;

/**
 * Test cases for ST_Transform() function. These are here because the system
 * function uses the command context to lookup values from the SPATIAL_REF_SYS
 * system table.
 */
@SuppressWarnings("nls")
public class TestGeometryTransform extends AbstractQueryTest {

    private static final String VDB = "PartsSupplier"; //$NON-NLS-1$

    private static FakeServer server;

    @BeforeClass public static void setup() throws Exception {
        server = new FakeServer(true);
        server.deployVDB(VDB, UnitTestUtil.getTestDataPath() + "/PartsSupplier.vdb"); //$NON-NLS-1$
    }

    @Before public void setUp() throws Exception {
        this.internalConnection = server.createConnection("jdbc:teiid:" + VDB); //$NON-NLS-1$
       }

    @AfterClass public static void teardown() throws Exception {
        server.stop();
    }

    private void assertTransform(String wkt, int srcSrid, int tgtSrid, String expectedWkt) throws Exception {
        String sql = String.format("select ST_AsText(ST_Transform(ST_GeomFromText('%s',%d),%d))", wkt, srcSrid, tgtSrid); //$NON-NLS-1$
        execute(sql);
        internalResultSet.next();
        String result = ClobType.getString(internalResultSet.getClob(1));
        Assert.assertEquals(expectedWkt, result);
    }

    private void assertTransformFail(String wkt, int srcSrid, int tgtSrid, String expectedWkt) throws Exception {
        try {
            assertTransform(wkt, srcSrid, tgtSrid, expectedWkt);
        } catch (TeiidSQLException e) {
            for (Throwable t = e; t != null; t = t.getCause()) {
                if (t instanceof Proj4jException) {
                    System.err.println(t.getMessage());
                    return;
                }
            }
        }
        throw new Exception("Expected transform to fail!");
    }

    @Test public void testSpatialTransform() throws Exception {
        // Example from PostGIS docs: http://postgis.net/docs/ST_Transform.html
        assertTransform(
                "POLYGON((743238 2967416,743238 2967450,743265 2967450,743265.625 2967416,743238 2967416))", 2249, 4326,
                "POLYGON ((-71.1776848522251 42.39028965129019, -71.17768437663263 42.390382947800894, -71.17758443054647 42.39038266779173, -71.17758259272306 42.39028936479872, -71.1776848522251 42.39028965129019))"
        );
        // Example from Sybase docs: http://dcx.sybase.com/1200/en/dbspatial/pg-api-spatial-st-geometry-type-st-transform-method.html
        assertTransform(
                "POINT(-118 34)", 4326, 3310,
                "POINT (184755.868610501 -444218.17569026677)"
        );
        // Example from IBM docs: http://www-01.ibm.com/support/knowledgecenter/SSGU8G_12.1.0/com.ibm.spatial.doc/ids_spat_239.htm
        // Note: Results deviate from IBM example, but seem to match PostGIS. Why?
        assertTransform(
                "multipoint(573900 9350, 573900 9351, 573901 9351, 573901 9350, 573900 9350)", 2153, 32611,
                "MULTIPOINT ((573899.999999995 9350.000000308342), (573899.9999999953 9351.000000308373), (573900.9999999935 9351.000000308373), (573900.9999999935 9350.000000308342), (573899.999999995 9350.000000308342))"
                // IBM expected "MULTIPOINT (573898.627678 9349.9324469, 573898.627678 9350.9324471, 573899.627679 9350.93244701, 573899.627679 9349.93244681, 573898.627678 9349.9324469)"
        );
        // Example from PostGIS mailing list.
        assertTransform(
                "LINESTRING(606388.046000039 5335648.05400051,606468.307000043 5335634.86000051)", 28355, 4283,
                "LINESTRING (148.28709837045656 -42.12405782385746, 148.28807154929314 -42.124165730126194)"
        );
        // Example from Spatialite docs: http://www.gaia-gis.it/gaia-sins/spatialite-tutorial-2.3.1.html
        assertTransform(
                "POINT(390084.12 5025551.73)", 32632, 4326,
                "POINT (7.596214015140495 45.37485400208321)"
        );
        // Misc example.
        assertTransform(
                "GEOMETRYCOLLECTION(POINT(390084.12 5025551.73),LINESTRING(367470.48 4962414.5, 427002.77 4996361.33))", 32632, 4326,
                "GEOMETRYCOLLECTION (POINT (7.596214015140495 45.37485400208321), LINESTRING (7.324218946503897 44.80283799572692, 8.071928941724545 45.116951986419586))"
        );
    }

    @Test public void testSpatialTranformFail() throws Exception {
        // Example from H2GIS docs: http://www.h2gis.org/docs/dev/ST_Transform/
        //
        // Note: Parameter "pm" is not supported by proj4j; there are several open tickets for this.
        // http://trac.osgeo.org/proj4j/ticket/24
        // http://trac.osgeo.org/proj4j/ticket/29
        // http://trac.osgeo.org/proj4j/ticket/12
        assertTransformFail(
                "POINT(584173 2594514)", 27572, 4326,
                "POINT (2.1145411092971056 50.345602339855326)"
        );
    }

    @Test public void testTransformForKml() throws Exception {
        String wkt= "POINT(390084.12 5025551.73)";
        int srcSrid = 32632;
        String sql = String.format("select ST_AsKML(ST_GeomFromText('%s',%d))", wkt, srcSrid); //$NON-NLS-1$
        execute(sql);
        internalResultSet.next();
        String result = ClobType.getString(internalResultSet.getClob(1));
        String expectedWkt = "<Point>\n  <coordinates>\n    7.596214015140495,45.37485400208321 \n  </coordinates>\n</Point>\n";
        Assert.assertEquals(expectedWkt, result);
    }

    @Test public void testGeoProjectedDistance() throws Exception {
        String sql = "select ST_Distance(\n" +
            "ST_Transform(ST_GeomFromText('POINT(-72.1235 42.3521)',4326),26986),\n" +
            "ST_Transform(ST_GeomFromText('LINESTRING(-72.1260 42.45, -72.123 42.1546)', 4326),26986))";
        execute(sql);
        internalResultSet.next();
        assertEquals(123.797937878454, internalResultSet.getDouble(1), .0000001);
    }
}
