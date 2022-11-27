package org.teiid.systemmodel;

import static org.junit.Assert.*;

import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.teiid.core.util.UnitTestUtil;
import org.teiid.jdbc.AbstractQueryTest;
import org.teiid.jdbc.FakeServer;
import org.teiid.jdbc.TeiidSQLException;

@SuppressWarnings("nls")
public class TestGeography extends AbstractQueryTest {

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

    @Test public void testNormalization() throws Exception {
        String sql = "select st_asewkt(st_geogfromwkb(st_asewkb(ST_GeomFromEWKT('SRID=4326;POINT(-192.1235 451)'))))";
        execute(sql);
        internalResultSet.next();
        assertEquals("SRID=4326;POINT (167.8765 89)", internalResultSet.getString(1));
    }

    @Test(expected=TeiidSQLException.class) public void testNotLonLat() throws Exception {
        String sql = "select st_asewkt(st_geogfromwkb(st_asewkb(ST_GeomFromEWKT('SRID=3333;POINT(-192.1235 451)'))))";
        execute(sql);
    }

    @Test public void testGeogFromText() throws Exception {
        String sql = "select st_asewkt(st_geogfromtext('POINT(-12 11)'))";
        execute(sql);
        internalResultSet.next();
        assertEquals("SRID=4326;POINT (-12 11)", internalResultSet.getString(1));
    }

    @Test public void testGeogCast() throws Exception {
        String sql = "select st_asewkt(cast(st_geogfromtext('POINT(-12 11)') as geometry))";
        execute(sql);
        internalResultSet.next();
        assertEquals("SRID=4326;POINT (-12 11)", internalResultSet.getString(1));
    }

    @Test public void testGeogBinary() throws Exception {
        String sql = "select st_asewkt(st_geogfromwkb(st_asbinary(st_geogfromtext('POINT(-12 11)'))))";
        execute(sql);
        internalResultSet.next();
        assertEquals("SRID=4326;POINT (-12 11)", internalResultSet.getString(1));
    }

}
