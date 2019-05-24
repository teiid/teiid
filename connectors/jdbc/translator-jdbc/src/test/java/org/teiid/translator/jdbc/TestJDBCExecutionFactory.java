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
package org.teiid.translator.jdbc;

import static org.junit.Assert.*;

import java.sql.Array;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Struct;
import java.sql.Types;
import java.util.Calendar;
import java.util.List;
import java.util.TimeZone;

import org.junit.Test;
import org.mockito.Mockito;
import org.teiid.core.types.GeographyType;
import org.teiid.core.types.GeometryType;
import org.teiid.core.util.TimestampWithTimezone;
import org.teiid.language.Literal;
import org.teiid.query.unittest.TimestampUtil;
import org.teiid.translator.TranslatorException;
import org.teiid.translator.TypeFacility;
import org.teiid.translator.jdbc.JDBCExecutionFactory.StructRetrieval;
import org.teiid.util.Version;

@SuppressWarnings("nls")
public class TestJDBCExecutionFactory {

    @Test public void testDatabaseCalender() throws Exception {
        final JDBCExecutionFactory jef = new JDBCExecutionFactory();
        jef.setDatabaseTimeZone("GMT"); //$NON-NLS-1$
        jef.start();

        final Calendar[] cals = new Calendar[2];

        Thread t1 = new Thread() {
            public void run() {
                cals[0] = jef.getDatabaseCalendar();
            }
        };
        t1.start();

        Thread t2 = new Thread() {
            public void run() {
                cals[1] = jef.getDatabaseCalendar();
            }
        };
        t2.start();
        t1.join();
        t2.join();

        assertNotSame(cals[0], cals[1]);
    }

    @Test public void testVersion() {
        JDBCExecutionFactory jef = new JDBCExecutionFactory();
        jef.setDatabaseVersion("Some db 1.2.3 (some build)");
        assertEquals("1.2.3", jef.getDatabaseVersion().toString());
        assertEquals(new Version(new Integer[] {1, 2, 3}), jef.getVersion());

        Version version = Version.getVersion("10.0");
        assertTrue(version.compareTo(Version.getVersion("9.1")) > 0);
        assertTrue(version.compareTo(Version.getVersion("10.0.1")) < 0);
    }

    @Test public void testStructRetrival() throws SQLException {
        JDBCExecutionFactory jef = new JDBCExecutionFactory();
        jef.setStructRetrieval(StructRetrieval.ARRAY);
        ResultSet rs = Mockito.mock(ResultSet.class);
        Struct s = Mockito.mock(Struct.class);
        Mockito.stub(rs.getObject(1)).toReturn(s);
        assertTrue(jef.retrieveValue(rs, 1, TypeFacility.RUNTIME_TYPES.OBJECT) instanceof Array);
    }

    @Test public void testBooleanRetrival() throws SQLException {
        JDBCExecutionFactory jef = new JDBCExecutionFactory();
        ResultSet rs = Mockito.mock(ResultSet.class);
        Mockito.stub(rs.getBoolean(1)).toReturn(false);
        Mockito.stub(rs.wasNull()).toReturn(true);
        assertNull(jef.retrieveValue(rs, 1, TypeFacility.RUNTIME_TYPES.BOOLEAN));
    }

    @Test public void testLiteralWithDatabaseTimezone() throws TranslatorException {
        TimestampWithTimezone.resetCalendar(TimeZone.getTimeZone("GMT"));
        try {
            JDBCExecutionFactory jef = new JDBCExecutionFactory();
            jef.setDatabaseTimeZone("GMT+1");
            jef.start();
            assertEquals("2015-02-03 05:00:00.0", jef.formatDateValue(TimestampUtil.createTimestamp(115, 1, 3, 4, 0, 0, 0)));
        } finally {
            TimestampWithTimezone.resetCalendar(null);
        }
    }

    @Test public void testInitCaps() throws Exception {
        JDBCExecutionFactory jef = new JDBCExecutionFactory();
        Connection connection = Mockito.mock(Connection.class);
        DatabaseMetaData mock = Mockito.mock(DatabaseMetaData.class);
        Mockito.stub(connection.getMetaData()).toReturn(mock);
        Mockito.stub(mock.supportsGetGeneratedKeys()).toThrow(new SQLException());
        //should still succeed even if an exception is thrown from supportsGetGeneratedKeys
        jef.initCapabilities(connection);
    }

    @Test public void testRemovePushdownCharacters() throws SQLException {
        JDBCExecutionFactory jef = new JDBCExecutionFactory();
        jef.setRemovePushdownCharacters("[\\u0000-\\u0002]");
        PreparedStatement ps = Mockito.mock(PreparedStatement.class);
        jef.bindValue(ps, "\u0000Hello\u0001World", TypeFacility.RUNTIME_TYPES.STRING, 1);
        Mockito.verify(ps).setObject(1, "HelloWorld", Types.VARCHAR);

        SQLConversionVisitor scv = new SQLConversionVisitor(jef);
        StringBuilder sb = new StringBuilder();
        scv.translateSQLType(TypeFacility.RUNTIME_TYPES.STRING, "\u0003?\u0002", sb);
        assertEquals("'\u0003?'", sb.toString());
    }

    @Test public void testBindNChar() throws SQLException {
        JDBCExecutionFactory jef = new JDBCExecutionFactory() {
            public boolean useUnicodePrefix() {return true;}
        };
        PreparedStatement ps = Mockito.mock(PreparedStatement.class);
        jef.bindValue(ps, "Hello\u0128World", TypeFacility.RUNTIME_TYPES.STRING, 1);
        Mockito.verify(ps).setObject(1, "Hello\u0128World", Types.NVARCHAR);
    }

    @Test public void testGeospatialLiterals() throws Exception {
        JDBCExecutionFactory jef = new JDBCExecutionFactory();
        List<?> result = jef.translateGeometryLiteral(new Literal(new GeometryType(), TypeFacility.RUNTIME_TYPES.GEOMETRY));
        assertTrue(result.get(0).toString().startsWith("st_geom"));

        GeographyType value = new GeographyType();
        result = jef.translateGeographyLiteral(new Literal(value, TypeFacility.RUNTIME_TYPES.GEOGRAPHY));
        assertTrue(result.get(0).toString().startsWith("st_geog"));

        value.setSrid(4333);
        result = jef.translateGeographyLiteral(new Literal(value, TypeFacility.RUNTIME_TYPES.GEOGRAPHY));
        assertTrue(result.get(0).toString().startsWith("st_setsrid(st_geog"));
        assertTrue(result.get(0).toString().endsWith("4333)"));
    }

}
