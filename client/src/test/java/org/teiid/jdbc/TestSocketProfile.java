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

package org.teiid.jdbc;

import static org.junit.Assert.*;

import java.sql.SQLException;
import java.util.Properties;

import org.junit.Test;
import org.teiid.jdbc.JDBCURL.ConnectionType;
import org.teiid.net.TeiidURL;


public class TestSocketProfile {
    public String localhost = "localhost"; //$NON-NLS-1$

    /** Valid format of urls*/
    @Test public void testAcceptsURL1()  throws Exception   {
        assertEquals(ConnectionType.Socket, JDBCURL.acceptsUrl("jdbc:teiid:jvdb@mm://localhost:1234")); //$NON-NLS-1$
        assertEquals(ConnectionType.Socket, JDBCURL.acceptsUrl("jdbc:teiid:jvdb@mm://localhost:1234")); //$NON-NLS-1$
        assertEquals(ConnectionType.Socket, JDBCURL.acceptsUrl("jdbc:teiid:vdb@mm://localhost:1234;version=x")); //$NON-NLS-1$
        assertEquals(ConnectionType.Socket, JDBCURL.acceptsUrl("jdbc:teiid:vdb@mm://localhost:1234")); //$NON-NLS-1$
        assertEquals(ConnectionType.Socket, JDBCURL.acceptsUrl("jdbc:teiid:vdb@mm://localhost:1234,localhost2:12342,localhost3:12343")); //$NON-NLS-1$
        assertEquals(ConnectionType.Socket, JDBCURL.acceptsUrl("jdbc:teiid:vdb@mm://localhost:1234;logFile=D:\\metamatrix\\work\\DQP\\log\\jdbcLogFile.log")); //$NON-NLS-1$
        assertEquals(ConnectionType.Socket, JDBCURL.acceptsUrl("jdbc:teiid:vdb@mm://localhost:1234,localhost2:12342,localhost3:12343;logFile=D:\\metamatrix\\work\\DQP\\log\\jdbcLogFile.log")); //$NON-NLS-1$
        assertEquals(ConnectionType.Socket, JDBCURL.acceptsUrl("jdbc:teiid:vdb@mms://localhost:1234;logLevel=1;logFile=D:\\metamatrix\\work\\DQP\\log\\jdbcLogFile.log")); //$NON-NLS-1$
        assertNull(JDBCURL.acceptsUrl("jdbc:teiid:@mm://localhost:1234;logLevel=2;logFile=D:\\metamatrix\\work\\DQP\\log\\jdbcLogFile.log")); //$NON-NLS-1$
        assertEquals(ConnectionType.Socket, JDBCURL.acceptsUrl("jdbc:teiid:vdb@mm://localhost:1234;logLevel=2;logFile=D:\\metamatrix\\work\\DQP\\log\\jdbcLogFile.log;autoCommitTxn=OFF;paritalResultsMode=true")); //$NON-NLS-1$
        assertEquals(ConnectionType.Socket, JDBCURL.acceptsUrl("jdbc:teiid:jvdb@mms://localhost:1234")); //$NON-NLS-1$
        assertEquals(ConnectionType.Socket, JDBCURL.acceptsUrl("jdbc:teiid:vdb@mm://localhost:1234;version=x")); //$NON-NLS-1$
        assertEquals(ConnectionType.Socket, JDBCURL.acceptsUrl("jdbc:teiid:vdb@mm://localhost:1234,localhost2:12342,localhost3:12343")); //$NON-NLS-1$
        assertEquals(ConnectionType.Socket, JDBCURL.acceptsUrl("jdbc:teiid:vdb@mms://localhost:1234,localhost2:12342,localhost3:12343;logFile=D:\\metamatrix\\work\\DQP\\log\\jdbcLogFile.log")); //$NON-NLS-1$
        assertEquals(ConnectionType.Socket, JDBCURL.acceptsUrl("jdbc:teiid:vdb@mms://localhost:1234;logLevel=2;logFile=D:\\metamatrix\\work\\DQP\\log\\jdbcLogFile.log;autoCommitTxn=OFF;paritalResultsMode=true")); //$NON-NLS-1$
        assertEquals(ConnectionType.Socket, JDBCURL.acceptsUrl("jdbc:teiid:vdb@mm://127.0.0.1:1234;logLevel=2")); //$NON-NLS-1$
        assertEquals(ConnectionType.Socket, JDBCURL.acceptsUrl("jdbc:teiid:vdb@mms://127.0.0.1:1234")); //$NON-NLS-1$
        assertEquals(ConnectionType.Socket, JDBCURL.acceptsUrl("jdbc:teiid:vdb@mm://127.0.0.1:1234,localhost.mydomain.com:63636;logLevel=2")); //$NON-NLS-1$
        assertEquals(ConnectionType.Socket, JDBCURL.acceptsUrl("jdbc:teiid:vdb@mm://my-host.mydomain.com:53535,127.0.0.1:1234")); //$NON-NLS-1$
        assertEquals(ConnectionType.Socket, JDBCURL.acceptsUrl("jdbc:teiid:vdb@mm://123.123.123.123:53535,127.0.0.1:1234")); //$NON-NLS-1$

        assertEquals(ConnectionType.Socket, JDBCURL.acceptsUrl("jdbc:teiid:jvdb@localhost:1234")); //$NON-NLS-1$

        //DQP type
        assertEquals(ConnectionType.Socket, JDBCURL.acceptsUrl("jdbc:teiid:jvdb@c:/dqp.properties;version=1")); //$NON-NLS-1$
        assertEquals(ConnectionType.Socket, JDBCURL.acceptsUrl("jdbc:teiid:jvdb@/foo/dqp.properties;version=1")); //$NON-NLS-1$
        assertEquals(ConnectionType.Socket, JDBCURL.acceptsUrl("jdbc:teiid:jvdb@../foo/dqp.properties;version=1")); //$NON-NLS-1$

        assertEquals(ConnectionType.Socket, JDBCURL.acceptsUrl("jdbc:teiid:jvdb@mm://localhost:port")); //$NON-NLS-1$
        assertEquals(ConnectionType.Socket, JDBCURL.acceptsUrl("jdbc:teiid:vdb@localhost:port;version=x")); //$NON-NLS-1$
        assertNull(JDBCURL.acceptsUrl("jdbc:teiid:@localhost:1234")); //$NON-NLS-1$
        assertNull(JDBCURL.acceptsUrl("jdbc:teiid:@localhost:1234,localhost2:12342,localhost3:12343")); //$NON-NLS-1$
        assertNull(JDBCURL.acceptsUrl("jdbc:teiid:@localhost:1234;logFile=D:\\metamatrix\\work\\DQP\\log\\jdbcLogFile.log")); //$NON-NLS-1$
        assertNull(JDBCURL.acceptsUrl("jdbc:teiid:@localhost:1234,localhost2:12342,localhost3:12343;logFile=D:\\metamatrix\\work\\DQP\\log\\jdbcLogFile.log")); //$NON-NLS-1$
        assertNull(JDBCURL.acceptsUrl("jdbc:teiid:@localhost:1234;logLevel=1;logFile=D:\\metamatrix\\work\\DQP\\log\\jdbcLogFile.log")); //$NON-NLS-1$
        assertNull(JDBCURL.acceptsUrl("jdbc:teiid:@localhost:1234;logLevel=2;logFile=D:\\metamatrix\\work\\DQP\\log\\jdbcLogFile.log")); //$NON-NLS-1$
        assertNull(JDBCURL.acceptsUrl("jdbc:teiid:@localhost:1234;logLevel=2;logFile=D:\\metamatrix\\work\\DQP\\log\\jdbcLogFile.log;autoCommitTxn=OFF;paritalResultsMode=true")); //$NON-NLS-1$
        assertNull(JDBCURL.acceptsUrl("jdbc:teiid:@localhost:1234;stickyConnections=false;socketsPerVM=4")); //$NON-NLS-1$
        assertEquals(ConnectionType.Socket, JDBCURL.acceptsUrl("jdbc:teiid:vdb@mm://my_host.mydomain.com:53535,127.0.0.1:1234")); //$NON-NLS-1$
    }

    /** Invalid format of urls*/
    @Test public void testAcceptsURL2() throws Exception    {
        assertTrue(!TeiidDriver.getInstance().acceptsURL("jdbc:matamatrix:test")); //$NON-NLS-1$
        assertTrue(!TeiidDriver.getInstance().acceptsURL("metamatrix:test")); //$NON-NLS-1$
        assertTrue(!TeiidDriver.getInstance().acceptsURL("jdbc&matamatrix:test")); //$NON-NLS-1$
        assertTrue(!TeiidDriver.getInstance().acceptsURL("jdbc;metamatrix:test")); //$NON-NLS-1$
    }

    @Test public void testParseURL() throws SQLException{
        Properties p = new Properties();
        TeiidDriver.parseURL("jdbc:teiid:BQT@mm://slwxp157:1234", p); //$NON-NLS-1$
        assertTrue(p.getProperty(BaseDataSource.VDB_NAME).equals("BQT")); //$NON-NLS-1$
        assertTrue(p.getProperty(TeiidURL.CONNECTION.SERVER_URL).equals("mm://slwxp157:1234")); //$NON-NLS-1$
        assertEquals(3, p.size());
    }

    @Test public void testParseURL2() throws SQLException {
        Properties p = new Properties();
        TeiidDriver.parseURL("jdbc:teiid:BQT@mms://slwxp157:1234;version=3", p); //$NON-NLS-1$
        assertTrue(p.getProperty(BaseDataSource.VDB_NAME).equals("BQT")); //$NON-NLS-1$
        assertTrue(p.getProperty(BaseDataSource.VDB_VERSION).equals("3")); //$NON-NLS-1$
        assertTrue(p.getProperty(TeiidURL.CONNECTION.SERVER_URL).equals("mms://slwxp157:1234")); //$NON-NLS-1$
        assertTrue(p.getProperty(BaseDataSource.VERSION).equals("3")); //$NON-NLS-1$
        assertTrue(p.getProperty(BaseDataSource.APP_NAME).equals(BaseDataSource.DEFAULT_APP_NAME));
        assertEquals(5, p.size());
    }

    @Test public void testParseURL3() throws SQLException{
        Properties p = new Properties();
        TeiidDriver.parseURL("jdbc:teiid:BQT@mm://slwxp157:1234,slntmm01:43401,sluxmm09:43302;version=4;autoCommitTxn=ON;partialResultsMode=YES;ApplicationName=Client", p); //$NON-NLS-1$
        assertTrue(p.getProperty(BaseDataSource.VDB_NAME).equals("BQT")); //$NON-NLS-1$
        assertTrue(p.getProperty(BaseDataSource.VDB_VERSION).equals("4"));         //$NON-NLS-1$
        assertTrue(p.getProperty(ExecutionProperties.PROP_TXN_AUTO_WRAP).equals("ON")); //$NON-NLS-1$
        assertTrue(p.getProperty(ExecutionProperties.PROP_PARTIAL_RESULTS_MODE).equals("YES")); //$NON-NLS-1$
        assertTrue(p.getProperty(TeiidURL.CONNECTION.SERVER_URL).equals("mm://slwxp157:1234,slntmm01:43401,sluxmm09:43302")); //$NON-NLS-1$
        assertTrue(p.getProperty(BaseDataSource.VERSION).equals("4")); //$NON-NLS-1$
        assertTrue(p.getProperty(BaseDataSource.APP_NAME).equals("Client")); //$NON-NLS-1$
        assertEquals(7, p.size());
    }

    @Test
    public void testIPV6() throws SQLException{
        assertEquals(ConnectionType.Socket, JDBCURL.acceptsUrl("jdbc:teiid:vdb@mm://[::1]:53535,127.0.0.1:1234")); //$NON-NLS-1$
        assertEquals(ConnectionType.Socket, JDBCURL.acceptsUrl("jdbc:teiid:vdb@mm://[3ffe:ffff:0100:f101::1]:53535,127.0.0.1:1234")); //$NON-NLS-1$

        Properties p = new Properties();
        TeiidDriver.parseURL("jdbc:teiid:BQT@mms://[3ffe:ffff:0100:f101::1]:1234;version=3", p); //$NON-NLS-1$
        assertTrue(p.getProperty(BaseDataSource.VDB_NAME).equals("BQT")); //$NON-NLS-1$
        assertTrue(p.getProperty(BaseDataSource.VDB_VERSION).equals("3")); //$NON-NLS-1$
        assertTrue(p.getProperty(TeiidURL.CONNECTION.SERVER_URL).equals("mms://[3ffe:ffff:0100:f101::1]:1234")); //$NON-NLS-1$
        assertTrue(p.getProperty(BaseDataSource.VERSION).equals("3")); //$NON-NLS-1$
    }

    @Test
    public void testIPV6MultipleHosts() throws SQLException{
        Properties p = new Properties();
        TeiidDriver.parseURL("jdbc:teiid:BQT@mms://[3ffe:ffff:0100:f101::1]:1234,[::1]:31000,127.0.0.1:2134;version=3", p); //$NON-NLS-1$
        assertTrue(p.getProperty(BaseDataSource.VDB_NAME).equals("BQT")); //$NON-NLS-1$
        assertTrue(p.getProperty(BaseDataSource.VDB_VERSION).equals("3")); //$NON-NLS-1$
        assertTrue(p.getProperty(TeiidURL.CONNECTION.SERVER_URL).equals("mms://[3ffe:ffff:0100:f101::1]:1234,[::1]:31000,127.0.0.1:2134")); //$NON-NLS-1$
        assertTrue(p.getProperty(BaseDataSource.VERSION).equals("3")); //$NON-NLS-1$
    }
}
