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

import java.io.ByteArrayOutputStream;
import java.io.PrintWriter;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Properties;

import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;
import org.teiid.client.RequestMessage.ShowPlan;
import org.teiid.core.util.UnitTestUtil;
import org.teiid.net.TeiidURL;

import junit.framework.TestCase;

@SuppressWarnings("nls")
public class TestTeiidDataSource extends TestCase {

    protected static final boolean VALID = true;
    protected static final boolean INVALID = false;

    private TeiidDataSource dataSource;

    protected static final String STD_SERVER_NAME           = "unitTestServerName"; //$NON-NLS-1$
    protected static final String STD_DATABASE_NAME         = "unitTestVdbName"; //$NON-NLS-1$
    protected static final String STD_DATABASE_VERSION      = "unitTestVdbVersion"; //$NON-NLS-1$
    protected static final String STD_DATA_SOURCE_NAME      = "unitTestDataSourceName"; //$NON-NLS-1$
    protected static final int    STD_PORT_NUMBER           = 7001;
    protected static final String STD_LOG_FILE              = UnitTestUtil.getTestScratchPath() + "/unitTestLogFile"; //$NON-NLS-1$
    protected static final int    STD_LOG_LEVEL             = 2;
    protected static final String STD_TXN_WRAP         = TeiidDataSource.TXN_WRAP_AUTO;
    protected static final String STD_PARTIAL_MODE         = "false"; //$NON-NLS-1$
    protected static final String STD_CONFIG_FILE          = UnitTestUtil.getTestDataPath() + "/bqt/bqt.properties";  //$NON-NLS-1$
    protected static final String STD_ALTERNATE_SERVERS     = "unitTestServerName2:7001,unitTestServerName2:7002,unitTestServerName3:7001"; //$NON-NLS-1$

    /**
     * Constructor for TestTeiidDataSource.
     * @param name
     */
    public TestTeiidDataSource(String name) {
        super(name);
    }

    /**
     * @see TestCase#setUp()
     */
    protected void setUp() throws Exception {
        super.setUp();
        dataSource = new TeiidDataSource();
        dataSource.setServerName(STD_SERVER_NAME);
        dataSource.setDatabaseVersion(STD_DATABASE_VERSION);
        dataSource.setDatabaseName(STD_DATABASE_NAME);
        dataSource.setPortNumber(STD_PORT_NUMBER);
        dataSource.setDataSourceName(STD_DATA_SOURCE_NAME);
        dataSource.setAutoCommitTxn(STD_TXN_WRAP);
        dataSource.setPartialResultsMode(STD_PARTIAL_MODE);
        dataSource.setSecure(true);
        dataSource.setAlternateServers(STD_ALTERNATE_SERVERS);
        dataSource.setUseJDBC4ColumnNameAndLabelSemantics(true);
    }

    // =========================================================================
    //                      H E L P E R   M E T H O D S
    // =========================================================================

    protected String getReasonWhyInvalid( final String propertyName, final String value ) {
        if ( propertyName.equals("DatabaseName") ) { //$NON-NLS-1$
            return TeiidDataSource.reasonWhyInvalidDatabaseName(value);
        } else if ( propertyName.equals("DatabaseVersion") ) { //$NON-NLS-1$
            return TeiidDataSource.reasonWhyInvalidDatabaseVersion(value);
        } else if ( propertyName.equals("DataSourceName") ) { //$NON-NLS-1$
            return TeiidDataSource.reasonWhyInvalidDataSourceName(value);
        } else if ( propertyName.equals("Description") ) { //$NON-NLS-1$
            return TeiidDataSource.reasonWhyInvalidDescription(value);
        } else if ( propertyName.equals("ServerName") ) { //$NON-NLS-1$
            return TeiidDataSource.reasonWhyInvalidServerName(value);
        } else if ( propertyName.equals("TransactionAutoWrap") ) { //$NON-NLS-1$
            return TeiidDataSource.reasonWhyInvalidTransactionAutoWrap(value);
        } else if ( propertyName.equals("partialResultsMode")) { //$NON-NLS-1$
            return TeiidDataSource.reasonWhyInvalidPartialResultsMode(value);
        } else if ( propertyName.equals("socketsPerVM")) { //$NON-NLS-1$
            return TeiidDataSource.reasonWhyInvalidSocketsPerVM(value);
        } else if ( propertyName.equals("stickyConnections")) { //$NON-NLS-1$
            return TeiidDataSource.reasonWhyInvalidStickyConnections(value);
        }

        fail("Unknown property name \"" + propertyName + "\""); //$NON-NLS-1$ //$NON-NLS-2$
        return null;
    }

    protected String getReasonWhyInvalid( final String propertyName, final int value ) {
        if ( propertyName.equals("PortNumber") ) { //$NON-NLS-1$
            return TeiidDataSource.reasonWhyInvalidPortNumber(value);
        }
        fail("Unknown property name \"" + propertyName + "\""); //$NON-NLS-1$ //$NON-NLS-2$
        return null;
    }

    public void helpTestReasonWhyInvalid( final String propertyName, final String value,
                                          final boolean shouldBeValid) {
        final String reason = getReasonWhyInvalid(propertyName,value);
        if ( shouldBeValid ) {
            assertNull("Unexpectedly considered invalid value \"" + value + "\"; reason = " + reason,reason); //$NON-NLS-1$ //$NON-NLS-2$
        } else {
            assertNotNull("Unexpectedly found no reason for value \"" + value + "\"",reason); //$NON-NLS-1$ //$NON-NLS-2$
        }
    }

    public void helpTestReasonWhyInvalid( final String propertyName, final int value,
                                          final boolean shouldBeValid) {
        final String reason = getReasonWhyInvalid(propertyName,value);
        if ( shouldBeValid ) {
            assertNull("Unexpectedly considered invalid value " + value + "; reason = " + reason,reason); //$NON-NLS-1$ //$NON-NLS-2$
        } else {
            assertNotNull("Unexpectedly found no reason for value " + value,reason); //$NON-NLS-1$
        }
    }

    public void helpTestBuildingURL( final String vdbName, final String vdbVersion,
                                     final String serverName, final int portNumber,
                                     final String alternateServers,
                                     final String txnAutoWrap, final String partialMode,
                                     final int fetchSize, final boolean showPlan,
                                     final boolean secure, final String expectedURL) {


            helpTestBuildingURL2(vdbName, vdbVersion, serverName, portNumber, alternateServers,
                        txnAutoWrap, partialMode, fetchSize, showPlan, secure, true, expectedURL);
    }

    public void helpTestBuildingURL2( final String vdbName, final String vdbVersion,
            final String serverName, final int portNumber,
            final String alternateServers,
            final String txnAutoWrap, final String partialMode,
            final int fetchSize, final boolean showPlan,
            final boolean secure, final boolean useJDBC4Semantics,
            final String expectedURL ) {

        final TeiidDataSource ds = new TeiidDataSource();
        ds.setServerName(serverName);
        ds.setDatabaseVersion(vdbVersion);
        ds.setDatabaseName(vdbName);
        ds.setPortNumber(portNumber);
        ds.setFetchSize(fetchSize);
        ds.setAutoCommitTxn(txnAutoWrap);
        ds.setPartialResultsMode(partialMode);
        if(showPlan) {
            ds.setShowPlan(ShowPlan.ON.toString());
        }
        ds.setSecure(secure);
        ds.setAlternateServers(alternateServers);
        ds.setUseJDBC4ColumnNameAndLabelSemantics(useJDBC4Semantics);

        String url;
        try {
            url = ds.buildURL().getJDBCURL();
        } catch (TeiidSQLException e) {
            throw new RuntimeException(e);
        }
        compareUrls(expectedURL, url);
    }

    /**
     * Compare urls without regard to property ordering
     * @param expectedURL
     * @param url
     */
    private void compareUrls(final String expectedURL, String url) {
        String parts[] = url.split(";", 2);
        String expectedParts[] = expectedURL.split(";", 2);

        assertEquals(parts[0], expectedParts[0]);
        assertEquals(new HashSet<String>(Arrays.asList(parts[1].split(";"))), new HashSet<String>(Arrays.asList(expectedParts[1].split(";"))));
    }

    public Connection helpTestConnection( final String vdbName, final String vdbVersion,
                                    final String serverName, final int portNumber, final String alternateServers,
                                    final String user, final String password,
                                    final String dataSourceName,
                                    final String txnAutoWrap, final String partialMode,
                                    final String configFile )
                                    throws SQLException {

        TeiidDataSource ds = new TeiidDataSource();

        ds.setServerName(serverName);
        ds.setDatabaseVersion(vdbVersion);
        ds.setDatabaseName(vdbName);
        ds.setPortNumber(portNumber);
        ds.setUser(user);
        ds.setPassword(password);
        ds.setDataSourceName(dataSourceName);
        ds.setAutoCommitTxn(txnAutoWrap);
        ds.setPartialResultsMode(partialMode);
        ds.setAlternateServers(alternateServers);
        ds.setUseJDBC4ColumnNameAndLabelSemantics(true);

        return ds.getConnection();

    }

    // =========================================================================
    //                         T E S T     C A S E S
    // =========================================================================

    // ----------------------------------------------------------------
    //                       Test Getters
    // ----------------------------------------------------------------

    public void testGetServerName() {
        final String result = dataSource.getServerName();
        assertEquals(result,STD_SERVER_NAME);
    }

    public void testGetDatabaseVersion() {
        final String result = dataSource.getDatabaseVersion();
        assertEquals(result,STD_DATABASE_VERSION);
    }

    public void testGetDatabaseName() {
        final String result = dataSource.getDatabaseName();
        assertEquals(result,STD_DATABASE_NAME);
    }

    public void testGetDefaultApplicationName() {
        final String result = dataSource.getApplicationName();
        assertEquals(result,BaseDataSource.DEFAULT_APP_NAME);
    }

    public void testGetApplicationName() {
        dataSource.setApplicationName("ClientApp"); //$NON-NLS-1$
        final String result = dataSource.getApplicationName();
        assertEquals(result,"ClientApp"); //$NON-NLS-1$
    }

    public void testGetPortNumber() {
        final int result = dataSource.getPortNumber();
        assertEquals(result,STD_PORT_NUMBER);
    }

    public void testGetDataSourceName() {
        final String result = dataSource.getDataSourceName();
        assertEquals(result,STD_DATA_SOURCE_NAME);
    }

    public void testGetLoginTimeout() {
        try {
            final int actual = 1000;
            dataSource.setLoginTimeout(actual);
            final int result = dataSource.getLoginTimeout();
            assertEquals(result,actual);
        } catch ( SQLException e ) {
            fail("Error obtaining login timeout"); //$NON-NLS-1$
        }
    }

    public void testGetLogWriter() {
        try {
            final PrintWriter actual = new PrintWriter( new ByteArrayOutputStream() );
            dataSource.setLogWriter(actual);
            final PrintWriter result = dataSource.getLogWriter();
            assertEquals(result,actual);
        } catch ( SQLException e ) {
            fail("Error obtaining login timeout"); //$NON-NLS-1$
        }
    }

    public void testGetTransactionAutoWrap() {
        final String result = dataSource.getAutoCommitTxn();
        Properties p = dataSource.buildProperties("foo", "bar");
        assertEquals(p.getProperty(ExecutionProperties.PROP_TXN_AUTO_WRAP), STD_TXN_WRAP);
        assertEquals(result,STD_TXN_WRAP);
    }

    public void testGetSecure() {
        assertTrue(dataSource.isSecure());
        dataSource.setSecure(false);
        assertFalse(dataSource.isSecure());
    }

    public void testGetAlternateServers() {
        String result = dataSource.getAlternateServers();
        assertEquals(result,STD_ALTERNATE_SERVERS);
        dataSource.setAlternateServers(null);
        result = dataSource.getAlternateServers();
        assertNull(result);
        dataSource.setAlternateServers(STD_ALTERNATE_SERVERS);
        result = dataSource.getAlternateServers();
        assertEquals(result,STD_ALTERNATE_SERVERS);
    }

    // ----------------------------------------------------------------
    //                       Test invalid reasons
    // ----------------------------------------------------------------

    public void testReasonWhyInvalidDatabaseName1() {
        helpTestReasonWhyInvalid("DatabaseName", "Valid VDB Name", VALID); //$NON-NLS-1$ //$NON-NLS-2$
    }
    public void testReasonWhyInvalidDatabaseName2() {
        helpTestReasonWhyInvalid("DatabaseName", "", INVALID); //$NON-NLS-1$ //$NON-NLS-2$
    }
    public void testReasonWhyInvalidDatabaseName3() {
        helpTestReasonWhyInvalid("DatabaseName", null, INVALID); //$NON-NLS-1$
    }


    public void testReasonWhyInvalidDatabaseVersion1() {
        helpTestReasonWhyInvalid("DatabaseVersion", "Valid VDB Version", VALID); //$NON-NLS-1$ //$NON-NLS-2$
    }
    public void testReasonWhyInvalidDatabaseVersion2() {
        helpTestReasonWhyInvalid("DatabaseVersion", "1", VALID); //$NON-NLS-1$ //$NON-NLS-2$
    }
    public void testReasonWhyInvalidDatabaseVersion3() {
        helpTestReasonWhyInvalid("DatabaseVersion", "1.2.3", VALID); //$NON-NLS-1$ //$NON-NLS-2$
    }
    public void testReasonWhyInvalidDatabaseVersion4() {
        helpTestReasonWhyInvalid("DatabaseVersion", "1 2 3", VALID); //$NON-NLS-1$ //$NON-NLS-2$
    }
    public void testReasonWhyInvalidDatabaseVersion5() {
        helpTestReasonWhyInvalid("DatabaseVersion", "", VALID); //$NON-NLS-1$ //$NON-NLS-2$
    }
    public void testReasonWhyInvalidDatabaseVersion6() {
        helpTestReasonWhyInvalid("DatabaseVersion", null, VALID); //$NON-NLS-1$
    }


    public void testReasonWhyInvalidDataSourceName1() {
        helpTestReasonWhyInvalid("DataSourceName", "Valid Data Source Name", VALID); //$NON-NLS-1$ //$NON-NLS-2$
    }
    public void testReasonWhyInvalidDataSourceName2() {
        helpTestReasonWhyInvalid("DataSourceName", "", VALID); //$NON-NLS-1$ //$NON-NLS-2$
    }
    public void testReasonWhyInvalidDataSourceName3() {
        helpTestReasonWhyInvalid("DataSourceName", "", VALID); //$NON-NLS-1$ //$NON-NLS-2$
    }


    public void testReasonWhyInvalidDescription1() {
        helpTestReasonWhyInvalid("Description", "Valid App Name", VALID); //$NON-NLS-1$ //$NON-NLS-2$
    }
    public void testReasonWhyInvalidDescription2() {
        helpTestReasonWhyInvalid("Description", "", VALID); //$NON-NLS-1$ //$NON-NLS-2$
    }
    public void testReasonWhyInvalidDescription3() {
        helpTestReasonWhyInvalid("Description", null, VALID); //$NON-NLS-1$
    }

    public void testReasonWhyInvalidPortNumber1() {
        helpTestReasonWhyInvalid("PortNumber", 1, VALID); //$NON-NLS-1$
    }
    public void testReasonWhyInvalidPortNumber2() {
        helpTestReasonWhyInvalid("PortNumber", 9999999, INVALID); //$NON-NLS-1$
    }
    public void testReasonWhyInvalidPortNumber3() {
        helpTestReasonWhyInvalid("PortNumber", 0, VALID); //$NON-NLS-1$
    }
    public void testReasonWhyInvalidPortNumber4() {
        helpTestReasonWhyInvalid("PortNumber", -1, INVALID); //$NON-NLS-1$
    }


    public void testReasonWhyInvalidServerName1() {
        helpTestReasonWhyInvalid("ServerName", "Valid Server Name", VALID); //$NON-NLS-1$ //$NON-NLS-2$
    }
    public void testReasonWhyInvalidServerName2() {
        helpTestReasonWhyInvalid("ServerName", "Valid Server Name", VALID); //$NON-NLS-1$ //$NON-NLS-2$
    }
    public void testReasonWhyInvalidServerName3() {
        helpTestReasonWhyInvalid("ServerName", "", INVALID); //$NON-NLS-1$ //$NON-NLS-2$
    }
    public void testReasonWhyInvalidServerName4() {
        helpTestReasonWhyInvalid("ServerName", null, INVALID); //$NON-NLS-1$
    }


    public void testReasonWhyInvalidTransactionAutoWrap1() {
        helpTestReasonWhyInvalid("TransactionAutoWrap", TeiidDataSource.TXN_WRAP_OFF, VALID); //$NON-NLS-1$
    }
    public void testReasonWhyInvalidTransactionAutoWrap2() {
        helpTestReasonWhyInvalid("TransactionAutoWrap", TeiidDataSource.TXN_WRAP_ON, VALID); //$NON-NLS-1$
    }
    public void testReasonWhyInvalidTransactionAutoWrap3() {
        helpTestReasonWhyInvalid("TransactionAutoWrap", TeiidDataSource.TXN_WRAP_AUTO, VALID); //$NON-NLS-1$
    }
    public void testReasonWhyInvalidTransactionAutoWrap5() {
        helpTestReasonWhyInvalid("TransactionAutoWrap", "off", INVALID);    // lowercase value //$NON-NLS-1$ //$NON-NLS-2$
    }
    public void testReasonWhyInvalidTransactionAutoWrap6() {
        helpTestReasonWhyInvalid("TransactionAutoWrap", "Invalid AutoWrap", INVALID); //$NON-NLS-1$ //$NON-NLS-2$
    }

    public void testreasonWhyInvalidPartialResultsMode1() {
        helpTestReasonWhyInvalid("partialResultsMode", "Invalid partial mode", INVALID); //$NON-NLS-1$ //$NON-NLS-2$
    }
    public void testreasonWhyInvalidPartialResultsMode2() {
        helpTestReasonWhyInvalid("partialResultsMode", "true", VALID); //$NON-NLS-1$ //$NON-NLS-2$
    }

    public void testReasonWhyInvalidSocketsPerVM1() {
        helpTestReasonWhyInvalid("socketsPerVM", null, VALID); //$NON-NLS-1$
    }
    public void testReasonWhyInvalidSocketsPerVM2() {
        helpTestReasonWhyInvalid("socketsPerVM", "4", VALID); //$NON-NLS-1$ //$NON-NLS-2$
    }
    public void testReasonWhyInvalidSocketsPerVM3() {
        helpTestReasonWhyInvalid("socketsPerVM", "-3", INVALID); //$NON-NLS-1$ //$NON-NLS-2$
    }
    public void testReasonWhyInvalidSocketsPerVM4() {
        helpTestReasonWhyInvalid("socketsPerVM", "5.6", INVALID); //$NON-NLS-1$ //$NON-NLS-2$
    }

    public void testReasonWhyInvalidStickyConnections1() {
        helpTestReasonWhyInvalid("stickyConnections", null, VALID); //$NON-NLS-1$
    }
    public void testReasonWhyInvalidStickyConnections2() {
        helpTestReasonWhyInvalid("stickyConnections", "false", VALID); //$NON-NLS-1$ //$NON-NLS-2$
    }
    public void testReasonWhyInvalidStickyConnections3() {
        helpTestReasonWhyInvalid("stickyConnections", "YES", INVALID); //$NON-NLS-1$ //$NON-NLS-2$
    }

    public void helpTestAlternateServer(String altServers, boolean valid) {
        this.dataSource.setAlternateServers(altServers);
        try {
            this.dataSource.buildURL();
            if (!valid) {
                fail("expected exception");
            }
        } catch (TeiidSQLException e) {
            if (valid) {
                throw new RuntimeException(e);
            }
        }
    }

    public void testReasonWhyInvalidAlternateServers1() {
        helpTestAlternateServer(null, VALID);
    }
    public void testReasonWhyInvalidAlternateServers2() {
        helpTestAlternateServer("", VALID); //$NON-NLS-1$
    }
    public void testReasonWhyInvalidAlternateServers3() {
        helpTestAlternateServer("server", VALID); //$NON-NLS-1$
    }
    public void testReasonWhyInvalidAlternateServers4() {
        helpTestAlternateServer("server:100", VALID); //$NON-NLS-1$
    }
    public void testReasonWhyInvalidAlternateServers5() {
        helpTestAlternateServer("server:port", INVALID); //$NON-NLS-1$
    }
    public void testReasonWhyInvalidAlternateServers6() {
        helpTestAlternateServer("server:100:1", INVALID); //$NON-NLS-1$
    }
    public void testReasonWhyInvalidAlternateServers7() {
        helpTestAlternateServer("server:100:abc", INVALID); //$NON-NLS-1$
    }
    public void testReasonWhyInvalidAlternateServers8() {
        helpTestAlternateServer("server:abc:100", INVALID); //$NON-NLS-1$
    }
    public void testReasonWhyInvalidAlternateServers9() {
        helpTestAlternateServer(":100", INVALID); //$NON-NLS-1$
    }
    public void testReasonWhyInvalidAlternateServers10() {
        helpTestAlternateServer(":abc", INVALID); //$NON-NLS-1$
    }
    public void testReasonWhyInvalidAlternateServers11() {
        helpTestAlternateServer("server1:100,server2", VALID); //$NON-NLS-1$
    }
    public void testReasonWhyInvalidAlternateServers12() {
        helpTestAlternateServer("server1:100,server2:101", VALID); //$NON-NLS-1$
    }
    public void testReasonWhyInvalidAlternateServers13() {
        helpTestAlternateServer("server1:100,", VALID); //$NON-NLS-1$
    }
    public void testReasonWhyInvalidAlternateServers14() {
        helpTestAlternateServer("server1:100,server2:abc", INVALID); //$NON-NLS-1$
    }
    public void testReasonWhyInvalidAlternateServers15() {
        helpTestAlternateServer("server1:100,server2:101:abc", INVALID); //$NON-NLS-1$
    }
    public void testReasonWhyInvalidAlternateServers16() {
        helpTestAlternateServer("server1,server2:100", VALID); //$NON-NLS-1$
    }
    public void testReasonWhyInvalidAlternateServers17() {
        helpTestAlternateServer("server1,server2", VALID); //$NON-NLS-1$
    }
    public void testReasonWhyInvalidAlternateServers18() {
        helpTestAlternateServer(",server2:100", INVALID); //$NON-NLS-1$
    }
    public void testReasonWhyInvalidAlternateServers19() {
        helpTestAlternateServer("server1,server2,server3,server4:500", VALID); //$NON-NLS-1$
    }




    // ----------------------------------------------------------------
    //                       Test building URLs
    // ----------------------------------------------------------------

    public void testBuildingURL1() {
        final String serverName = "hostName"; //$NON-NLS-1$
        final String vdbName = "vdbName"; //$NON-NLS-1$
        final String vdbVersion = "1.2.3"; //$NON-NLS-1$
        final int portNumber = 7001;
        final String transactionAutoWrap = null;
        final String partialMode = "true"; //$NON-NLS-1$
        final boolean secure = false;
        helpTestBuildingURL(vdbName,vdbVersion,serverName,portNumber,null,transactionAutoWrap, partialMode, 500, false, secure,
                            "jdbc:teiid:vdbName@mm://hostname:7001;fetchSize=500;ApplicationName=JDBC;VirtualDatabaseVersion=1.2.3;partialResultsMode=true;VirtualDatabaseName=vdbName"); //$NON-NLS-1$
    }

    public void testBuildingIPv6() {
        final String serverName = "3ffe:ffff:0100:f101::1"; //$NON-NLS-1$
        final String vdbName = "vdbName"; //$NON-NLS-1$
        final String vdbVersion = "1"; //$NON-NLS-1$
        final int portNumber = 7001;
        final String transactionAutoWrap = null;
        final String partialMode = "true"; //$NON-NLS-1$
        final boolean secure = false;
        helpTestBuildingURL(vdbName,vdbVersion,serverName,portNumber,null,transactionAutoWrap, partialMode, 500, false, secure,
                            "jdbc:teiid:vdbName@mm://[3ffe:ffff:0100:f101::1]:7001;fetchSize=500;ApplicationName=JDBC;VirtualDatabaseVersion=1;partialResultsMode=true;VirtualDatabaseName=vdbName"); //$NON-NLS-1$
    }

    public void testBuildingIPv6WithBrackets() {
        final String serverName = "[3ffe:ffff:0100:f101::1]"; //$NON-NLS-1$
        final String vdbName = "vdbName"; //$NON-NLS-1$
        final String vdbVersion = "1"; //$NON-NLS-1$
        final int portNumber = 7001;
        final String transactionAutoWrap = null;
        final String partialMode = "true"; //$NON-NLS-1$
        final boolean secure = false;
        helpTestBuildingURL(vdbName,vdbVersion,serverName,portNumber,null,transactionAutoWrap, partialMode, 500, false, secure,
                            "jdbc:teiid:vdbName@mm://[3ffe:ffff:0100:f101::1]:7001;fetchSize=500;ApplicationName=JDBC;VirtualDatabaseVersion=1;partialResultsMode=true;VirtualDatabaseName=vdbName"); //$NON-NLS-1$
    }

    public void testBuildingIPv6Alternate() {
        final String serverName = "3ffe:ffff:0100:f101::1"; //$NON-NLS-1$
        final String vdbName = "vdbName"; //$NON-NLS-1$
        final String vdbVersion = "1"; //$NON-NLS-1$
        final int portNumber = 7001;
        final String transactionAutoWrap = null;
        final String partialMode = "true"; //$NON-NLS-1$
        final boolean secure = false;
        final String alternates = "[::1],127.0.0.1:1234"; //$NON-NLS-1$
        helpTestBuildingURL(vdbName,vdbVersion,serverName,portNumber,alternates,transactionAutoWrap, partialMode, 500, false, secure,
                            "jdbc:teiid:vdbName@mm://[3ffe:ffff:0100:f101::1]:7001,[::1]:7001,127.0.0.1:1234;fetchSize=500;ApplicationName=JDBC;VirtualDatabaseVersion=1;partialResultsMode=true;VirtualDatabaseName=vdbName"); //$NON-NLS-1$
    }

    public void testBuildingURL2() {
        final String serverName = "hostName"; //$NON-NLS-1$
        final String vdbName = "vdbName"; //$NON-NLS-1$
        final String vdbVersion = ""; //$NON-NLS-1$
        final int portNumber = 7001;
        final String transactionAutoWrap = TeiidDataSource.TXN_WRAP_AUTO;
        final String partialMode = "false"; //$NON-NLS-1$
        final boolean secure = false;
        helpTestBuildingURL(vdbName,vdbVersion,serverName,portNumber,null,transactionAutoWrap, partialMode, -1, false, secure,
                            "jdbc:teiid:vdbName@mm://hostname:7001;ApplicationName=JDBC;partialResultsMode=false;autoCommitTxn=DETECT;VirtualDatabaseName=vdbName"); //$NON-NLS-1$
    }

    public void testBuildURL3() {
        final String serverName = "hostName"; //$NON-NLS-1$
        final String vdbName = "vdbName"; //$NON-NLS-1$
        final String vdbVersion = ""; //$NON-NLS-1$
        final int portNumber = 7001;
        final String transactionAutoWrap = TeiidDataSource.TXN_WRAP_AUTO;
        final String partialMode = "false"; //$NON-NLS-1$
        final boolean secure = false;
        helpTestBuildingURL(vdbName,vdbVersion,serverName,portNumber,null,transactionAutoWrap, partialMode, -1, true, secure,
                            "jdbc:teiid:vdbName@mm://hostname:7001;ApplicationName=JDBC;SHOWPLAN=ON;partialResultsMode=false;autoCommitTxn=DETECT;VirtualDatabaseName=vdbName"); //$NON-NLS-1$
    }

    // Test secure protocol
    public void testBuildURL4() {
        final String serverName = "hostName"; //$NON-NLS-1$
        final String vdbName = "vdbName"; //$NON-NLS-1$
        final String vdbVersion = ""; //$NON-NLS-1$
        final int portNumber = 7001;
        final String transactionAutoWrap = TeiidDataSource.TXN_WRAP_AUTO;
        final String partialMode = "false"; //$NON-NLS-1$
        final boolean secure = true;
        helpTestBuildingURL(vdbName,vdbVersion,serverName,portNumber,null,transactionAutoWrap, partialMode, -1, true, secure,
                            "jdbc:teiid:vdbName@mms://hostname:7001;ApplicationName=JDBC;SHOWPLAN=ON;partialResultsMode=false;autoCommitTxn=DETECT;VirtualDatabaseName=vdbName"); //$NON-NLS-1$
    }

    /*
     * Test alternate servers list
     *
     * Server list uses server:port pairs
     */
    public void testBuildURL5() {
        final String serverName = "hostName"; //$NON-NLS-1$
        final String vdbName = "vdbName"; //$NON-NLS-1$
        final String vdbVersion = ""; //$NON-NLS-1$
        final int portNumber = 7001;
        final String alternateServers = "hostName:7002,hostName2:7001,hostName2:7002"; //$NON-NLS-1$
        final String transactionAutoWrap = TeiidDataSource.TXN_WRAP_AUTO;
        final String partialMode = "false"; //$NON-NLS-1$
        final boolean secure = false;
        helpTestBuildingURL(vdbName,vdbVersion,serverName,portNumber,alternateServers,transactionAutoWrap, partialMode, -1, true, secure,
                            "jdbc:teiid:vdbName@mm://hostName:7001,hostName:7002,hostName2:7001,hostName2:7002;ApplicationName=JDBC;SHOWPLAN=ON;partialResultsMode=false;autoCommitTxn=DETECT;VirtualDatabaseName=vdbName"); //$NON-NLS-1$
    }

    /*
     * Test alternate servers list
     *
     * Server list uses server:port pairs and we set secure to true
     */
    public void testBuildURL6() {
        final String serverName = "hostName"; //$NON-NLS-1$
        final String vdbName = "vdbName"; //$NON-NLS-1$
        final String vdbVersion = ""; //$NON-NLS-1$
        final int portNumber = 7001;
        final String alternateServers = "hostName:7002,hostName2:7001,hostName2:7002"; //$NON-NLS-1$
        final String transactionAutoWrap = TeiidDataSource.TXN_WRAP_AUTO;
        final String partialMode = "false"; //$NON-NLS-1$
        final boolean secure = true;
        helpTestBuildingURL(vdbName,vdbVersion,serverName,portNumber,alternateServers,transactionAutoWrap, partialMode, -1, true, secure,
                            "jdbc:teiid:vdbName@mms://hostName:7001,hostName:7002,hostName2:7001,hostName2:7002;ApplicationName=JDBC;SHOWPLAN=ON;partialResultsMode=false;autoCommitTxn=DETECT;VirtualDatabaseName=vdbName"); //$NON-NLS-1$
    }

    /*
     * Test alternate servers list
     *
     * Server list uses server:port pairs and server with no port
     * In this case, the server with no port should default to ds.portNumber.
     */
    public void testBuildURL7() {
        final String serverName = "hostName"; //$NON-NLS-1$
        final String vdbName = "vdbName"; //$NON-NLS-1$
        final String vdbVersion = ""; //$NON-NLS-1$
        final int portNumber = 7001;
        final String alternateServers = "hostName:7002,hostName2,hostName2:7002"; //$NON-NLS-1$
        final String transactionAutoWrap = TeiidDataSource.TXN_WRAP_AUTO;
        final String partialMode = "false"; //$NON-NLS-1$
        final boolean secure = false;
        helpTestBuildingURL(vdbName,vdbVersion,serverName,portNumber,alternateServers,transactionAutoWrap, partialMode, -1, true, secure,
                            "jdbc:teiid:vdbName@mm://hostName:7001,hostName:7002,hostName2:7001,hostName2:7002;ApplicationName=JDBC;SHOWPLAN=ON;partialResultsMode=false;autoCommitTxn=DETECT;VirtualDatabaseName=vdbName"); //$NON-NLS-1$
    }

    /**
     * Test turning off using JDBC4 semantics
     */
    public void testBuildURL8() {
        final String serverName = "hostName"; //$NON-NLS-1$
        final String vdbName = "vdbName"; //$NON-NLS-1$
        final String vdbVersion = "1.2.3"; //$NON-NLS-1$
        final int portNumber = 7001;
        final String transactionAutoWrap = null;
        final String partialMode = "true"; //$NON-NLS-1$
        final boolean secure = false;
        helpTestBuildingURL2(vdbName,vdbVersion,serverName,portNumber,null,transactionAutoWrap, partialMode, 500, false, secure, false,
                            "jdbc:teiid:vdbName@mm://hostname:7001;fetchSize=500;ApplicationName=JDBC;VirtualDatabaseVersion=1.2.3;partialResultsMode=true;useJDBC4ColumnNameAndLabelSemantics=false;VirtualDatabaseName=vdbName"); //$NON-NLS-1$
    }

    public void testBuildURL_AdditionalProperties() throws TeiidSQLException {
        final TeiidDataSource ds = new TeiidDataSource();
        ds.setAdditionalProperties("foo=bar;a=b"); //$NON-NLS-1$
        ds.setServerName("hostName"); //$NON-NLS-1$
        ds.setDatabaseName("vdbName"); //$NON-NLS-1$
        ds.setPortNumber(1);
        assertEquals("jdbc:teiid:vdbName@mm://hostname:1;ApplicationName=JDBC;VirtualDatabaseName=vdbName;a=b;fetchSize=2048;foo=bar", ds.buildURL().getJDBCURL()); //$NON-NLS-1$
    }

    public void testBuildURLEncryptRequests() throws TeiidSQLException {
        final TeiidDataSource ds = new TeiidDataSource();
        ds.setServerName("hostName"); //$NON-NLS-1$
        ds.setDatabaseName("vdbName"); //$NON-NLS-1$
        ds.setEncryptRequests(true);
        compareUrls("jdbc:teiid:vdbName@mm://hostname:0;fetchSize=2048;ApplicationName=JDBC;encryptRequests=true;VirtualDatabaseName=vdbName", ds.buildURL().getJDBCURL()); //$NON-NLS-1$
    }

    public void testInvalidDataSource() {
        final String serverName = "hostName"; //$NON-NLS-1$
        final String vdbName = "vdbName"; //$NON-NLS-1$
        final String vdbVersion = ""; //$NON-NLS-1$
        final int portNumber = -1;              // this is what is invalid
        final String dataSourceName = null;
        final String transactionAutoWrap = null;
        final String configFile = UnitTestUtil.getTestDataPath() + "/config.txt"; //$NON-NLS-1$
        try {
            helpTestConnection(vdbName,vdbVersion,serverName,portNumber, null, null, null, dataSourceName,transactionAutoWrap,
                "false", configFile);       // TRUE TO OVERRIDE USERNAME & PASSWORD //$NON-NLS-1$
            fail("Unexpectedly able to connect"); //$NON-NLS-1$
        } catch ( SQLException e) {
            // this is expected!
        }
    }

    /*
     * Test invalid alternateServer list
     *
     * Server list uses a non numeric value for port.
     */
    public void testInvalidDataSource2() {
        final String serverName = "hostName"; //$NON-NLS-1$
        final String vdbName = "vdbName"; //$NON-NLS-1$
        final String vdbVersion = ""; //$NON-NLS-1$
        final int portNumber = 31000;
        final String alternateServers = "hostName:-1"; // this is what is invalid //$NON-NLS-1$
        final String dataSourceName = null;
        final String transactionAutoWrap = null;
        final String configFile = UnitTestUtil.getTestDataPath() + "/config.txt"; //$NON-NLS-1$
        try {
            helpTestConnection(vdbName, vdbVersion, serverName, portNumber,
                    alternateServers, null, null, dataSourceName, transactionAutoWrap, "false", configFile);     //$NON-NLS-1$  // TRUE TO OVERRIDE USERNAME & PASSWORD
            fail("Unexpectedly able to connect"); //$NON-NLS-1$
        } catch ( SQLException e) {
            // this is expected!
        }
    }

    public void testUrlEncodedProperties() throws SQLException {
        TeiidDriver td = Mockito.mock(TeiidDriver.class);
        TeiidDataSource tds = new TeiidDataSource(td);
        tds.setDatabaseName("y");
        tds.setUser("%25user");
        tds.setServerName("x");
        tds.getConnection();

        ArgumentCaptor<Properties> argument = ArgumentCaptor.forClass(Properties.class);
        Mockito.verify(td).connect(Mockito.eq("jdbc:teiid:y@mm://x:0"), argument.capture());
        Properties p = argument.getValue();
        assertEquals("%25user", p.getProperty(BaseDataSource.USER_NAME));
    }

    public void testLoginTimeout() throws SQLException {
        TeiidDriver td = Mockito.mock(TeiidDriver.class);
        TeiidDataSource tds = new TeiidDataSource(td);
        tds.setDatabaseName("y");
        tds.setServerName("x");
        tds.setLoginTimeout(2);
        tds.getConnection();

        ArgumentCaptor<Properties> argument = ArgumentCaptor.forClass(Properties.class);
        Mockito.verify(td).connect(Mockito.eq("jdbc:teiid:y@mm://x:0"), argument.capture());
        Properties p = argument.getValue();
        assertEquals("2", p.getProperty(TeiidURL.CONNECTION.LOGIN_TIMEOUT));
    }

    public void testGetConnectionWithUser() throws SQLException {
        TeiidDriver td = Mockito.mock(TeiidDriver.class);
        TeiidDataSource tds = new TeiidDataSource(td);
        tds.setDatabaseName("y");
        tds.setUser("%25user");
        tds.setServerName("x");
        tds.getConnection("user", "password");

        ArgumentCaptor<Properties> argument = ArgumentCaptor.forClass(Properties.class);
        Mockito.verify(td).connect(Mockito.eq("jdbc:teiid:y@mm://x:0"), argument.capture());
        Properties p = argument.getValue();
        assertEquals("user", p.getProperty(BaseDataSource.USER_NAME));
    }

    public void testKerberos() throws SQLException {
        TeiidDataSource tds = new TeiidDataSource();
        tds.setDatabaseName("y");
        tds.setUser("%25user");
        tds.setJaasName("x");
        tds.setKerberosServicePrincipleName("z");
        tds.setServerName("t");
        compareUrls("jdbc:teiid:y@mm://t:0;fetchSize=2048;ApplicationName=JDBC;user=%2525user;jaasName=x;VirtualDatabaseName=y;kerberosServicePrincipleName=z", tds.buildURL().getJDBCURL());

    }

}