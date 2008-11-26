/*
 * Copyright � 2000-2005 MetaMatrix, Inc.  All rights reserved.
 */
package com.metamatrix.jdbc;

import java.net.InetAddress;
import java.sql.DriverPropertyInfo;
import java.sql.SQLException;
import java.util.Properties;

import junit.framework.TestCase;

import com.metamatrix.common.api.MMURL_Properties;
import com.metamatrix.jdbc.api.ExecutionProperties;

public class TestMMDriver extends TestCase {
    MMDriver drv = new MMDriver();
    public String localhost = "localhost"; //$NON-NLS-1$
    
    public TestMMDriver(String name)    {
        super(name);
        
    }
    
    protected void setUp() throws Exception {
        localhost =  InetAddress.getLocalHost().getHostName();
    }

    /** Valid format of urls*/
    public void testAcceptsURL1()  throws Exception   {
        assertNotNull(drv);
        assertTrue(drv.acceptsURL("jdbc:metamatrix:jvdb@mm://localhost:1234")); //$NON-NLS-1$
        assertTrue(drv.acceptsURL("jdbc:metamatrix:vdb@mm://localhost:1234;version=x")); //$NON-NLS-1$
        assertTrue(drv.acceptsURL("jdbc:metamatrix:vdb@mm://localhost:1234")); //$NON-NLS-1$
        assertTrue(drv.acceptsURL("jdbc:metamatrix:vdb@mm://localhost:1234,localhost2:12342,localhost3:12343")); //$NON-NLS-1$       
        assertTrue(drv.acceptsURL("jdbc:metamatrix:vdb@mm://localhost:1234;logFile=D:\\metamatrix\\work\\DQP\\log\\jdbcLogFile.log")); //$NON-NLS-1$
        assertTrue(drv.acceptsURL("jdbc:metamatrix:vdb@mm://localhost:1234,localhost2:12342,localhost3:12343;logFile=D:\\metamatrix\\work\\DQP\\log\\jdbcLogFile.log")); //$NON-NLS-1$
        assertTrue(drv.acceptsURL("jdbc:metamatrix:vdb@mms://localhost:1234;logLevel=1;logFile=D:\\metamatrix\\work\\DQP\\log\\jdbcLogFile.log")); //$NON-NLS-1$
        assertTrue(!drv.acceptsURL("jdbc:metamatrix:@mm://localhost:1234;logLevel=2;logFile=D:\\metamatrix\\work\\DQP\\log\\jdbcLogFile.log")); //$NON-NLS-1$
        assertTrue(drv.acceptsURL("jdbc:metamatrix:vdb@mm://localhost:1234;logLevel=2;logFile=D:\\metamatrix\\work\\DQP\\log\\jdbcLogFile.log;txnAutoWrap=OFF;paritalResultsMode=true")); //$NON-NLS-1$
        assertTrue(drv.acceptsURL("jdbc:metamatrix:jvdb@mms://localhost:1234")); //$NON-NLS-1$
        assertTrue(drv.acceptsURL("jdbc:metamatrix:vdb@mm://localhost:1234;version=x")); //$NON-NLS-1$
        assertTrue(drv.acceptsURL("jdbc:metamatrix:vdb@mm://localhost:1234,localhost2:12342,localhost3:12343")); //$NON-NLS-1$       
        assertTrue(drv.acceptsURL("jdbc:metamatrix:vdb@mms://localhost:1234,localhost2:12342,localhost3:12343;logFile=D:\\metamatrix\\work\\DQP\\log\\jdbcLogFile.log")); //$NON-NLS-1$
        assertTrue(drv.acceptsURL("jdbc:metamatrix:vdb@mms://localhost:1234;logLevel=2;logFile=D:\\metamatrix\\work\\DQP\\log\\jdbcLogFile.log;txnAutoWrap=OFF;paritalResultsMode=true")); //$NON-NLS-1$
        assertTrue(drv.acceptsURL("jdbc:metamatrix:vdb@mm://127.0.0.1:1234;logLevel=2")); //$NON-NLS-1$
        assertTrue(drv.acceptsURL("jdbc:metamatrix:vdb@mms://127.0.0.1:1234")); //$NON-NLS-1$
        assertTrue(drv.acceptsURL("jdbc:metamatrix:vdb@mm://127.0.0.1:1234,localhost.mydomain.com:63636;logLevel=2")); //$NON-NLS-1$
        assertTrue(drv.acceptsURL("jdbc:metamatrix:vdb@mm://my-host.mydomain.com:53535,127.0.0.1:1234")); //$NON-NLS-1$
        assertTrue(drv.acceptsURL("jdbc:metamatrix:vdb@mm://123.123.123.123:53535,127.0.0.1:1234")); //$NON-NLS-1$

        //DQP type
        assertTrue(!drv.acceptsURL("jdbc:metamatrix:jvdb@c:/dqp.properties;version=1")); //$NON-NLS-1$
        assertTrue(!drv.acceptsURL("jdbc:metamatrix:jvdb@/foo/dqp.properties;version=1")); //$NON-NLS-1$
        assertTrue(!drv.acceptsURL("jdbc:metamatrix:jvdb@../foo/dqp.properties;version=1")); //$NON-NLS-1$
        
        assertTrue(!drv.acceptsURL("jdbc:metamatrix:jvdb@mm://localhost:port")); //$NON-NLS-1$
        assertTrue(!drv.acceptsURL("jdbc:metamatrix:vdb@localhost:port;version=x")); //$NON-NLS-1$
        assertTrue(!drv.acceptsURL("jdbc:metamatrix:@localhost:1234")); //$NON-NLS-1$
        assertTrue(!drv.acceptsURL("jdbc:metamatrix:@localhost:1234,localhost2:12342,localhost3:12343")); //$NON-NLS-1$       
        assertTrue(!drv.acceptsURL("jdbc:metamatrix:@localhost:1234;logFile=D:\\metamatrix\\work\\DQP\\log\\jdbcLogFile.log")); //$NON-NLS-1$
        assertTrue(!drv.acceptsURL("jdbc:metamatrix:@localhost:1234,localhost2:12342,localhost3:12343;logFile=D:\\metamatrix\\work\\DQP\\log\\jdbcLogFile.log")); //$NON-NLS-1$
        assertTrue(!drv.acceptsURL("jdbc:metamatrix:@localhost:1234;logLevel=1;logFile=D:\\metamatrix\\work\\DQP\\log\\jdbcLogFile.log")); //$NON-NLS-1$
        assertTrue(!drv.acceptsURL("jdbc:metamatrix:@localhost:1234;logLevel=2;logFile=D:\\metamatrix\\work\\DQP\\log\\jdbcLogFile.log")); //$NON-NLS-1$
        assertTrue(!drv.acceptsURL("jdbc:metamatrix:@localhost:1234;logLevel=2;logFile=D:\\metamatrix\\work\\DQP\\log\\jdbcLogFile.log;txnAutoWrap=OFF;paritalResultsMode=true")); //$NON-NLS-1$
        assertTrue(!drv.acceptsURL("jdbc:metamatrix:@localhost:1234;stickyConnections=false;socketsPerVM=4")); //$NON-NLS-1$
        assertTrue(!drv.acceptsURL("jdbc:metamatrix:vdb@mm://my_host.mydomain.com:53535,127.0.0.1:1234")); //$NON-NLS-1$        
    }

    /** Invalid format of urls*/
    public void testAcceptsURL2() throws Exception    {
        assertNotNull(drv);
        assertTrue(!drv.acceptsURL("jdbc:matamatrix:test")); //$NON-NLS-1$
        assertTrue(!drv.acceptsURL("metamatrix:test")); //$NON-NLS-1$
        assertTrue(!drv.acceptsURL("jdbc&matamatrix:test")); //$NON-NLS-1$
        assertTrue(!drv.acceptsURL("jdbc;metamatrix:test")); //$NON-NLS-1$
    }   

    public void testParseURL() throws SQLException{
        Properties p = new Properties();
        MMDriver.parseURL("jdbc:metamatrix:BQT@mm://slwxp157:1234", p); //$NON-NLS-1$
        assertTrue(p.getProperty(BaseDataSource.VDB_NAME).equals("BQT")); //$NON-NLS-1$
        assertTrue(p.getProperty(MMURL_Properties.SERVER.SERVER_URL).equals("mm://slwxp157:1234")); //$NON-NLS-1$
        assertEquals(2, p.size());        
    }

    public void testParseURL2() throws SQLException {
        Properties p = new Properties();       
        MMDriver.parseURL("jdbc:metamatrix:BQT@mms://slwxp157:1234;version=3", p); //$NON-NLS-1$
        assertTrue(p.getProperty(BaseDataSource.VDB_NAME).equals("BQT")); //$NON-NLS-1$
        assertTrue(p.getProperty(BaseDataSource.VDB_VERSION).equals("3")); //$NON-NLS-1$
        assertTrue(p.getProperty(MMURL_Properties.SERVER.SERVER_URL).equals("mms://slwxp157:1234")); //$NON-NLS-1$
        assertTrue(p.getProperty(BaseDataSource.VERSION).equals("3")); //$NON-NLS-1$
        assertEquals(4, p.size());
    }
    
    public void testParseURL3() throws SQLException{
        Properties p = new Properties();
        MMDriver.parseURL("jdbc:metamatrix:BQT@mm://slwxp157:1234,slntmm01:43401,sluxmm09:43302;version=4;txnAutoWrap=ON;partialResultsMode=YES;logFile=jdbcLogFile.log", p); //$NON-NLS-1$
        assertTrue(p.getProperty(BaseDataSource.VDB_NAME).equals("BQT")); //$NON-NLS-1$
        assertTrue(p.getProperty(BaseDataSource.VDB_VERSION).equals("4"));         //$NON-NLS-1$
        assertTrue(p.getProperty(ExecutionProperties.PROP_TXN_AUTO_WRAP).equals("ON")); //$NON-NLS-1$
        assertTrue(p.getProperty(ExecutionProperties.PROP_PARTIAL_RESULTS_MODE).equals("YES")); //$NON-NLS-1$
        assertTrue(p.getProperty(BaseDataSource.LOG_FILE).equals("jdbcLogFile.log")); //$NON-NLS-1$
        assertTrue(p.getProperty(MMURL_Properties.SERVER.SERVER_URL).equals("mm://slwxp157:1234,slntmm01:43401,sluxmm09:43302")); //$NON-NLS-1$
        assertTrue(p.getProperty(BaseDataSource.VERSION).equals("4")); //$NON-NLS-1$
        assertEquals(7, p.size());        
    }    
    
    /**should prompt for VirtualDatabaseVersion, logFile and logLevel */
    public void testGetPropertyInfo1() throws Exception {        
        DriverPropertyInfo info[] = drv.getPropertyInfo("jdbc:metamatrix:vdb@mm://localhost:12345", null); //$NON-NLS-1$

        assertNotNull(info);
        assertEquals(13, info.length);
        for(int i=0; i< info.length; i++) {
            DriverPropertyInfo propInfo = info[i];
            assertTrue(propInfo.name == "VirtualDatabaseVersion" || propInfo.name == "user" || propInfo.name == "password"  //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
                || propInfo.name == "logFile" || propInfo.name == "logLevel" //$NON-NLS-1$ //$NON-NLS-2$
                || propInfo.name == "txnAutoWrap" ||  propInfo.name == "partialResultsMode"  //$NON-NLS-1$ //$NON-NLS-2$ 
                || propInfo.name == "clientToken" || propInfo.name == "resultSetCacheMode"  //$NON-NLS-1$ //$NON-NLS-2$
                || propInfo.name == "socketsPerVM" || propInfo.name == "stickyConnections" //$NON-NLS-1$ //$NON-NLS-2$
                || propInfo.name == "allowDoubleQuotedVariable" || propInfo.name == "sqlOptions" //$NON-NLS-1$ //$NON-NLS-2$
            	|| propInfo.name == "disableLocalTxn" || propInfo.name == "autoFailover"); //$NON-NLS-1$ //$NON-NLS-2$
        }
    }
    
    /** prompt for logFile and logLevel */
    public void testGetPropertyInfo2() throws Exception {
        DriverPropertyInfo info[] = drv.getPropertyInfo("jdbc:metamatrix:vdb@mm://localhost:12345;version=1", null); //$NON-NLS-1$

        assertNotNull(info);
        assertEquals(12, info.length);
        for(int i=0; i< info.length; i++) {
            DriverPropertyInfo propInfo = info[i];
            assertTrue( propInfo.name == "user" || propInfo.name == "password" || propInfo.name == "logFile" || propInfo.name == "logLevel" //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$ //$NON-NLS-4$
                || propInfo.name == "txnAutoWrap" ||  propInfo.name == "partialResultsMode" //$NON-NLS-1$ //$NON-NLS-2$ 
                || propInfo.name == "clientToken" || propInfo.name == "resultSetCacheMode" //$NON-NLS-1$ //$NON-NLS-2$
                || propInfo.name == "socketsPerVM" || propInfo.name == "stickyConnections" //$NON-NLS-1$ //$NON-NLS-2$
                || propInfo.name == "allowDoubleQuotedVariable" || propInfo.name == "sqlOptions" //$NON-NLS-1$ //$NON-NLS-2$
            	|| propInfo.name == "disableLocalTxn" || propInfo.name == "autoFailover"); //$NON-NLS-1$ //$NON-NLS-2$            
        }
    }      
    
    /** prompt for logFile and logLevel */
    public void testGetPropertyInfo3() throws Exception {
        DriverPropertyInfo info[] = drv.getPropertyInfo("jdbc:metamatrix:vdb@mm://localhost:12345,localhost:23456;version=1", null); //$NON-NLS-1$

        assertNotNull(info);
        assertEquals(12, info.length);
        for(int i=0; i< info.length; i++) {
            DriverPropertyInfo propInfo = info[i];
            assertTrue( propInfo.name == "user" || propInfo.name == "password" || propInfo.name == "logFile" || propInfo.name == "logLevel" //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$ //$NON-NLS-4$
                || propInfo.name == "txnAutoWrap" ||  propInfo.name == "partialResultsMode" //$NON-NLS-1$ //$NON-NLS-2$ 
                || propInfo.name == "clientToken" || propInfo.name == "resultSetCacheMode" //$NON-NLS-1$ //$NON-NLS-2$
                || propInfo.name == "socketsPerVM" || propInfo.name == "stickyConnections" //$NON-NLS-1$ //$NON-NLS-2$
                || propInfo.name == "allowDoubleQuotedVariable" || propInfo.name == "sqlOptions" //$NON-NLS-1$ //$NON-NLS-2$
               	|| propInfo.name == "disableLocalTxn" || propInfo.name == "autoFailover"); //$NON-NLS-1$ //$NON-NLS-2$                	
        }
    }    

}
