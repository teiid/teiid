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

package com.metamatrix.common.jdbc;

import junit.framework.TestCase;


/** 
 * @since 4.3
 */
public class TestJDBCURL extends TestCase {
    
    /**
     * Constructor for JDBCUnitTestUtilTest.
     * @param arg0
     */
    public TestJDBCURL(String arg0) {
        super(arg0);
    }

    public void testNativeOracle()  throws Exception{
    

        //"jdbc:oracle:thin:@slntds04:1521:ds04"
        
        String p2 = "oracle:thin";//$NON-NLS-1$
        String d2 = "@slntds04:1521:ds0";//$NON-NLS-1$
         
        String url2 = "jdbc:" + p2 + d2;//$NON-NLS-1$
        JDBCURL j2 = new JDBCURL(url2);
        if (!p2.equalsIgnoreCase(j2.getProtocol())) {
            fail("Protocol " + p2 + " does not equal " + j2.getProtocol());//$NON-NLS-1$ //$NON-NLS-2$
        }
        
        if (!d2.endsWith(j2.getDataConnectionInfo())) {
            fail("DatabaseInfo " + d2+ " does not equal " + j2.getDataConnectionInfo());//$NON-NLS-1$ //$NON-NLS-2$
        } 
    }
    
    // this the metamatrix branded versions from datadirect
    public void testMMOracle() throws Exception {
        String p1 = "mmx:oracle";//$NON-NLS-1$
        String d1 = "//slntdb01:1521;SID=db01";//$NON-NLS-1$
         
        String url1 = "jdbc:" + p1 + d1;//$NON-NLS-1$
        JDBCURL j1 = new JDBCURL(url1);
        if (!p1.equalsIgnoreCase(j1.getProtocol())) {
            fail("Protocol " + p1 + " does not equal " + j1.getProtocol());//$NON-NLS-1$ //$NON-NLS-2$
        }
        
        if (!d1.endsWith(j1.getDataConnectionInfo())) {
            fail("DatabaseInfo " + d1+ " does not equal " + j1.getDataConnectionInfo());//$NON-NLS-1$ //$NON-NLS-2$
        }
        
        
    }

                
    public void testWeblogicSQLServer() throws Exception {
                 
        String p3 = "weblogic:mssqlserver4";//$NON-NLS-1$
        String d3 = "//slntdb01:1521;database=gg_vhalbert";//$NON-NLS-1$
         
        String url3 = "jdbc:" + p3 + d3;//$NON-NLS-1$
        JDBCURL j3 = new JDBCURL(url3);
        if (!p3.equalsIgnoreCase(j3.getProtocol())) {
            fail("Protocol " + p3 + " does not equal " + j3.getProtocol());//$NON-NLS-1$ //$NON-NLS-2$
        }
        
        if (!d3.endsWith(j3.getDataConnectionInfo())) {
            fail("DatabaseInfo " + d3+ " does not equal " + j3.getDataConnectionInfo());//$NON-NLS-1$ //$NON-NLS-2$
        }  
    }
    
    
    public void testMetaMatrixJDBC() throws Exception {
        
            String p4 = "metamatrix:QT_Ora9DS";//$NON-NLS-1$
            String d4 = "@slntdb01:1521;";//$NON-NLS-1$
             
            String url4 = "jdbc:" + p4 + d4;//$NON-NLS-1$
            JDBCURL j4 = new JDBCURL(url4);
            if (!p4.equalsIgnoreCase(j4.getProtocol())) {
                fail("Protocol " + p4 + " does not equal " + j4.getProtocol());//$NON-NLS-1$ //$NON-NLS-2$
            }
            
            if (!d4.endsWith(j4.getDataConnectionInfo())) {
                fail("DatabaseInfo " + d4+ " does not equal " + j4.getDataConnectionInfo());//$NON-NLS-1$ //$NON-NLS-2$
            }  
    
        }    
    

}
