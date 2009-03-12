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

package org.teiid.connector.jdbc;

import java.util.Properties;

import static org.junit.Assert.*;
import org.junit.Test;
import org.teiid.connector.api.ConnectorCapabilities;
import org.teiid.connector.api.ConnectorException;
import org.teiid.connector.jdbc.translator.Translator;

import com.metamatrix.cdk.api.EnvironmentUtility;


public class TestJDBCConnector {

    public void helpTestMaxIn(int setting, int expected) throws Exception {
        Properties connProps = new Properties();
        connProps.setProperty(JDBCPropertyNames.SET_CRITERIA_BATCH_SIZE, String.valueOf(setting)); 
        connProps.setProperty(JDBCPropertyNames.EXT_CAPABILITY_CLASS, SimpleCapabilities.class.getName());
        Translator t = new Translator();
        t.initialize(EnvironmentUtility.createEnvironment(connProps));
        ConnectorCapabilities caps = t.getConnectorCapabilities();
        int maxIn = caps.getMaxInCriteriaSize();
        assertEquals(expected, maxIn);
    }

    @Test
    public void test1() throws Exception {
        helpTestMaxIn(-1, 250);
    }

    @Test
    public void test2() throws Exception {
        helpTestMaxIn(0, 250);
    }

    @Test
    public void test3() throws Exception {
        helpTestMaxIn(1, 1);
    }	
    
    @Test
    public void testParseUrl() throws ConnectorException {
    	String urlWithEmptyProp = "jdbc:mmx:db2://aHost:aPort;DatabaseName=DB2_DataBase;CollectionID=aCollectionID;PackageName=aPackageName;BogusProp=aBogusProp;UnEmptyProp=;"; //$NON-NLS-1$
    	Properties props = new Properties();
    	JDBCConnector.parseURL(urlWithEmptyProp, props);
    	
    	assertEquals("aPort", props.getProperty(XAJDBCPropertyNames.PORT_NUMBER)); //$NON-NLS-1$
    	assertEquals("aHost", props.getProperty(XAJDBCPropertyNames.SERVER_NAME)); //$NON-NLS-1$
    	assertEquals("XADS_aHost_null", props.getProperty(XAJDBCPropertyNames.DATASOURCE_NAME)); //$NON-NLS-1$
    	assertEquals("aBogusProp", props.getProperty("bogusprop")); //$NON-NLS-1$ //$NON-NLS-2$
    	assertNull(props.getProperty("unemptyprop")); //$NON-NLS-1$
    }
    
}
