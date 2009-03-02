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

package com.metamatrix.connector.loopback;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.List;
import java.util.Properties;

import org.teiid.connector.api.ConnectorException;

import junit.framework.TestCase;

import com.metamatrix.cdk.api.ConnectorHost;
import com.metamatrix.cdk.api.TranslationUtility;
import com.metamatrix.cdk.unittest.FakeTranslationFactory;

public class TestLoopbackExecution extends TestCase {

    public TestLoopbackExecution(String name) {
        super(name);
    }

    public Properties exampleProperties(int waitTime, int rowCount) {
        Properties props = new Properties();
        props.setProperty(LoopbackProperties.WAIT_TIME, String.valueOf(waitTime)); 
        props.setProperty(LoopbackProperties.ROW_COUNT, String.valueOf(rowCount));
        return props;
    }
    
    public void helpTestQuery(String sql, TranslationUtility metadata, Object[][] expectedResults) throws ConnectorException {
        helpTestQuery(sql, metadata, 0, 1, expectedResults);
    }

    public void helpTestQuery(String sql, TranslationUtility metadata, int waitTime, int rowCount, Object[][] expectedResults) throws ConnectorException {
    	ConnectorHost host = new ConnectorHost(new LoopbackConnector(), exampleProperties(waitTime, rowCount), metadata, false);
                              
    	List actualResults = host.executeCommand(sql);
       
        // Compare actual and expected results
        assertEquals("Did not get expected number of rows", expectedResults.length, actualResults.size()); //$NON-NLS-1$
        
        if(expectedResults.length > 0) {
            // Compare column sizes
            assertEquals("Did not get expected number of columns", expectedResults[0].length, ((List)actualResults.get(0)).size()); //$NON-NLS-1$

            // Compare results
            for(int r=0; r<expectedResults.length; r++) {
                Object[] expectedRow = expectedResults[r];
                List actualRow = (List) actualResults.get(r);
                
                for(int c=0; c<expectedRow.length; c++) {
                    Object expectedValue = expectedRow[c];
                    Object actualValue = actualRow.get(c);
                    
                    if(expectedValue == null) {
                        if(actualValue != null) {
                            fail("Row " + r + ", Col " + c + ": Expected null but got " + actualValue + " of type " + actualValue.getClass().getName()); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$ //$NON-NLS-4$
                        }
                    } else if(actualValue == null) {
                        fail("Row " + r + ", Col " + c + ": Expected " + expectedValue + " but got null"); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$ //$NON-NLS-4$
                    } else {
                        assertEquals("Row " + r + ", Col " + c + ": Expected " + expectedValue + " but got " + actualValue, expectedValue, actualValue); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$ //$NON-NLS-4$
                    }
                }
            }
        }      
    }
    
    public void testSimple() throws Exception {
        Object[][] results = new Object[][] {
            new Object[] { new Integer(0) }  
        };
        helpTestQuery("SELECT intkey FROM BQT1.SmallA", FakeTranslationFactory.getInstance().getBQTTranslationUtility(), results);     //$NON-NLS-1$
    }
    
    public void testMostTypes() throws Exception {
        Object[][] results = new Object[][] {
            new Object[] { new Integer(0), "ABCDEFGHIJ", new Float(0), new Long(0), new Double(0), new Byte((byte)0), //$NON-NLS-1$
                LoopbackExecution.SQL_DATE_VAL, LoopbackExecution.TIME_VAL, 
                LoopbackExecution.TIMESTAMP_VAL, Boolean.FALSE, 
                new BigInteger("0"), new BigDecimal("0"), "ABCDEFGHIJ", //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
                new Short((short)0), new Character('c')
                 }  
        };
        
        helpTestQuery("SELECT intkey, StringKey, floatnum, longnum, doublenum, bytenum, " + //$NON-NLS-1$
            "datevalue, timevalue, timestampvalue, booleanvalue, bigintegervalue, bigdecimalvalue, " + //$NON-NLS-1$
            "objectvalue, shortvalue, charvalue FROM BQT1.SmallA", FakeTranslationFactory.getInstance().getBQTTranslationUtility(), results);      //$NON-NLS-1$
    }
    
    public void testInsert() throws Exception {
        Object[][] results = new Object[][] {
            new Object[] { new Integer(0) }  
        };
        helpTestQuery("INSERT INTO BQT1.SmallA (stringkey) VALUES ('x')", FakeTranslationFactory.getInstance().getBQTTranslationUtility(), results);     //$NON-NLS-1$
    }

    public void testUpdate() throws Exception {
        Object[][] results = new Object[][] {
            new Object[] { new Integer(0) }  
        };
        helpTestQuery("UPDATE BQT1.SmallA SET stringkey = 'x'", FakeTranslationFactory.getInstance().getBQTTranslationUtility(), results);     //$NON-NLS-1$
    }

    public void testDelete() throws Exception {
        Object[][] results = new Object[][] {
            new Object[] { new Integer(0) }  
        };
        helpTestQuery("DELETE FROM BQT1.SmallA", FakeTranslationFactory.getInstance().getBQTTranslationUtility(), results);     //$NON-NLS-1$
    }

    public void testExec() throws Exception {
        Object[][] results = new Object[][] {
            new Object[] { "ABCDEFGHIJ" } //$NON-NLS-1$  
        };
        helpTestQuery("EXEC mmspTest1.MMSP1()", FakeTranslationFactory.getInstance().getBQTTranslationUtility(), results);     //$NON-NLS-1$
    }
    
    public void testExecWithoutResultSet() throws Exception {
    	Object[][] results = new Object[][] {  
        };
    	helpTestQuery("exec pm4.spTest9(1)", FakeTranslationFactory.getInstance().getBQTTranslationUtility(), results); //$NON-NLS-1$
    }
    
    /**
     * wait time is implemented as a random value up to the specified value.  assertions are then not really possible
     * based upon that time.
     */
    public void defer_testWaitTime() throws Exception {
        int waitTime = 100;
        int testCount = 10;
        
        ConnectorHost host = new ConnectorHost(new LoopbackConnector(), exampleProperties(waitTime, 1), FakeTranslationFactory.getInstance().getBQTTranslationUtility());
                
        for(int i=0; i<testCount; i++) {
            long before = System.currentTimeMillis();
            host.executeCommand("SELECT intkey FROM BQT1.SmallA"); //$NON-NLS-1$
            long after = System.currentTimeMillis();
            assertTrue("Waited too long", (after-before) <= waitTime); //$NON-NLS-1$
        }            
    }
    
    public void testQueryWithLimit() throws Exception {
        Object[][] expected = {{new Integer(0)},
                                {new Integer(0)},
                                {new Integer(0)}};
        helpTestQuery("SELECT intkey FROM BQT1.SmallA LIMIT 3", FakeTranslationFactory.getInstance().getBQTTranslationUtility(), 0, 100, expected); //$NON-NLS-1$
    }
    
}
