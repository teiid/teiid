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

package org.teiid.translator.loopback;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.Calendar;
import java.util.List;

import junit.framework.Assert;
import junit.framework.TestCase;

import org.teiid.cdk.api.ConnectorHost;
import org.teiid.cdk.api.TranslationUtility;
import org.teiid.cdk.unittest.FakeTranslationFactory;
import org.teiid.translator.TranslatorException;
import org.teiid.translator.loopback.LoopbackExecution;
import org.teiid.translator.loopback.LoopbackExecutionFactory;


public class TestLoopbackExecutionIncremental extends TestCase {

    public TestLoopbackExecutionIncremental(String name) {
        super(name);
    }

    public LoopbackExecutionFactory exampleProperties(int waitTime, int rowCount) {
    	LoopbackExecutionFactory config = new LoopbackExecutionFactory();
    	config.setWaitTime(waitTime);
    	config.setRowCount(rowCount);
    	config.setIncrementRows(true);
        return config;
    }
    
    public void helpTestQuery(String sql, TranslationUtility metadata, Object[][] expectedResults) throws TranslatorException {
        helpTestQuery(sql, metadata, 0, 2, expectedResults);
    }

    public void helpTestQuery(String sql, TranslationUtility metadata, int waitTime, int rowCount, Object[][] expectedResults) throws TranslatorException {
    	ConnectorHost host = new ConnectorHost(exampleProperties(waitTime, rowCount), null, metadata);
                              
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
            new Object[] { new Integer(0) },
            new Object[] { new Integer(1) }
        };
        helpTestQuery("SELECT intkey FROM BQT1.SmallA", FakeTranslationFactory.getInstance().getBQTTranslationUtility(), results);     //$NON-NLS-1$
    }
    
    public void testMostTypes() throws Exception {
    	Object[] row1=   new Object[] { new Integer(0), "ABCDEFGHIJ", new Float(0), new Long(0), new Double(0), new Byte((byte)0), //$NON-NLS-1$
                LoopbackExecution.SQL_DATE_VAL, LoopbackExecution.TIME_VAL, 
                LoopbackExecution.TIMESTAMP_VAL, Boolean.FALSE, 
                new BigInteger("0"), new BigDecimal("0"), "ABCDEFGHIJ", //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
                new Short((short)0), new Character('c')
                 }  ;
    	
    	Object[] row2=   new Object[] { 
    			new Integer(1), 
    			"ABCDEFGHI1",//First row is same as before, for backward compatibility 
    			new Float(0.1), 
    			new Long(1), 
    			new Double(0.1), 
    			new Byte((byte)1), //$NON-NLS-1$
                new Date(LoopbackExecution.SQL_DATE_VAL.getTime()+LoopbackExecution.DAY_MILIS),
                new Time(LoopbackExecution.TIME_VAL.getTime()+1000), 
                new Time(LoopbackExecution.TIMESTAMP_VAL.getTime()+1),
                Boolean.TRUE, 
                new BigInteger("1"), 
                new BigDecimal("0.1"), 
                "ABCDEFGHI1", //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
                new Short((short)1), 
                new Character('d')
                 }  ;
    	
    	
    	
        Object[][] results = new Object[][] {
          row1, row2
        };
        
        helpTestQuery("SELECT intkey, StringKey, floatnum, longnum, doublenum, bytenum, " + //$NON-NLS-1$
            "datevalue, timevalue, timestampvalue, booleanvalue, bigintegervalue, bigdecimalvalue, " + //$NON-NLS-1$
            "objectvalue, shortvalue, charvalue FROM BQT1.SmallA", FakeTranslationFactory.getInstance().getBQTTranslationUtility(), results);      //$NON-NLS-1$
    }
    

    public void testExec() throws Exception {
        Object[][] results = new Object[][] {
            new Object[] { "ABCDEFGHIJ" } //$NON-NLS-1$,
            ,new Object[] { "ABCDEFGHI1" } //$NON-NLS-1$
        };
        helpTestQuery("EXEC mmspTest1.MMSP1()", FakeTranslationFactory.getInstance().getBQTTranslationUtility(), results);     //$NON-NLS-1$
    }
    
    
    
    /**
     * wait time is implemented as a random value up to the specified value.  assertions are then not really possible
     * based upon that time.
     */
    public void defer_testWaitTime() throws Exception {
        int waitTime = 100;
        int testCount = 10;
        
        ConnectorHost host = new ConnectorHost(exampleProperties(waitTime, 1), null, FakeTranslationFactory.getInstance().getBQTTranslationUtility());
                
        for(int i=0; i<testCount; i++) {
            long before = System.currentTimeMillis();
            host.executeCommand("SELECT intkey FROM BQT1.SmallA"); //$NON-NLS-1$
            long after = System.currentTimeMillis();
            assertTrue("Waited too long", (after-before) <= waitTime); //$NON-NLS-1$
        }            
    }
    
    public void testQueryWithLimit() throws Exception {
        Object[][] expected = {{new Integer(0)},
                                {new Integer(1)},
                                {new Integer(2)}};
        helpTestQuery("SELECT intkey FROM BQT1.SmallA LIMIT 3", FakeTranslationFactory.getInstance().getBQTTranslationUtility(), 0, 100, expected); //$NON-NLS-1$
    }
    
    public void testConstructIncrementedString(){
    	Assert.assertEquals("A",LoopbackExecution.constructIncrementedString(1));
    	Assert.assertEquals("ABC",LoopbackExecution.constructIncrementedString(3));
    	Assert.assertEquals("ABCDEFGHIJKLMNOPQRSTUVWXYZABCDEFGHIJKLMNOPQRSTUVWXYZA",LoopbackExecution.constructIncrementedString(53));
    }
    
    public void testIncrementString(){
    	Assert.assertEquals("A100",LoopbackExecution.incrementString("ABCD",new BigInteger("100")));
    	Assert.assertEquals("ABCD",LoopbackExecution.incrementString("ABCD",new BigInteger("0")));
    }
    
}
