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
import java.util.List;

import junit.framework.Assert;

import org.junit.Test;
import org.teiid.cdk.api.ConnectorHost;
import org.teiid.cdk.api.TranslationUtility;
import org.teiid.cdk.unittest.FakeTranslationFactory;
import org.teiid.translator.TranslatorException;


public class TestLoopbackExecutionIncremental  {


    
    public void helpTestQuery(String sql, TranslationUtility metadata, Object[][] expectedResults) throws TranslatorException {
    	TestHelper.helpTestQuery(true, sql, metadata, 0, 2, expectedResults);
    }

    
    @Test
    public void testSimple() throws Exception {
        Object[][] results = new Object[][] {
            new Object[] { new Integer(0) },
            new Object[] { new Integer(1) }
        };
        helpTestQuery("SELECT intkey FROM BQT1.SmallA", FakeTranslationFactory.getInstance().getBQTTranslationUtility(), results);     //$NON-NLS-1$
    }
    @Test
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
    
    @Test
    public void testExec() throws Exception {
        Object[][] results = new Object[][] {
            new Object[] { "ABCDEFGHIJ" } //$NON-NLS-1$,
            ,new Object[] { "ABCDEFGHI1" } //$NON-NLS-1$
        };
        helpTestQuery("EXEC mmspTest1.MMSP1()", FakeTranslationFactory.getInstance().getBQTTranslationUtility(), results);     //$NON-NLS-1$
    }
    
    
    
  
    @Test
    public void testQueryWithLimit() throws Exception {
        Object[][] expected = {{new Integer(0)},
                                {new Integer(1)},
                                {new Integer(2)}};
        TestHelper.helpTestQuery(true, "SELECT intkey FROM BQT1.SmallA LIMIT 3", FakeTranslationFactory.getInstance().getBQTTranslationUtility(), 0, 100, expected); //$NON-NLS-1$
    }
    @Test
    public void testConstructIncrementedString(){
    	Assert.assertEquals("A",LoopbackExecution.constructIncrementedString(1));
    	Assert.assertEquals("ABC",LoopbackExecution.constructIncrementedString(3));
    	Assert.assertEquals("ABCDEFGHIJKLMNOPQRSTUVWXYZABCDEFGHIJKLMNOPQRSTUVWXYZA",LoopbackExecution.constructIncrementedString(53));
    }
    @Test
    public void testIncrementString(){
    	Assert.assertEquals("A100",LoopbackExecution.incrementString("ABCD",new BigInteger("100")));
    	Assert.assertEquals("ABCD",LoopbackExecution.incrementString("ABCD",new BigInteger("0")));
    }
    
}
