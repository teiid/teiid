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

package org.teiid.translator.loopback;

import static org.junit.Assert.*;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;

import org.junit.Test;
import org.teiid.cdk.api.TranslationUtility;
import org.teiid.cdk.unittest.FakeTranslationFactory;
import org.teiid.translator.TranslatorException;


@SuppressWarnings("nls")
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
                new Date(0), new Time(0),
                new Timestamp(0), Boolean.FALSE,
                new BigInteger("0"), new BigDecimal("0.0"), "ABCDEFGHIJ", //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
                new Short((short)0), new Character('c')
                 }  ;

        Object[] row2=   new Object[] {
                new Integer(1),
                "ABCDEFGHI1",//First row is same as before, for backward compatibility
                new Float(0.1),
                new Long(1),
                new Double(0.1),
                new Byte((byte)1), //$NON-NLS-1$
                new Date(LoopbackExecution.DAY_SECONDS*1000),
                new Time(1000),
                new Time(1),
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
        assertEquals("A",LoopbackExecution.constructIncrementedString(1));
        assertEquals("ABC",LoopbackExecution.constructIncrementedString(3));
        assertEquals("ABCDEFGHIJKLMNOPQRSTUVWXYZABCDEFGHIJKLMNOPQRSTUVWXYZA",LoopbackExecution.constructIncrementedString(53));
    }
    @Test
    public void testIncrementString(){
        assertEquals("A100",LoopbackExecution.incrementString("ABCD",new BigInteger("100")));
        assertEquals("ABCD",LoopbackExecution.incrementString("ABCD",new BigInteger("0")));
    }

    /**
     * Shows that we'll use the connector limit over the row limit, and apply the offset
     * @throws Exception
     */
    @Test
    public void testQueryWithLimitOffset() throws Exception {
        Object[][] expected = new Object[90][1];
        for (int i = 0; i < expected.length; i++) {
            expected[i] = new Object[] {i+10};
        }
        TestHelper.helpTestQuery(true, "SELECT intkey FROM BQT1.SmallA LIMIT 10, 300", FakeTranslationFactory.getInstance().getBQTTranslationUtility(), 0, 100, expected); //$NON-NLS-1$
    }

}
