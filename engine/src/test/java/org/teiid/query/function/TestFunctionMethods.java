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

package org.teiid.query.function;

import static org.junit.Assert.*;

import java.sql.Timestamp;
import java.util.TimeZone;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.teiid.api.exception.query.FunctionExecutionException;
import org.teiid.core.types.BinaryType;
import org.teiid.core.types.ClobImpl;
import org.teiid.core.types.ClobType;
import org.teiid.core.util.PropertiesUtils;
import org.teiid.core.util.TimestampWithTimezone;
import org.teiid.language.SQLConstants.NonReserved;
import org.teiid.query.processor.TestProcessor;
import org.teiid.query.unittest.TimestampUtil;

@SuppressWarnings("nls")
public class TestFunctionMethods {
	
	@BeforeClass public static void oneTimeSetup() {
		TimestampWithTimezone.resetCalendar(TimeZone.getTimeZone("GMT-0600"));
	}
	
	@AfterClass public static void oneTimeTearDown() {
		TimestampWithTimezone.resetCalendar(null);
	}
	
	@Test public void testUnescape() {
		assertEquals("a\t\n\n%6", FunctionMethods.unescape("a\\t\\n\\012\\456"));
	}
	
	@Test public void testUnescape1() {
		assertEquals("a\u45AA'", FunctionMethods.unescape("a\\u45Aa\'"));
	}
	
	@Test public void testIso8601Week() {
		assertEquals(53, FunctionMethods.week(TimestampUtil.createDate(105, 0, 1)));
	}
	
	@Test public void testIso8601Week1() {
		assertEquals(52, FunctionMethods.week(TimestampUtil.createDate(106, 0, 1)));
	}
	
	@Test public void testDayOfWeek() {
		assertEquals(2, FunctionMethods.dayOfWeek(TimestampUtil.createDate(111, 10, 28)));
	}
	
	@Test public void testTimestampDiffTimeStamp_ErrorUsingEndDate2304() throws Exception {
		assertEquals(Long.valueOf(106753), FunctionMethods.timestampDiff(NonReserved.SQL_TSI_DAY, 
				new Timestamp(TimestampUtil.createDate(112, 0, 1).getTime()),
				new Timestamp(TimestampUtil.createDate(404, 3, 13).getTime()), false));
	}
	
	@Test public void testTimestampDiffTimeStamp_ErrorUsingEndDate2304a() throws Exception {
		assertEquals(Long.valueOf(32244), FunctionMethods.timestampDiff(NonReserved.SQL_TSI_DAY, 
				new Timestamp(TimestampUtil.createDate(112, 0, 1).getTime()),
				new Timestamp(TimestampUtil.createDate(200, 3, 13).getTime()), true));
	}
	
	@Test public void testTimestampDiffCalendarBasedHour() throws Exception {
		assertEquals(Long.valueOf(2562072), FunctionMethods.timestampDiff(NonReserved.SQL_TSI_HOUR, 
				new Timestamp(TimestampUtil.createDate(112, 0, 1).getTime()),
				new Timestamp(TimestampUtil.createDate(404, 3, 13).getTime()), true));
	}
	
	@Test public void testTimestampDiffCalendarBasedHour1() throws Exception {
		TimestampWithTimezone.resetCalendar(TimeZone.getTimeZone("America/New York"));
		try {
			assertEquals(Long.valueOf(2472), FunctionMethods.timestampDiff(NonReserved.SQL_TSI_HOUR, 
				new Timestamp(TimestampUtil.createDate(112, 0, 1).getTime()),
				new Timestamp(TimestampUtil.createDate(112, 3, 13).getTime()), true));
		} finally {
			TimestampWithTimezone.resetCalendar(TimeZone.getTimeZone("GMT-0600"));
		}
	}
	
	@Test public void testTimestampDiffCalendarBasedMonth() throws Exception {
		assertEquals(Long.valueOf(1), FunctionMethods.timestampDiff(NonReserved.SQL_TSI_MONTH, 
				new Timestamp(TimestampUtil.createDate(112, 0, 10).getTime()),
				new Timestamp(TimestampUtil.createDate(112, 1, 1).getTime()), true));
	}
	
	@Test public void testTimestampDiffCalendarBasedWeek() throws Exception {
		assertEquals(Long.valueOf(1), FunctionMethods.timestampDiff(NonReserved.SQL_TSI_WEEK, 
				new Timestamp(TimestampUtil.createDate(113, 2, 2).getTime()),
				new Timestamp(TimestampUtil.createDate(113, 2, 3).getTime()), true));
	}
	
	@Test public void testTimestampDiffCalendarBasedWeek1() throws Exception {
		assertEquals(Long.valueOf(0), FunctionMethods.timestampDiff(NonReserved.SQL_TSI_WEEK, 
				new Timestamp(TimestampUtil.createDate(113, 2, 3).getTime()),
				new Timestamp(TimestampUtil.createDate(113, 2, 4).getTime()), true));
	}
	
	@Test public void testTimestampDiffCalendarBasedWeek2() throws Exception {
		assertEquals(Long.valueOf(0), FunctionMethods.timestampDiff(NonReserved.SQL_TSI_WEEK, 
				new Timestamp(TimestampUtil.createDate(113, 2, 4).getTime()),
				new Timestamp(TimestampUtil.createDate(113, 2, 3).getTime()), true));
	}

    @Test public void regexpReplaceOkay() throws Exception {
        assertEquals(
                "fooXbaz",
                FunctionMethods.regexpReplace(null, "foobarbaz", "b..", "X")
        );
        assertEquals(
                "fooXX",
                FunctionMethods.regexpReplace(null, "foobarbaz", "b..", "X", "g")
        );
        assertEquals(
                "fooXarYXazY",
                FunctionMethods.regexpReplace(null, "foobarbaz", "b(..)", "X$1Y", "g")
        );
        assertEquals(
                "fooBXRbXz",
                FunctionMethods.regexpReplace(null, "fooBARbaz", "a", "X", "gi")
        );
        assertEquals(
                "xxbye Wxx",
                FunctionMethods.regexpReplace(TestProcessor.createCommandContext(), "Goodbye World", "[g-o].", "x", "gi")
        );
        assertEquals(
        		new ClobType(new ClobImpl("xxbye Wxx")),
                FunctionMethods.regexpReplace(TestProcessor.createCommandContext(), new ClobType(new ClobImpl("Goodbye World")), "[g-o].", "x", "gi")
        );
    }

    @Test(expected=FunctionExecutionException.class)
    public void regexpInvalidFlagsBad() throws Exception {
        FunctionMethods.regexpReplace(null, "foobarbaz", "b..", "X", "y");
    }
    
    @Test(expected=FunctionExecutionException.class) public void testAbsIntBounds() throws FunctionExecutionException {
    	FunctionMethods.abs(Integer.MIN_VALUE);
    }
    
    @Test(expected=FunctionExecutionException.class) public void testAbsLongBounds() throws FunctionExecutionException {
    	FunctionMethods.abs(Long.MIN_VALUE);
    }
    
    @Test(expected=FunctionExecutionException.class) public void testPlusLongBounds() throws FunctionExecutionException {
    	FunctionMethods.plus(Long.MIN_VALUE, -1);
    }
    
    @Test(expected=FunctionExecutionException.class) public void testPlusLongBounds1() throws FunctionExecutionException {
    	FunctionMethods.plus(Long.MAX_VALUE, 1);
    }
    
    @Test(expected=FunctionExecutionException.class) public void testPlusIntBounds() throws FunctionExecutionException {
    	FunctionMethods.plus(Integer.MIN_VALUE, -1);
    }
        
    @Test(expected=FunctionExecutionException.class) public void testPlusIntBounds1() throws FunctionExecutionException {
    	FunctionMethods.plus(Integer.MAX_VALUE, 1);
    }
    
    @Test(expected=FunctionExecutionException.class) public void testMinusIntBounds1() throws FunctionExecutionException {
    	FunctionMethods.minus(Integer.MAX_VALUE, -1);
    }
    
    @Test(expected=FunctionExecutionException.class) public void testMinusLongBounds() throws FunctionExecutionException {
    	FunctionMethods.minus(Long.MIN_VALUE, 1);
    }
    
    @Test(expected=FunctionExecutionException.class) public void testMinusLongBounds1() throws FunctionExecutionException {
    	FunctionMethods.minus(Long.MAX_VALUE, -1);
    }
    
    @Test(expected=FunctionExecutionException.class) public void testMinusIntBounds() throws FunctionExecutionException {
    	FunctionMethods.minus(Integer.MIN_VALUE, 1);
    }
    
    @Test(expected=FunctionExecutionException.class) public void testDivideIntBounds() throws FunctionExecutionException {
    	FunctionMethods.divide(Integer.MIN_VALUE, -1);
    }
    
    @Test(expected=FunctionExecutionException.class) public void testDivedLongBounds() throws FunctionExecutionException {
    	FunctionMethods.divide(Long.MIN_VALUE, -1);
    }
    
    @Test(expected=FunctionExecutionException.class) public void testMultLongBounds() throws FunctionExecutionException {
    	FunctionMethods.multiply(Long.MIN_VALUE, -1);
    }
    
    @Test(expected=FunctionExecutionException.class) public void testMultLongBounds1() throws FunctionExecutionException {
    	FunctionMethods.multiply(Long.MAX_VALUE, 2);
    }
    
    @Test(expected=FunctionExecutionException.class) public void testMultLongBounds2() throws FunctionExecutionException {
    	FunctionMethods.multiply(Long.MIN_VALUE, -2);
    }
    
    @Test(expected=FunctionExecutionException.class) public void testMultIntBounds() throws FunctionExecutionException {
    	FunctionMethods.multiply(Integer.MIN_VALUE, -1);
    }
    
    @Test(expected=FunctionExecutionException.class) public void testMultIntBounds1() throws FunctionExecutionException {
    	FunctionMethods.multiply(Integer.MAX_VALUE, 2);
    }
    
    @Test(expected=FunctionExecutionException.class) public void testMultIntBounds2() throws FunctionExecutionException {
    	FunctionMethods.multiply(Integer.MIN_VALUE, -2);
    }
    
    @Test public void testHashes() throws Exception {
        assertEquals("900150983CD24FB0D6963F7D28E17F72", PropertiesUtils.toHex(FunctionMethods.md5("abc").getBytesDirect()));
        assertEquals("A9993E364706816ABA3E25717850C26C9CD0D89D", PropertiesUtils.toHex(FunctionMethods.sha1("abc").getBytesDirect()));
        assertEquals("BA7816BF8F01CFEA414140DE5DAE2223B00361A396177A9CB410FF61F20015AD", PropertiesUtils.toHex(FunctionMethods.sha2_256("abc").getBytesDirect()));
        assertEquals("DDAF35A193617ABACC417349AE20413112E6FA4E89A97EA20A9EEEE64B55D39A2192992A274FC1A836BA3C23A3FEEBBD454D4423643CE80E2A9AC94FA54CA49F", PropertiesUtils.toHex(FunctionMethods.sha2_512("abc").getBytesDirect()));
    }
    
    @Test
    public void testEncryptDecrypt() throws Exception {
        
        String key = "redhat"; //$NON-NLS-1$
        String data = "jboss teiid"; //$NON-NLS-1$
        
        String encrypted = FunctionMethods.aes_encrypt(data, key);
        String decrypted = FunctionMethods.aes_decrypt(encrypted, key);
        assertEquals(data, decrypted);
        
        BinaryType encryptedBytes = FunctionMethods.aes_encrypt(new BinaryType(data.getBytes("UTF-8")), new BinaryType(key.getBytes("UTF-8"))); //$NON-NLS-1$ //$NON-NLS-2$
        BinaryType decryptedBytes = FunctionMethods.aes_decrypt(encryptedBytes, new BinaryType(key.getBytes("UTF-8"))); //$NON-NLS-1$ 
        assertArrayEquals(data.getBytes("UTF-8"), decryptedBytes.getBytesDirect()); //$NON-NLS-1$     
    }
    
}
