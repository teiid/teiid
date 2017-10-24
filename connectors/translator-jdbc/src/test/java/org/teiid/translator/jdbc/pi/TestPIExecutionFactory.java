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

package org.teiid.translator.jdbc.pi;

import java.util.TimeZone;

import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.teiid.core.util.ObjectConverterUtil;
import org.teiid.core.util.TimestampWithTimezone;
import org.teiid.core.util.UnitTestUtil;
import org.teiid.translator.TranslatorException;
import org.teiid.translator.TypeFacility;
import org.teiid.translator.jdbc.TranslationHelper;

@SuppressWarnings("nls")
public class TestPIExecutionFactory {
	
	private static PIExecutionFactory TRANSLATOR;
	
	@BeforeClass
	public static void setup() throws TranslatorException {
		TRANSLATOR = new PIExecutionFactory();
		TRANSLATOR.start();
		TimestampWithTimezone.resetCalendar(TimeZone.getTimeZone("GMT")); 
	}
	
	@AfterClass
	public static void tearDown() {
		TimestampWithTimezone.resetCalendar(null);
	}
	
	@Test 
	public void testDateFormats() throws TranslatorException {
		String input = "SELECT stringkey FROM BQT1.MediumA where datevalue < '2001-01-01' and timevalue < '12:11:01' and timestampvalue < '2012-02-03 11:12:13'"; //$NON-NLS-1$
		String output = "SELECT MediumA.StringKey FROM MediumA WHERE MediumA.DateValue < '2001-01-01' AND MediumA.TimeValue < '12:11:01' AND MediumA.TimestampValue < '2012-02-03 11:12:13.0'"; //$NON-NLS-1$
		TranslationHelper.helpTestVisitor(TranslationHelper.BQT_VDB, input, output, TRANSLATOR);
	}

    @Test 
    public void testLeftJoin() throws Exception {
        String input = "SELECT * FROM Sample.Asset.ElementAttribute EA LEFT JOIN "
                + "Sample.Asset.ElementAttributeCategory EAC ON EA.ID = EAC.ElementAttributeID"; //$NON-NLS-1$
        String output = "SELECT EAC.[ElementAttributeID], cast(EAC.[CategoryID] as String), "
                + "cast(EA.[ID] as String), EA.[Path], EA.[Name] "
                + "FROM Sample.Asset.ElementAttribute AS EA "
                + "LEFT OUTER JOIN [Sample].[Asset].[ElementAttributeCategory] AS EAC "
                + "ON EA.[ID] = EAC.[ElementAttributeID]"; //$NON-NLS-1$
        String ddl = ObjectConverterUtil.convertFileToString(UnitTestUtil.getTestDataFile("pi.ddl"));
        TranslationHelper.helpTestVisitor(ddl, input, output, TRANSLATOR);
    }
    
    @Test 
    public void testCrossJoinAsInnter() throws Exception {
        String input = "SELECT * FROM Sample.Asset.ElementAttribute EA CROSS JOIN "
                + "Sample.Asset.ElementAttributeCategory EAC"; //$NON-NLS-1$
        String output = "SELECT EAC.[ElementAttributeID], cast(EAC.[CategoryID] as String), "
                + "cast(EA.[ID] as String), EA.[Path], EA.[Name] "
                + "FROM Sample.Asset.ElementAttribute AS EA "
                + "INNER JOIN [Sample].[Asset].[ElementAttributeCategory] AS EAC "
                + "ON 1 = 1"; //$NON-NLS-1$
        String ddl = ObjectConverterUtil.convertFileToString(UnitTestUtil.getTestDataFile("pi.ddl"));
        TranslationHelper.helpTestVisitor(ddl, input, output, TRANSLATOR);
    }
    
    @Test 
    public void testRightOuterAsLeftOuter() throws Exception {
        String input = "SELECT * FROM Sample.Asset.ElementAttribute EA RIGHT OUTER JOIN "
                + "Sample.Asset.ElementAttributeCategory EAC ON EA.ID = EAC.ElementAttributeID"; //$NON-NLS-1$
        String output = "SELECT EAC.[ElementAttributeID], cast(EAC.[CategoryID] as String), "
                + "cast(EA.[ID] as String), EA.[Path], EA.[Name] "
                + "FROM [Sample].[Asset].[ElementAttributeCategory] AS EAC "
                + "LEFT OUTER JOIN Sample.Asset.ElementAttribute AS EA "
                + "ON EA.[ID] = EAC.[ElementAttributeID]"; //$NON-NLS-1$
        String ddl = ObjectConverterUtil.convertFileToString(UnitTestUtil.getTestDataFile("pi.ddl"));
        TranslationHelper.helpTestVisitor(ddl, input, output, TRANSLATOR);
    }     
    
    @Test 
    public void testCrossApply() throws Exception {
        String input = "SELECT * FROM Sample.Asset.ElementAttribute EA CROSS JOIN "
                + "LATERAL (exec GetPIPoint(EA.ID)) EAC "; //$NON-NLS-1$
        String output = "SELECT Path, Server, Tag, [Number of Computers], cast(EA.[ID] as String), "
                + "EA.[Path], EA.[Name] FROM Sample.Asset.ElementAttribute AS EA "
                + "CROSS APPLY Sample.EventFrame.GetPIPoint(EA.[ID])"; //$NON-NLS-1$
        String ddl = ObjectConverterUtil.convertFileToString(UnitTestUtil.getTestDataFile("pi.ddl"));
        TranslationHelper.helpTestVisitor(ddl, input, output, TRANSLATOR);
    }   
    
    @Test 
    public void testApplyOuter() throws Exception {
        String input = "SELECT EA.ID, EAC.Path FROM ElementAttribute EA LEFT OUTER JOIN "
                + "LATERAL (exec GetPIPoint(EA.ID)) AS EAC ON 1=1"; //$NON-NLS-1$
        String output = "SELECT cast(EA.[ID] as String), Path "
                + "FROM Sample.Asset.ElementAttribute AS EA "
                + "OUTER APPLY Sample.EventFrame.GetPIPoint(EA.[ID])"; //$NON-NLS-1$
        String ddl = ObjectConverterUtil.convertFileToString(UnitTestUtil.getTestDataFile("pi.ddl"));
        TranslationHelper.helpTestVisitor(ddl, input, output, TRANSLATOR);
    }  
    
	@Test 
	public void testTimestamp2Time() throws TranslatorException {
		String input = "select cast(timestampvalue as time) from BQT1.MediumA"; //$NON-NLS-1$
		String output = "SELECT cast(format(MediumA.TimestampValue, 'hh:mm:ss.fff') as Time) FROM MediumA"; //$NON-NLS-1$
		TranslationHelper.helpTestVisitor(TranslationHelper.BQT_VDB, input, output, TRANSLATOR);
	}
	
	@Test 
	public void testLength() throws TranslatorException {
		String input = "select length(STRINGKEY) from BQT1.MediumA"; //$NON-NLS-1$
		String output = "SELECT LEN(MediumA.StringKey) FROM MediumA"; //$NON-NLS-1$
		TranslationHelper.helpTestVisitor(TranslationHelper.BQT_VDB, input, output, TRANSLATOR);
	}
	
	@Test 
	public void testOrderBy() throws TranslatorException {
		String input = "SELECT FloatNum FROM BQT1.MediumA ORDER BY FloatNum ASC"; //$NON-NLS-1$
		String output = "SELECT MediumA.FloatNum FROM MediumA ORDER BY MediumA.FloatNum"; //$NON-NLS-1$
		TranslationHelper.helpTestVisitor(TranslationHelper.BQT_VDB, input, output, TRANSLATOR);
	}	
	
	@Test 
	public void testConvertToBigDecimal() throws TranslatorException {
		Assert.assertFalse(
				TRANSLATOR.supportsConvert(TypeFacility.RUNTIME_CODES.STRING, TypeFacility.RUNTIME_CODES.BIG_DECIMAL));
		Assert.assertFalse(
				TRANSLATOR.supportsConvert(TypeFacility.RUNTIME_CODES.STRING, TypeFacility.RUNTIME_CODES.BIG_INTEGER));
		Assert.assertFalse(
				TRANSLATOR.supportsConvert(TypeFacility.RUNTIME_CODES.STRING, TypeFacility.RUNTIME_CODES.GEOMETRY));
		Assert.assertTrue(
				TRANSLATOR.supportsConvert(TypeFacility.RUNTIME_CODES.STRING, TypeFacility.RUNTIME_CODES.INTEGER));
		
	} 	
}
