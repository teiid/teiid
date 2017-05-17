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

package org.teiid.translator.jdbc.pi;

import java.util.TimeZone;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.teiid.core.util.ObjectConverterUtil;
import org.teiid.core.util.TimestampWithTimezone;
import org.teiid.core.util.UnitTestUtil;
import org.teiid.translator.TranslatorException;
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
}
