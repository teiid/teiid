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


package org.teiid.translator.jdbc.sybase;

import static org.junit.Assert.*;

import org.junit.BeforeClass;
import org.junit.Test;
import org.mockito.Mockito;
import org.teiid.language.Command;
import org.teiid.translator.ExecutionContext;
import org.teiid.translator.TranslatorException;
import org.teiid.translator.jdbc.TranslatedCommand;
import org.teiid.translator.jdbc.TranslationHelper;

public class TestSybaseIQTranslator {
	
    private static SybaseIQExecutionFactory trans = new SybaseIQExecutionFactory();
    
    @BeforeClass
    public static void setup() throws TranslatorException {
    	trans.setUseBindVariables(false);
        trans.start();
    }
    
    public void helpTestVisitor(String vdb, String input, String expectedOutput) {
        // Convert from sql to objects
        Command obj = TranslationHelper.helpTranslate(vdb, input);
        
        TranslatedCommand tc = new TranslatedCommand(Mockito.mock(ExecutionContext.class), trans);
		try {
			tc.translateCommand(obj);
		} catch (TranslatorException e) {
			throw new RuntimeException(e);
		}
        
        assertEquals("Did not get correct sql", expectedOutput, tc.getSql());             //$NON-NLS-1$
    }
    
    @Test public void testTimestampDiff() {
    	String input = "SELECT timestampadd(sql_tsi_quarter, 1, timestampvalue), timestampadd(sql_tsi_frac_second, 1000, timestampvalue), timestampdiff(sql_tsi_frac_second, timestampvalue, timestampvalue) from bqt1.smalla"; //$NON-NLS-1$
        String output = "SELECT dateadd('QUARTER', 1, SmallA.TimestampValue), dateadd('MILLISECOND', (1000 / 1000000), SmallA.TimestampValue), datediff('MILLISECOND', SmallA.TimestampValue, SmallA.TimestampValue) * 1000000 FROM SmallA";  //$NON-NLS-1$
        
        helpTestVisitor(TranslationHelper.BQT_VDB,
            input, 
            output);
    }
    
    @Test public void testLocate() {
        String input = "SELECT locate('a', stringkey, 2) from bqt1.smalla"; //$NON-NLS-1$
        String output = "SELECT locate(SmallA.StringKey, 'a', 2) FROM SmallA";  //$NON-NLS-1$
        
        helpTestVisitor(TranslationHelper.BQT_VDB,
            input, 
            output);
    }
    
    @Test public void testWeek() {
        String input = "SELECT week(datevalue) from bqt1.smalla"; //$NON-NLS-1$
        String output = "SELECT {fn week(SmallA.DateValue)} FROM SmallA";  //$NON-NLS-1$
        
        helpTestVisitor(TranslationHelper.BQT_VDB,
            input, 
            output);
    }
    
    @Test public void testDayOfYear() {
        String input = "SELECT dayofyear(datevalue) from bqt1.smalla"; //$NON-NLS-1$
        String output = "SELECT DATEPART(dy,SmallA.DateValue) FROM SmallA";  //$NON-NLS-1$
        
        helpTestVisitor(TranslationHelper.BQT_VDB,
            input, 
            output);
    }
    
}
