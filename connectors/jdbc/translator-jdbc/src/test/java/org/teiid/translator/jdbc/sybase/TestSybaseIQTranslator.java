/*
 * JBoss, Home of Professional Open Source.
 * See the COPYRIGHT.txt file distributed with this work for information
 * regarding copyright ownership.  Some portions may be licensed
 * to Red Hat, Inc. under one or more contributor license agreements.
 * 
 * This library is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License AS published by the Free Software Foundation; either
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

}
