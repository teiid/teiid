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

package org.teiid.translator.jdbc.h2;

import static org.junit.Assert.*;

import org.junit.BeforeClass;
import org.junit.Test;
import org.teiid.translator.TranslatorException;
import org.teiid.translator.jdbc.TranslationHelper;

@SuppressWarnings("nls")
public class TestH2Translator {
	
    private static H2ExecutionFactory TRANSLATOR; 

    @BeforeClass
    public static void setUp() throws TranslatorException {
        TRANSLATOR = new H2ExecutionFactory();        
        TRANSLATOR.start();
    }
	
	@Test public void testTimestampDiff() throws Exception {
		String input = "select h2.timestampdiff('SQL_TSI_FRAC_SECOND', timestampvalue, {d '1970-01-01'}) from BQT1.Smalla"; //$NON-NLS-1$       
        String output = "SELECT datediff('MILLISECOND', SmallA.TimestampValue, TIMESTAMP '1970-01-01 00:00:00.0') * 1000000 FROM SmallA";  //$NON-NLS-1$
        
        TranslationHelper.helpTestVisitor(TranslationHelper.BQT_VDB, input, output, TRANSLATOR);
	}
	
	@Test public void testTimestampAddDiff() throws Exception {
		String input = "SELECT timestampadd(sql_tsi_quarter, 1, timestampvalue), timestampadd(sql_tsi_frac_second, 1000, timestampvalue), timestampdiff(sql_tsi_frac_second, timestampvalue, timestampvalue) from bqt1.smalla"; //$NON-NLS-1$       
        String output = "SELECT dateadd('MONTH', 1 * 3, SmallA.TimestampValue), dateadd('MILLISECOND', 1000 / 1000000, SmallA.TimestampValue), datediff('MILLISECOND', SmallA.TimestampValue, SmallA.TimestampValue) * 1000000 FROM SmallA";  //$NON-NLS-1$
        
        TranslationHelper.helpTestVisitor(TranslationHelper.BQT_VDB, input, output, TRANSLATOR);
	}
	
	@Test public void testTimestampAdd1() throws Exception {
		String input = "select timestampadd(SQL_TSI_HOUR, intnum, {t '00:00:00'}) from BQT1.Smalla"; //$NON-NLS-1$       
        String output = "SELECT cast(dateadd('HOUR', SmallA.IntNum, TIMESTAMP '1970-01-01 00:00:00.0') AS time) FROM SmallA";  //$NON-NLS-1$
        
        TranslationHelper.helpTestVisitor(TranslationHelper.BQT_VDB, input, output, TRANSLATOR);
	}
	
	@Test public void testLikeEscape() throws Exception {
		String input = "select 1 from BQT1.Smalla where stringkey like '_a*'"; //$NON-NLS-1$       
        String output = "SELECT 1 FROM SmallA WHERE SmallA.StringKey LIKE '_a*' ESCAPE ''";  //$NON-NLS-1$
        
        TranslationHelper.helpTestVisitor(TranslationHelper.BQT_VDB, input, output, TRANSLATOR);
	}
	
    @Test public void testTempTable() throws Exception {
    	assertEquals("create cached local temporary table if not exists foo (COL1 integer, COL2 varchar(100)) on commit drop transactional", TranslationHelper.helpTestTempTable(TRANSLATOR, true));
    }
    
    @Test public void testJoinNesting() throws Exception {
		String input = "select a.intkey from (BQT1.Smalla a left outer join bqt1.smallb b on a.intkey = b.intkey) inner join (bqt1.mediuma ma inner join bqt1.mediumb mb on mb.intkey = ma.intkey) on a.intkey = mb.intkey"; //$NON-NLS-1$       
        String output = "SELECT a.IntKey FROM (SmallA AS a LEFT OUTER JOIN SmallB AS b ON a.IntKey = b.IntKey) INNER JOIN (MediumA AS ma INNER JOIN MediumB AS mb ON mb.IntKey = ma.IntKey) ON a.IntKey = mb.IntKey";  //$NON-NLS-1$
        
        TranslationHelper.helpTestVisitor(TranslationHelper.BQT_VDB, input, output, TRANSLATOR);
    }

}
