package org.teiid.translator.jdbc.informix;
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

import org.junit.BeforeClass;
import org.junit.Test;
import org.teiid.translator.TranslatorException;
import org.teiid.translator.jdbc.TranslationHelper;

public class TestInformixExecutionFactory {
	
    private static InformixExecutionFactory TRANSLATOR; 

    @BeforeClass
    public static void setUp() throws TranslatorException {
        TRANSLATOR = new InformixExecutionFactory();        
        TRANSLATOR.start();
    }
	
    @Test public void testCast() throws Exception {
		String input = "SELECT cast(INTKEY as string), cast(stringkey as time), cast(stringkey as date), cast(stringkey as timestamp) FROM BQT1.SmallA"; //$NON-NLS-1$       
        String output = "SELECT cast(SmallA.IntKey AS varchar(255)), cast(SmallA.StringKey AS datetime hour to second), cast(SmallA.StringKey AS date), cast(SmallA.StringKey AS datetime year to fraction(5)) FROM SmallA";  //$NON-NLS-1$
        
        TranslationHelper.helpTestVisitor(TranslationHelper.BQT_VDB, input, output, TRANSLATOR);
	}
    
    @Test public void testTimeLiteral() throws Exception {
		String input = "SELECT {t '12:11:01'} FROM BQT1.SmallA"; //$NON-NLS-1$       
        String output = "SELECT {t '12:11:01'} FROM SmallA";  //$NON-NLS-1$
        
        TranslationHelper.helpTestVisitor(TranslationHelper.BQT_VDB, input, output, TRANSLATOR);
	}
    
    @Test public void testMinMaxBoolean() throws Exception {
    	String input = "SELECT min(booleanvalue), max(booleanvalue) FROM BQT1.SmallA"; //$NON-NLS-1$       
        String output = "SELECT cast(MIN(cast(SmallA.BooleanValue as char)) as boolean), cast(MAX(cast(SmallA.BooleanValue as char)) as boolean) FROM SmallA";  //$NON-NLS-1$
        
        TranslationHelper.helpTestVisitor(TranslationHelper.BQT_VDB, input, output, TRANSLATOR);
    }
	
}
