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

package org.teiid.translator.jdbc.ingres;

import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;
import org.teiid.translator.TranslatorException;
import org.teiid.translator.jdbc.TranslationHelper;

public class TestIngresExecutionFactory {
	
    private static IngresExecutionFactory TRANSLATOR; 

    @BeforeClass
    public static void setUp() throws TranslatorException {
        TRANSLATOR = new IngresExecutionFactory();        
        TRANSLATOR.start();
    }
	
    @Ignore
	@Test public void testLocate() throws Exception {
		String input = "SELECT INTKEY FROM BQT1.SmallA WHERE LOCATE(1, INTKEY) = 1 ORDER BY INTKEY"; //$NON-NLS-1$       
        String output = "SELECT SmallA.IntKey FROM SmallA WHERE locate(cast(SmallA.IntKey AS varchar(4000)), '1') = 1 ORDER BY SmallA.IntKey";  //$NON-NLS-1$
        
        TranslationHelper.helpTestVisitor(TranslationHelper.BQT_VDB, input, output, TRANSLATOR);
	}
    
    @Test public void testLimit() throws Exception {
		String input = "SELECT INTKEY FROM BQT1.SmallA LIMIT 1"; //$NON-NLS-1$       
        String output = "SELECT SmallA.IntKey FROM SmallA FETCH FIRST 1 ROWS ONLY";  //$NON-NLS-1$
        
        TranslationHelper.helpTestVisitor(TranslationHelper.BQT_VDB, input, output, TRANSLATOR);
	}
	
}
