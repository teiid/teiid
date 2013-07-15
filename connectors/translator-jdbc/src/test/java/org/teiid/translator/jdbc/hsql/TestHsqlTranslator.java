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

package org.teiid.translator.jdbc.hsql;

import static org.junit.Assert.*;

import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;
import org.teiid.translator.TranslatorException;
import org.teiid.translator.jdbc.TranslationHelper;

@SuppressWarnings("nls")
public class TestHsqlTranslator {
	
    private static HsqlExecutionFactory TRANSLATOR; 

    @BeforeClass
    public static void setUp() throws TranslatorException {
        TRANSLATOR = new HsqlExecutionFactory();        
        TRANSLATOR.start();
    }

    @Ignore("the hibernate dialect has the version set reflectively so we can't set version 2")
    @Test public void testTempTable() throws Exception {
    	assertEquals("declare local temporary table foo (COL1 integer, COL2 varchar(100)) ", TranslationHelper.helpTestTempTable(TRANSLATOR, true));
    }
	
}
