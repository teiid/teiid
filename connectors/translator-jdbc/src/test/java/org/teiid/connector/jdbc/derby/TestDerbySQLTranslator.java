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

package org.teiid.connector.jdbc.derby;

import org.junit.BeforeClass;
import org.junit.Test;
import org.mockito.Mockito;
import org.teiid.connector.jdbc.TranslationHelper;
import org.teiid.resource.ConnectorException;
import org.teiid.resource.adapter.jdbc.JDBCExecutionFactory;
import org.teiid.translator.jdbc.derby.DerbySQLTranslator;


/**
 */
public class TestDerbySQLTranslator {

    private static DerbySQLTranslator TRANSLATOR; 

    @BeforeClass
    public static void setUp() throws ConnectorException {
        TRANSLATOR = new DerbySQLTranslator();        
        TRANSLATOR.initialize(Mockito.mock(JDBCExecutionFactory.class));
    }
    
    @Test
    public void testConcat_useLiteral() throws Exception {
        String input = "select concat(stringnum,'_xx') from BQT1.Smalla"; //$NON-NLS-1$
        String output = "SELECT {fn concat(SmallA.StringNum, '_xx')} FROM SmallA";  //$NON-NLS-1$
        
        TranslationHelper.helpTestVisitor(TranslationHelper.BQT_VDB, input, output, TRANSLATOR);
    }

    @Test
    public void testConcat() throws Exception {
        String input = "select concat(stringnum, stringnum) from BQT1.Smalla"; //$NON-NLS-1$       
        String output = "SELECT {fn concat(SmallA.StringNum, SmallA.StringNum)} FROM SmallA";  //$NON-NLS-1$
        
        TranslationHelper.helpTestVisitor(TranslationHelper.BQT_VDB, input, output, TRANSLATOR);
    }    
    
    @Test
    public void testConcat2_useLiteral() throws Exception {
        String input = "select concat2(stringnum,'_xx') from BQT1.Smalla"; //$NON-NLS-1$
        String output = "SELECT {fn concat(coalesce(SmallA.StringNum, ''), '_xx')} FROM SmallA";  //$NON-NLS-1$
        
        TranslationHelper.helpTestVisitor(TranslationHelper.BQT_VDB, input, output, TRANSLATOR);
    }

    @Test
    public void testConcat2() throws Exception {
        String input = "select concat2(stringnum, stringnum) from BQT1.Smalla"; //$NON-NLS-1$       
        String output = "SELECT CASE WHEN SmallA.StringNum IS NULL THEN NULL ELSE {fn concat(coalesce(SmallA.StringNum, ''), coalesce(SmallA.StringNum, ''))} END FROM SmallA";  //$NON-NLS-1$
        
        TranslationHelper.helpTestVisitor(TranslationHelper.BQT_VDB, input, output, TRANSLATOR);
    }    
    
}
