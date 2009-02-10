/*
 * JBoss, Home of Professional Open Source.
 * Copyright (C) 2008 Red Hat, Inc.
 * Copyright (C) 2000-2007 MetaMatrix, Inc.
 * Licensed to Red Hat, Inc. under one or more contributor 
 * license agreements.  See the copyright.txt file in the
 * distribution for a full listing of individual contributors.
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

package com.metamatrix.connector.jdbc.sybase;

import java.sql.Timestamp;
import java.util.Properties;

import junit.framework.TestCase;

import com.metamatrix.cdk.CommandBuilder;
import com.metamatrix.cdk.api.EnvironmentUtility;
import com.metamatrix.connector.language.IExpression;
import com.metamatrix.connector.language.IFunction;
import com.metamatrix.connector.language.ILanguageFactory;

/**
 */
public class TestFormatTimestampModifier extends TestCase {

    private static final ILanguageFactory LANG_FACTORY = CommandBuilder.getLanuageFactory();

    /**
     * Constructor for TestFormatTimestampModifier.
     * @param name
     */
    public TestFormatTimestampModifier(String name) {
        super(name);
    }

    public void helpTestMod(String format, String expectedStr) throws Exception {
        Timestamp ts = new Timestamp(103, 10, 1, 12, 5, 2, 0);
        IFunction func = LANG_FACTORY.createFunction("formattimestamp",  //$NON-NLS-1$
            new IExpression[] { LANG_FACTORY.createLiteral(ts, Timestamp.class), 
                LANG_FACTORY.createLiteral(format, String.class) }, 
            String.class);
        
        FormatTimestampModifier mod = new FormatTimestampModifier(LANG_FACTORY);
        IExpression expr = mod.modify(func);
        
        SybaseSQLTranslator trans = new SybaseSQLTranslator();
        trans.initialize(EnvironmentUtility.createEnvironment(new Properties(), false), null);
        
        SybaseSQLConversionVisitor sqlVisitor = new SybaseSQLConversionVisitor(); 
        sqlVisitor.setFunctionModifiers(trans.getFunctionModifiers());
        sqlVisitor.setLanguageFactory(LANG_FACTORY);  
        sqlVisitor.append(expr);        
        assertEquals(expectedStr, sqlVisitor.toString());
    }
    
    public void testModStyle1() throws Exception {
        helpTestMod("MM/dd/yyyy", "convert(varchar, {ts'2003-11-01 12:05:02.0'}, 1)");       //$NON-NLS-1$ //$NON-NLS-2$
    }

    public void testModUnknownFormat() throws Exception {
        helpTestMod("MM:yyy", "convert(varchar, {ts'2003-11-01 12:05:02.0'})");      //$NON-NLS-1$ //$NON-NLS-2$
    }    

}
