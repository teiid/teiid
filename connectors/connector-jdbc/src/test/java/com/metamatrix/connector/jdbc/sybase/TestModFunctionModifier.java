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

package com.metamatrix.connector.jdbc.sybase;

import java.util.Properties;

import junit.framework.TestCase;

import com.metamatrix.cdk.CommandBuilder;
import com.metamatrix.cdk.api.EnvironmentUtility;
import com.metamatrix.connector.language.IExpression;
import com.metamatrix.connector.language.IFunction;
import com.metamatrix.connector.language.ILanguageFactory;

/**
 */
public class TestModFunctionModifier extends TestCase {

    private static final ILanguageFactory LANG_FACTORY = CommandBuilder.getLanuageFactory();


    /**
     * Constructor for TestModFunctionModifier.
     * @param name
     */
    public TestModFunctionModifier(String name) {
        super(name);
    }

    public void helpTestMod(int first, int second, String expectedStr) throws Exception {
        IFunction func = LANG_FACTORY.createFunction("mod",  //$NON-NLS-1$
            new IExpression[] { LANG_FACTORY.createLiteral(new Integer(first), Integer.class), 
                LANG_FACTORY.createLiteral(new Integer(second), Integer.class) }, 
            String.class);
        
        ModFunctionModifier mod = new ModFunctionModifier();
        IExpression expr = mod.modify(func);
        
        SybaseSQLTranslator trans = new SybaseSQLTranslator();
        trans.initialize(EnvironmentUtility.createEnvironment(new Properties(), false), null);
        
        SybaseSQLConversionVisitor sqlVisitor = new SybaseSQLConversionVisitor(); 
        sqlVisitor.setFunctionModifiers(trans.getFunctionModifiers());
        sqlVisitor.setLanguageFactory(LANG_FACTORY);  
        sqlVisitor.append(expr);        
        assertEquals(expectedStr, sqlVisitor.toString());
    }
    
    public void testMod1() throws Exception {
        helpTestMod(5, 5, "(5 % 5)"); //$NON-NLS-1$
    }
 
}
