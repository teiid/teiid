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

package com.metamatrix.connector.jdbc.oracle;

import java.util.Properties;

import junit.framework.TestCase;

import com.metamatrix.cdk.CommandBuilder;
import com.metamatrix.cdk.api.EnvironmentUtility;
import com.metamatrix.connector.jdbc.extension.SQLConversionVisitor;
import com.metamatrix.connector.language.IExpression;
import com.metamatrix.connector.language.IFunction;
import com.metamatrix.connector.language.ILanguageFactory;

/**
 */
public class TestLocateFunctionModifier extends TestCase {

    private static final ILanguageFactory LANG_FACTORY = CommandBuilder.getLanuageFactory();

    /**
     * Constructor for TestLocateFunctionModifier.
     * @param name
     */
    public TestLocateFunctionModifier(String name) {
        super(name);
    }

    public IExpression helpTestMod(IExpression[] args, String expectedStr) throws Exception {
        IFunction func = LANG_FACTORY.createFunction("hour",  //$NON-NLS-1$
            args,
            Integer.class);
        
        LocateFunctionModifier mod = new LocateFunctionModifier(LANG_FACTORY);
        IExpression expr = mod.modify(func);
        
        OracleSQLTranslator trans = new OracleSQLTranslator();
        trans.initialize(EnvironmentUtility.createEnvironment(new Properties(), false));
        
        SQLConversionVisitor sqlVisitor = new SQLConversionVisitor(trans); 
        sqlVisitor.append(expr);  
        
        assertEquals(expectedStr, sqlVisitor.toString());
        
        return expr;
    }

    public void testTwoArgs() throws Exception {
        IExpression[] args = new IExpression[] {
            LANG_FACTORY.createLiteral(".", String.class), //$NON-NLS-1$
            LANG_FACTORY.createLiteral("a.b.c", String.class)             //$NON-NLS-1$
        }; 
        helpTestMod(args, "instr('a.b.c', '.')"); //$NON-NLS-1$
    }

    public void testThreeArgsWithConstant() throws Exception {
        IExpression[] args = new IExpression[] {
            LANG_FACTORY.createLiteral(".", String.class), //$NON-NLS-1$
            LANG_FACTORY.createLiteral("a.b.c", String.class),             //$NON-NLS-1$
            LANG_FACTORY.createLiteral(new Integer(2), Integer.class)            
        }; 
        helpTestMod(args, "instr('a.b.c', '.', 3)"); //$NON-NLS-1$
    }

    public void testThreeArgsWithElement() throws Exception {
        IExpression[] args = new IExpression[] {
            LANG_FACTORY.createLiteral(".", String.class), //$NON-NLS-1$
            LANG_FACTORY.createLiteral("a.b.c", String.class),             //$NON-NLS-1$
            LANG_FACTORY.createElement("e1", null, null, Integer.class)             //$NON-NLS-1$
        }; 
        helpTestMod(args, "instr('a.b.c', '.', (e1 + 1))"); //$NON-NLS-1$
    }

    public void testThreeArgsWithNull() throws Exception {
        IExpression[] args = new IExpression[] {
            LANG_FACTORY.createLiteral(".", String.class), //$NON-NLS-1$
            LANG_FACTORY.createLiteral("a.b.c", String.class),             //$NON-NLS-1$
            LANG_FACTORY.createLiteral(null, Integer.class)            
        }; 
        helpTestMod(args, "instr('a.b.c', '.', NULL)"); //$NON-NLS-1$
    }

}
