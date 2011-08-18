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

package org.teiid.translator.jdbc.netezza;

import java.util.Arrays;

import junit.framework.TestCase;

import org.teiid.language.Expression;
import org.teiid.language.Function;
import org.teiid.language.LanguageFactory;
import org.teiid.translator.TypeFacility;
import org.teiid.translator.jdbc.SQLConversionVisitor;

/**
 */
public class TestSubstringFunctionModifier extends TestCase {

    private static final LanguageFactory LANG_FACTORY = new LanguageFactory();


    /**
     * Constructor for TestSubstringFunctionModifier.
     * @param name
     */
    public TestSubstringFunctionModifier(String name) {
        super(name);
    }

    public void helpTestMod(Expression[] args, String expectedStr) throws Exception {
        Function func = LANG_FACTORY.createFunction("substring",  
            Arrays.asList(args), TypeFacility.RUNTIME_TYPES.STRING);
        
        NetezzaExecutionFactory trans = new NetezzaExecutionFactory();
        trans.start();

        SQLConversionVisitor sqlVisitor = trans.getSQLConversionVisitor(); 
        sqlVisitor.append(func);  
        
        assertEquals(expectedStr, sqlVisitor.toString());
    }

    public void testTwoArgs() throws Exception {
        Expression[] args = new Expression[] {
            LANG_FACTORY.createLiteral("a.b.c", String.class), 
            LANG_FACTORY.createLiteral(new Integer(1), Integer.class)           
        }; 
        helpTestMod(args, "substring('a.b.c', 1)"); 
    }

    public void testThreeArgsWithConstant() throws Exception {
        Expression[] args = new Expression[] {
            LANG_FACTORY.createLiteral("a.b.c", String.class), 
            LANG_FACTORY.createLiteral(new Integer(3), Integer.class),
            LANG_FACTORY.createLiteral(new Integer(1), Integer.class) 
        }; 
        helpTestMod(args, "substring('a.b.c', 3, 1)"); 
    }

    public void testThreeArgsWithElement() throws Exception {
        Expression[] args = new Expression[] {
            LANG_FACTORY.createLiteral("a.b.c", String.class), 
            LANG_FACTORY.createColumnReference("e1", null, null, Integer.class), 
            LANG_FACTORY.createLiteral(new Integer(1), Integer.class) 
        }; 
        helpTestMod(args, "substring('a.b.c', e1, 1)"); 
    }

    public void testThreeArgsWithNull() throws Exception {
        Expression[] args = new Expression[] {
            LANG_FACTORY.createLiteral("a.b.c", String.class), 
            LANG_FACTORY.createLiteral(null, Integer.class),
            LANG_FACTORY.createLiteral(new Integer(5), Integer.class) 
        }; 
        helpTestMod(args, "substring('a.b.c', NULL, 5)"); 
    }

}
