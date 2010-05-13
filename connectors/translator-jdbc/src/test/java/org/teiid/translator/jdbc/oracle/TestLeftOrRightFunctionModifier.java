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

package org.teiid.translator.jdbc.oracle;

import java.util.Arrays;

import junit.framework.TestCase;

import org.teiid.language.Function;
import org.teiid.language.LanguageFactory;
import org.teiid.language.Literal;
import org.teiid.translator.jdbc.SQLConversionVisitor;

/**
 */
public class TestLeftOrRightFunctionModifier extends TestCase {

    private static final LanguageFactory LANG_FACTORY = new LanguageFactory();

    /**
     * Constructor for TestHourFunctionModifier.
     * @param name
     */
    public TestLeftOrRightFunctionModifier(String name) {
        super(name);
    }

    public void helpTestMod(Literal c, Literal d, String target, String expectedStr) throws Exception {
        Function func = LANG_FACTORY.createFunction(target,
            Arrays.asList( c, d ),
            String.class);
        
        OracleExecutionFactory trans = new OracleExecutionFactory();
        trans.start();
        
        SQLConversionVisitor sqlVisitor = trans.getSQLConversionVisitor(); 
        sqlVisitor.append(func);  
        assertEquals(expectedStr, sqlVisitor.toString());
    }

    public void test1() throws Exception {
        Literal arg1 = LANG_FACTORY.createLiteral("1234214", String.class); //$NON-NLS-1$
        Literal count = LANG_FACTORY.createLiteral(new Integer(11), Integer.class);
        helpTestMod(arg1, count, "left", //$NON-NLS-1$
            "SUBSTR('1234214', 1, 11)"); //$NON-NLS-1$
    }
    
    public void test2() throws Exception {
        Literal arg1 = LANG_FACTORY.createLiteral("1234214", String.class); //$NON-NLS-1$
        Literal count = LANG_FACTORY.createLiteral(new Integer(2), Integer.class);
        helpTestMod(arg1, count, "right", //$NON-NLS-1$
            "SUBSTR('1234214', (-1 * 2))"); //$NON-NLS-1$
    }
}