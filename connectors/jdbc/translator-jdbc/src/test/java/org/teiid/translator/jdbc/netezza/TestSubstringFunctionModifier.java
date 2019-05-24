/*
 * Copyright Red Hat, Inc. and/or its affiliates
 * and other contributors as indicated by the @author tags and
 * the COPYRIGHT.txt file distributed with this work.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
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
@SuppressWarnings("nls")
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
