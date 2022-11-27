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

package org.teiid.translator.jdbc.modeshape;

import java.util.Arrays;
import java.util.Collections;

import junit.framework.TestCase;

import org.teiid.language.Expression;
import org.teiid.language.Function;
import org.teiid.language.LanguageFactory;
import org.teiid.language.Literal;
import org.teiid.translator.jdbc.SQLConversionVisitor;


/**
 */
@SuppressWarnings("nls")
public class TestPathFunctionModifier extends TestCase {

    private static final LanguageFactory LANG_FACTORY = new LanguageFactory();

    /**
     * Constructor for TestMonthFunctionModifier.
     * @param name
     */
    public TestPathFunctionModifier(String name) {
        super(name);
    }

    public void helpTestMod(Expression c, String expectedStr, String target) throws Exception {
        Function func = null;
        if (c != null) {
            func = LANG_FACTORY.createFunction(target,
            Arrays.asList(c),
            String.class);
        } else {
            func = LANG_FACTORY.createFunction(target,
                    Collections.EMPTY_LIST,
                    String.class);

        }

        ModeShapeExecutionFactory trans = new ModeShapeExecutionFactory();
        trans.start();

        SQLConversionVisitor sqlVisitor = trans.getSQLConversionVisitor();

        sqlVisitor.append(func);
        assertEquals(expectedStr, sqlVisitor.toString());
    }


    public void test1() throws Exception {
        Literal arg1 = LANG_FACTORY.createLiteral("car", String.class); //$NON-NLS-1$
        helpTestMod(arg1, "PATH('car')", "PATH"); //$NON-NLS-1$
    }

    public void test2() throws Exception {
        helpTestMod(null, "PATH()", "PATH"); //$NON-NLS-1$
    }


}

