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
            "substr('1234214', 1, 11)"); //$NON-NLS-1$
    }

}