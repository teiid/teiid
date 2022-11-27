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
import java.util.List;

import junit.framework.TestCase;

import org.teiid.language.Expression;
import org.teiid.language.Function;
import org.teiid.language.LanguageFactory;
import org.teiid.language.Literal;
import org.teiid.language.visitor.SQLStringVisitor;
import org.teiid.translator.jdbc.oracle.Log10FunctionModifier;

/**
 */
public class TestLog10FunctionModifier extends TestCase {
    private static final LanguageFactory LANG_FACTORY = new LanguageFactory();

    /**
     * Constructor for TestLog10FunctionModifier.
     * @param name
     */
    public TestLog10FunctionModifier(String name) {
        super(name);
    }

    public void testModifier() {
        Literal arg = LANG_FACTORY.createLiteral(new Double(5.2), Double.class);
        Function func = LANG_FACTORY.createFunction("log10", Arrays.asList(arg), Double.class); //$NON-NLS-1$

        Log10FunctionModifier modifier = new Log10FunctionModifier(LANG_FACTORY);
        modifier.translate(func);

        assertEquals("log", func.getName()); //$NON-NLS-1$
        assertEquals(Double.class, func.getType());

        List<Expression> outArgs = func.getParameters();
        assertEquals(2, outArgs.size());
        assertEquals(arg, outArgs.get(1));

        assertTrue(outArgs.get(1) instanceof Literal);
        Literal newArg = (Literal) outArgs.get(0);
        assertEquals(Integer.class, newArg.getType());
        assertEquals(new Integer(10), newArg.getValue());

        assertEquals("log(10, 5.2)", SQLStringVisitor.getSQLString(func));              //$NON-NLS-1$
    }
}
