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

package org.teiid.translator.jdbc;

import static org.junit.Assert.*;

import java.util.Arrays;

import org.junit.Test;
import org.teiid.language.Expression;
import org.teiid.language.Function;
import org.teiid.language.LanguageFactory;
import org.teiid.translator.SourceSystemFunctions;
import org.teiid.translator.TranslatorException;

/**
 * Test <code>LOCATEFunctionModifier</code> by invoking its methods with varying
 * parameters to validate it performs as designed and expected.
 */
public class TestLocateFunctionModifier {

    private static final LanguageFactory LANG_FACTORY = new LanguageFactory();

    /**
     * Create an expression containing a LOCATE function using <code>args</code>
     * and pass it to the <code>Translator</code>'s LOCATE function modifier and
     * compare the resulting expression to <code>expectedStr</code>.
     *
     * @param args An array of <code>IExpression</code>'s to use as the
     *             arguments to the LOCATE() function
     * @param expectedStr A string representing the modified expression
     * @throws Exception
     */
    public void helpTestLocate(Expression[] args, String expectedStr) throws Exception {
        this.helpTestLocate(LocateFunctionModifier.LOCATE, false, args, expectedStr);
    }

    /**
     * Create an expression containing a LOCATE function using a function name of
     * <code>locateFunctionName</code> with the parameter order of
     * <code>parameterOrder</code> and a string index base of
     * <code>stringIndexBase</code> and uses the arguments <code>args</code> and
     * pass it to the <code>Translator</code>'s LOCATE function modifier and
     * compare the resulting expression to <code>expectedStr</code>.
     *
     * @param locateFunctionName the name to use for the function modifier
     * @param args an array of <code>IExpression</code>'s to use as the
     *             arguments to the LOCATE() function
     * @param expectedStr A string representing the modified expression
     * @throws Exception
     */
    public void helpTestLocate(final String locateFunctionName, final boolean parameterOrder, Expression[] args, String expectedStr) throws Exception {
        Expression param1 = null;
        Expression param2 = null;
        Expression param3 = null;

        if (args.length > 0 ) param1 = args[0];
        if (args.length > 1 ) param2 = args[1];
        if (args.length > 2 ) param3 = args[2];

        Function func = null;

        if (param3 != null) {
            func = LANG_FACTORY.createFunction(SourceSystemFunctions.LOCATE,
                    Arrays.asList(param1, param2, param3), Integer.class);
        } else {
            func = LANG_FACTORY.createFunction(SourceSystemFunctions.LOCATE,
                    Arrays.asList(param1, param2), Integer.class);
        }

        JDBCExecutionFactory trans = new JDBCExecutionFactory() {
            @Override
            public void start() throws TranslatorException {
                super.start();
                registerFunctionModifier(SourceSystemFunctions.LOCATE, new LocateFunctionModifier(getLanguageFactory(), locateFunctionName, parameterOrder));
            }
        };
        trans.setUseBindVariables(false);
        trans.start();

        SQLConversionVisitor sqlVisitor = trans.getSQLConversionVisitor();
        sqlVisitor.append(func);

        assertEquals("Modified function does not match", expectedStr, sqlVisitor.toString()); //$NON-NLS-1$
    }

    /**
     * Test {@link LocateFunctionModifier#modify(Function)} to validate a call
     * to LOCATE(search_str, source_str) using constants for both parameters
     * returns LOCATE(search_str, source_str).
     * <p>
     * {@link LocateFunctionModifier} will be constructed without specifying a
     * function name or parameter order.
     *
     * @throws Exception
     */
    @Test public void testModifySimple() throws Exception {
        Expression[] args = new Expression[] {
                LANG_FACTORY.createLiteral("a", String.class), //$NON-NLS-1$
                LANG_FACTORY.createLiteral("abcdefg", String.class) //$NON-NLS-1$
        };
        // default / default
        helpTestLocate(args, "LOCATE('a', 'abcdefg')"); //$NON-NLS-1$
    }

    /**
     * Test {@link LocateFunctionModifier#modify(Function)} to validate a call
     * to LOCATE(search_str, source_str) using constants for both parameters
     * returns locate(search_str, source_str).
     * <p>
     * {@link LocateFunctionModifier} will be constructed specifying a function
     * name of locate but no parameter order.
     *
     * @throws Exception
     */
    @Test public void testModifySimple2() throws Exception {
        Expression[] args = new Expression[] {
                LANG_FACTORY.createLiteral("a", String.class), //$NON-NLS-1$
                LANG_FACTORY.createLiteral("abcdefg", String.class) //$NON-NLS-1$
        };
        // locate / default
        helpTestLocate("locate", false, args, "locate('a', 'abcdefg')"); //$NON-NLS-1$ //$NON-NLS-2$
    }

    /**
     * Test {@link LocateFunctionModifier#modify(Function)} to validate a call
     * to LOCATE(search_str, source_str) using constants for both parameters
     * returns INSTR(source_str, search_str).
     * <p>
     *
     * @throws Exception
     */
    @Test public void testModifySimple3() throws Exception {
        Expression[] args = new Expression[] {
                LANG_FACTORY.createLiteral("a", String.class), //$NON-NLS-1$
                LANG_FACTORY.createLiteral("abcdefg", String.class) //$NON-NLS-1$
        };
        // INSTR / SOURCE_SEARCH_INDEX
        helpTestLocate("INSTR", true, args, "INSTR('abcdefg', 'a')"); //$NON-NLS-1$ //$NON-NLS-2$
    }

    /**
     * Test {@link LocateFunctionModifier#modify(Function)} to validate a call
     * to LOCATE(search_str, source_str) using constants for both parameters
     * returns locate(search_str, source_str).
     * <p>
     *
     * @throws Exception
     */
    @Test public void testModifySimple4() throws Exception {
        Expression[] args = new Expression[] {
                LANG_FACTORY.createLiteral("a", String.class), //$NON-NLS-1$
                LANG_FACTORY.createLiteral("abcdefg", String.class) //$NON-NLS-1$
        };
        // locate / DEFAULT
        helpTestLocate("locate", false, args, "locate('a', 'abcdefg')"); //$NON-NLS-1$ //$NON-NLS-2$
    }

    /**
     * Test {@link LocateFunctionModifier#modify(Function)} to validate a call
     * to LOCATE(search_str, source_str, 1) using constants for all parameters
     * returns INSTR(source_str, search_str, 1).
     * <p>
     *
     * @throws Exception
     */
    @Test public void testModifyWithStartIndex() throws Exception {
        Expression[] args = new Expression[] {
                LANG_FACTORY.createLiteral("a", String.class), //$NON-NLS-1$
                LANG_FACTORY.createLiteral("abcdefg", String.class), //$NON-NLS-1$
                LANG_FACTORY.createLiteral(1, Integer.class)
        };
        // INSTR / SOURCE_SEARCH_INDEX
        helpTestLocate("INSTR", true, args, "INSTR('abcdefg', 'a', 1)"); //$NON-NLS-1$ //$NON-NLS-2$
    }

    /**
     * Test {@link LocateFunctionModifier#modify(Function)} to validate a call
     * to LOCATE(search_str, source_str, 4) using constants for all parameters
     * returns LOCATE(search_str, source_str, 5).
     * <p>
     * {@link LocateFunctionModifier} will be constructed specifying no function
     * name or parameter order.
     *
     * @throws Exception
     */
    @Test public void testModifyWithStartIndex2() throws Exception {
        Expression[] args = new Expression[] {
                LANG_FACTORY.createLiteral("a", String.class), //$NON-NLS-1$
                LANG_FACTORY.createLiteral("abcdefg", String.class), //$NON-NLS-1$
                LANG_FACTORY.createLiteral(4, Integer.class)
        };
        // default / default
        helpTestLocate(args, "LOCATE('a', 'abcdefg', 4)"); //$NON-NLS-1$
    }

    /**
     * Test {@link LocateFunctionModifier#modify(Function)} to validate a call
     * to LOCATE(search_str, source_str, -5) using constants for all parameters
     * returns LOCATE(search_str, source_str, 1).
     * <p>
     * {@link LocateFunctionModifier} will be constructed specifying no function
     * name or parameter order.
     *
     * @throws Exception
     */
    @Test public void testModifyWithStartIndex3() throws Exception {
        Expression[] args = new Expression[] {
                LANG_FACTORY.createLiteral("a", String.class), //$NON-NLS-1$
                LANG_FACTORY.createLiteral("abcdefg", String.class), //$NON-NLS-1$
                LANG_FACTORY.createLiteral(-5, Integer.class)
        };
        // default / default
        helpTestLocate(args, "LOCATE('a', 'abcdefg', 1)"); //$NON-NLS-1$
    }

    /**
     * Test {@link LocateFunctionModifier#modify(Function)} to validate a call
     * to LOCATE(search_str, source_str, null) using constants for all parameters
     * returns LOCATE(search_str, source_str, NULL).
     * <p>
     * {@link LocateFunctionModifier} will be constructed specifying no function
     * name or parameter order.
     *
     * @throws Exception
     */
    @Test public void testModifyWithStartIndex4() throws Exception {
        Expression[] args = new Expression[] {
                LANG_FACTORY.createLiteral("a", String.class), //$NON-NLS-1$
                LANG_FACTORY.createLiteral("abcdefg", String.class), //$NON-NLS-1$
                LANG_FACTORY.createLiteral(null, Integer.class)
        };
        // default / default
        helpTestLocate(args, "LOCATE('a', 'abcdefg', NULL)"); //$NON-NLS-1$
    }

    /**
     * Test {@link LocateFunctionModifier#modify(Function)} to validate a call
     * to LOCATE(search_str, source_str, e1) using an element for start index
     * parameter returns INSTR(source_str, search_str, CASE WHEN e1 < 1 THEN 1 ELSE e1 END).
     * <p>
     *
     * @throws Exception
     */
    @Test public void testModifyWithElementStartIndex() throws Exception {
        Expression[] args = new Expression[] {
                LANG_FACTORY.createLiteral("a", String.class), //$NON-NLS-1$
                LANG_FACTORY.createLiteral("abcdefg", String.class), //$NON-NLS-1$
                LANG_FACTORY.createColumnReference("e1", null, null, Integer.class) //$NON-NLS-1$
        };
        // INSTR / SOURCE_SEARCH_INDEX
        helpTestLocate("INSTR", true, args, "INSTR('abcdefg', 'a', CASE WHEN e1 < 1 THEN 1 ELSE e1 END)"); //$NON-NLS-1$ //$NON-NLS-2$
    }

    /**
     * Test {@link LocateFunctionModifier#modify(Function)} to validate a call
     * to LOCATE(search_str, source_str, e1) using an element for start index
     * parameter returns LOCATE(search_str, source_str, CASE WHEN e1 < 0 THEN 0 ELSE e1 END).
     * <p>
     * {@link LocateFunctionModifier} will be constructed specifying no function
     * name and no parameter order.
     *
     * @throws Exception
     */
    @Test public void testModifyWithElementStartIndex2() throws Exception {
        Expression[] args = new Expression[] {
                LANG_FACTORY.createLiteral("a", String.class), //$NON-NLS-1$
                LANG_FACTORY.createLiteral("abcdefg", String.class), //$NON-NLS-1$
                LANG_FACTORY.createColumnReference("e1", null, null, Integer.class) //$NON-NLS-1$
        };
        // default / default
        helpTestLocate(args, "LOCATE('a', 'abcdefg', CASE WHEN e1 < 1 THEN 1 ELSE e1 END)"); //$NON-NLS-1$
    }

}
