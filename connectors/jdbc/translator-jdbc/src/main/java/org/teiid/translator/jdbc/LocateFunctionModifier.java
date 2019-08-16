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

import java.util.Arrays;
import java.util.List;

import org.teiid.language.Comparison;
import org.teiid.language.Comparison.Operator;
import org.teiid.language.Expression;
import org.teiid.language.Function;
import org.teiid.language.LanguageFactory;
import org.teiid.language.Literal;
import org.teiid.translator.TypeFacility;


/**
 * A modifier class that can be used to translate the scalar function
 * <code>locate(search_string, source_string)</code> and
 * <code>locate(search_string, source_string, start_index)</code> to a function
 * or expression that can be used at the data source.
 * <p>
 * If the default implementation is used, a function name of LOCATE will be used
 * for the function name.
 * <p>
 * If the default implementation is used, the expression will not be modified if:
 * <ul>
 * <li><code>locate(search_string, source_string)</code> is used</li>
 * <li><code>locate(search_string, source_string, start_index)</code> is used
 * and <code>start_index</code> is a literal integer greater then 0</li>
 * <li>the default function parameter order is used or unspecified</li>
 * </ul>
 * <p>
 * If the default implementation is used, the expression will be modified if:
 * <ul>
 * <li><code>locate(search_string, source_string, start_index)</code> is used
 * and <code>start_index</code> is a literal integer less then 1</li>
 * <li><code>locate(search_string, source_string, start_index)</code> is used
 * and <code>start_index</code> is not a literal integer</li>
 * <li>the function parameter order is something other than the default</li>
 * </ul>
 * <p>
 * If the default implementation is used and the expression is modified, it is
 * modified to ensure that any literal integer value less than 1 is made equal
 * to 1 and any non literal value is wrapped by a searched case expression
 * to ensure that a value of less then 1 will be equal to 1 and the parameter
 * order matches that of what the data source expects.
 * <p>
 * For example:
 * <ul>
 * <li><code>locate('a', 'abcdef')</code> --&gt; <code>LOCATE('a', 'abcdef')</code></li>
 * <li><code>locate('a', 'abcdef', 2)</code> --&gt; <code>LOCATE('a', 'abcdef', 2)</code></li>
 * <li><code>locate('a', 'abcdef', 0)</code> --&gt; <code>LOCATE('a', 'abcdef', 1)</code></li>
 * <li><code>locate('a', 'abcdef', intCol)</code> --&gt; <code>LOCATE('a', 'abcdef', CASE WHEN intCol &lt; 1 THEN 1 ELSE intCol END)</code></li>
 * </ul>
 * @since 6.2
 */
public class LocateFunctionModifier extends AliasModifier {

    public static String LOCATE = "LOCATE"; //$NON-NLS-1$

    private LanguageFactory langFactory;
    private boolean sourceStringFirst;

    /**
     * Translates the scalar function LOCATE() to a source specific scalar
     * function or expression.
     *
     * @param langFactory the language factory associated with translation
     */
    public LocateFunctionModifier(LanguageFactory langFactory) {
        this(langFactory, LOCATE, false);
    }

    /**
     * Translates the scalar function LOCATE() to a source specific scalar
     * function or expression.
     *
     * @param langFactory the language factory associated with translation
     * @param functionName the function name or alias to be used instead of LOCATE
     * @param sourceStringFirst
     */
    public LocateFunctionModifier(LanguageFactory langFactory, final String functionName, boolean sourceStringFirst) {
        super(functionName);
        this.langFactory = langFactory;
        this.sourceStringFirst = sourceStringFirst;
    }

    /**
     * Returns a version of <code>function</code> suitable for executing at the
     * data source.
     * <p>
     * For example:
     * <code>locate('a', 'abcdefg')  ---&gt;  LOCATE('a', 'abcdefg')</code><br>
     * <code>locate('a', 'abcdefg', 1)  ---&gt;  LOCATE('a', 'abcdefg', 1)</code><br>
     * <code>locate('a', 'abcdefg', 1)  ---&gt;  INSTR('abcdefg', 'a', 1)</code><br>
     * <code>locate('a', 'abcdefg', -5)  ---&gt;  INSTR('abcdefg', 'a', 1)</code><br>
     * <code>locate('a', 'abcdefg', 1)  ---&gt;  FINDSTR('a', 'abcdefg', 1)</code><br>
     * <code>locate('a', 'abcdefg', myCol)  ---&gt;  LOCATE('a', 'abcdefg', CASE WHEN myCol &lt; 1 THEN 1 ELSE myCol END)</code>
     *
     * @param function the LOCATE function that may need to be modified
     */
    public void modify(Function function) {
        super.modify(function);
        List<Expression> args = function.getParameters();
        Expression searchStr = args.get(0);
        Expression sourceStr = args.get(1);

        // if startIndex was given then we may need to do additional work
        if (args.size() > 2) {
            args.set(2, ensurePositiveStartIndex(args.get(2)));
        }
        if (sourceStringFirst) {
            args.set(0, sourceStr);
            args.set(1, searchStr);
        }
    }

    private Expression ensurePositiveStartIndex(Expression startIndex) {
        if (startIndex instanceof Literal) {
            Literal literal = (Literal)startIndex;
            if (literal.getValue() instanceof Integer && ((Integer)literal.getValue() < 1)) {
                literal.setValue(1);
            }
        } else {
            Comparison whenExpr = langFactory.createCompareCriteria(
                    Operator.LT,
                    startIndex,
                    langFactory.createLiteral(1, Integer.class)
                );
            Literal thenExpr = langFactory.createLiteral(1, Integer.class);
            startIndex = langFactory.createSearchedCaseExpression(Arrays.asList(langFactory.createSearchedWhenCondition(whenExpr, thenExpr)), startIndex, TypeFacility.RUNTIME_TYPES.INTEGER);
        }
        return startIndex;
    }

    /**
     * Get the instance of {@link LanguageFactory} set during construction.
     *
     * @return the <code>ILanguageFactory</code> instance
     */
    protected LanguageFactory getLanguageFactory() {
        return this.langFactory;
    }

}
