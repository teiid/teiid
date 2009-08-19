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

package org.teiid.connector.jdbc.translator;

import java.util.Arrays;
import java.util.List;

import org.teiid.connector.language.ICompareCriteria;
import org.teiid.connector.language.IExpression;
import org.teiid.connector.language.IFunction;
import org.teiid.connector.language.ILanguageFactory;
import org.teiid.connector.language.ILiteral;
import org.teiid.connector.language.ICompareCriteria.Operator;


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
 * <li><code>locate(search_string, source_string)</code> is used</li>
 * <li><code>locate(search_string, source_string, start_index)</code> is used 
 * and <code>start_index</code> is a literal integer greater then 0</li>
 * <li>the default function parameter order is used or unspecified</li>
 * <p>
 * If the default implementation is used, the expression will be modified if: 
 * <li><code>locate(search_string, source_string, start_index)</code> is used 
 * and <code>start_index</code> is a literal integer less then 1</li>  
 * <li><code>locate(search_string, source_string, start_index)</code> is used 
 * and <code>start_index</code> is not a literal integer</li> 
 * <li>the function parameter order is something other than the default</li>
 * <p>
 * If the default implementation is used and the expression is modified, it is 
 * modified to ensure that any literal integer value less than 1 is made equal 
 * to 1 and any non literal value is wrapped by a searched case expression 
 * to ensure that a value of less then 1 will be equal to 1 and the parameter 
 * order matches that of what the data source expects.
 * <p>
 * For example:
 * <li><code>locate('a', 'abcdef')</code> --> <code>LOCATE('a', 'abcdef')</code></li>
 * <li><code>locate('a', 'abcdef', 2)</code> --> <code>LOCATE('a', 'abcdef', 2)</code></li>
 * <li><code>locate('a', 'abcdef', 0)</code> --> <code>LOCATE('a', 'abcdef', 1)</code></li>
 * <li><code>locate('a', 'abcdef', intCol)</code> --> <code>LOCATE('a', 'abcdef', CASE WHEN intCol < 1 THEN 1 ELSE intCol END)</code></li>
 * 
 * @since 6.2
 */
public class LocateFunctionModifier extends BasicFunctionModifier {

	public static String LOCATE = "LOCATE"; //$NON-NLS-1$
	
    private ILanguageFactory langFactory;
    private String functionName = LOCATE;
    private boolean sourceStringFirst;
    
	/**
	 * Constructs a {@link BasicFunctionModifier} object that can be used to 
	 * translate the scalar function LOCATE() to a source specific scalar 
	 * function or expression.
	 * 
	 * @param langFactory the language factory associated with translation
	 */
    public LocateFunctionModifier(ILanguageFactory langFactory) {
    	this(langFactory, LOCATE, false);
    }

	/**
	 * Constructs a {@link BasicFunctionModifier} object that can be used to 
	 * translate the scalar function LOCATE() to a source specific scalar 
	 * function or expression.
	 * 
	 * @param langFactory the language factory associated with translation
	 * @param functionName the function name or alias to be used instead of LOCATE
	 * @param sourceStringFirst
	 */
    public LocateFunctionModifier(ILanguageFactory langFactory, final String functionName, boolean sourceStringFirst) {
    	this.langFactory = langFactory;
    	this.functionName = functionName;
    	this.sourceStringFirst = sourceStringFirst;
    }

	/**
	 * Returns a version of <code>function</code> suitable for executing at the 
	 * data source.
	 * <p>
	 * First, a default function name or the value specified during construction 
	 * of <code>MODFunctionModifier</code> is set on <code>function</code>.
	 * <p>
	 * If <code>function</code> represents <code>LOCATE(searchStr, sourceStr, startIndex)</code>
	 * and <code>startIndex</code> is a literal value, it is translated for 
	 * consistency between the built-in system function 
	 * <code>LOCATE(searchStr, sourceStr, startIndex)</code> and the sources 
	 * implementation.  This is done by calling {@link #getStartIndexExpression(ILiteral)} 
	 * and passing it the literal <code>startIndex</code> value.
	 * <p>
	 * If <code>function</code> represents <code>LOCATE(searchStr, sourceStr, startIndex)</code>
	 * and <code>startIndex</code> is not a literal value, it is translated for 
	 * consistency between the built-in system function 
	 * <code>LOCATE(searchStr, sourceStr, startIndex)</code> and the sources 
	 * implementation.  This is done by calling {@link #getStartIndexExpression(IExpression)} 
	 * and passing it the non-literal <code>startIndex</code> value.
	 * <p>
	 * Finally, <code>function</code>'s parameters may be rearranged depending 
	 * on the value specified by {@link ParameterOrder} during construction of 
	 * <code>MODFunctionModifier</code>.
	 * <p>
	 * The translated <code>function</code> is then returned.
	 * <p>
	 * For example:
	 * <ul>
	 * <code>locate('a', 'abcdefg')  --->  LOCATE('a', 'abcdefg')</code><br />
	 * <code>locate('a', 'abcdefg', 1)  --->  LOCATE('a', 'abcdefg', 1)</code><br />
	 * <code>locate('a', 'abcdefg', 1)  --->  INSTR('abcdefg', 'a', 1)</code><br />
	 * <code>locate('a', 'abcdefg', -5)  --->  INSTR('abcdefg', 'a', 1)</code><br />
	 * <code>locate('a', 'abcdefg', 1)  --->  FINDSTR('a', 'abcdefg', 1)</code><br />
	 * <code>locate('a', 'abcdefg', myCol)  --->  LOCATE('a', 'abcdefg', CASE WHEN myCol < 1 THEN 1 ELSE myCol END)</code>
	 * </ul>
	 * 
	 * @param function the LOCATE function that may need to be modified
	 */
    public IExpression modify(IFunction function) {
    	function.setName(this.functionName);
        List<IExpression> args = function.getParameters();
        IExpression searchStr = args.get(0);
        IExpression sourceStr = args.get(1);

        // if startIndex was given then we may need to do additional work
        if (args.size() > 2) {
        	args.set(2, ensurePositiveStartIndex(args.get(2)));
        }
        if (sourceStringFirst) {
			args.set(0, sourceStr);
			args.set(1, searchStr);
        }
        return function;
    }

	private IExpression ensurePositiveStartIndex(IExpression startIndex) {
		if (startIndex instanceof ILiteral) {
			ILiteral literal = (ILiteral)startIndex;  
			if (literal.getValue() != null && ((Integer)literal.getValue() < 1)) {
				literal.setValue(1);
			}
		} else {
			ICompareCriteria[] whenExpr = {langFactory.createCompareCriteria(
					Operator.LT, 
					startIndex, 
					langFactory.createLiteral(1, Integer.class)
				)};
			ILiteral[] thenExpr = {langFactory.createLiteral(1, Integer.class)};
			startIndex = langFactory.createSearchedCaseExpression(Arrays.asList(whenExpr), Arrays.asList(thenExpr), startIndex, Integer.class);
		}
		return startIndex;
	}
	
	/**
     * Get the instance of {@link ILanguageFactory} set during construction.
     * 
     * @return the <code>ILanguageFactory</code> instance
     */
	protected ILanguageFactory getLanguageFactory() {
		return this.langFactory;
	}

}
