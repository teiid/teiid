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

import org.teiid.connector.jdbc.translator.BasicFunctionModifier;
import org.teiid.connector.jdbc.translator.FunctionModifier;
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
public class LOCATEFunctionModifier extends BasicFunctionModifier implements FunctionModifier {

	/**
	 * An <code>enum</code> that defines the parameter orders that can be used 
	 * with <code>LOCATEFunctionModifier</code>
	 * 
	 */
	public static enum ParameterOrder {
		/**
		 * Indicates that the parameter order should be consistent with the 
		 * built-in system function <code>LOCATE(searchStr, sourceStr, startIndex)</code>.
		 */
		DEFAULT,
		
		/**
		 * Indicates that the parameter order should be changed from the default 
		 * built-in system function <code>LOCATE(searchStr, sourceStr, startIndex)</code> 
		 * to <code>LOCATE(sourceStr, searchStr, startIndex)</code>.
		 */
		SOURCE_SEARCH_INDEX,

		/**
		 * Indicates that the parameter order should be changed from the default 
		 * built-in system function <code>LOCATE(searchStr, sourceStr, startIndex)</code> 
		 * to <code>LOCATE(startIndex, sourceStr, searchStr)</code>.
		 */
		INDEX_SOURCE_SEARCH 

	}
	
    private ILanguageFactory langFactory;
    private String functionName = "LOCATE"; //$NON-NLS-1$
    private ParameterOrder parameterOrder = ParameterOrder.DEFAULT;
    private final Integer systemStringIndexBase = 1;
    
	/**
	 * Constructs a {@link BasicFunctionModifier} object that can be used to 
	 * translate the scalar function LOCATE() to a source specific scalar 
	 * function or expression.
	 * <p>
	 * This constructor invokes {@link #LOCATEFunctionModifier(ILanguageFactory, String, ParameterOrder)}
	 * passing it <code>langFactory</code>, <code>null</code>, <code>null</code>.
	 * 
	 * @param langFactory the language factory associated with translation
	 */
    public LOCATEFunctionModifier(ILanguageFactory langFactory) {
    	this(langFactory, null, null);
    }

	/**
	 * Constructs a {@link BasicFunctionModifier} object that can be used to 
	 * translate the scalar function LOCATE() to a source specific scalar 
	 * function or expression.
	 * <p>
	 * This constructor invokes {@link #LOCATEFunctionModifier(ILanguageFactory, String, ParameterOrder)}
	 * passing it <code>langFactory</code>, <code>functionName</code>, 
	 * <code>null</code>.
	 * 
	 * @param langFactory the language factory associated with translation
	 * @param functionName the function name or alias to be used instead of LOCATE
	 */
    public LOCATEFunctionModifier(ILanguageFactory langFactory, final String functionName) {
    	this(langFactory, functionName, null);
    }

	/**
	 * Constructs a {@link BasicFunctionModifier} object that can be used to 
	 * translate the scalar function LOCATE() to a source specific scalar 
	 * function or expression.
	 * <p>
	 * <code>functionName</code> should represent the default function name or 
	 * alias used by the data source.  If this value is <code>null</code> a 
	 * default value will be used.
	 * <p>
	 * <code>paramOrder</code> should represent how the data source's version of
	 * the LOCATE() function expects its parameters.  This value can be any 
	 * supported value offered by {@link ParameterOrder}.  If this value is 
	 * <code>null</code> a default value will be used.
	 * <p> 
	 * 
	 * @param langFactory the language factory associated with translation
	 * @param functionName the function name or alias to be used instead of 
	 *        LOCATE, or <code>null</code> if the default should be used
	 * @param paramOrder the order in which parameters should be translated, or 
	 *        <code>null</code> if the default should be used
	 */
    public LOCATEFunctionModifier(ILanguageFactory langFactory, final String functionName, final ParameterOrder paramOrder) {
    	if (functionName != null) this.functionName = functionName;
        if (paramOrder != null) this.parameterOrder = paramOrder;
        this.langFactory = langFactory;
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
        IExpression startIndex = (args.size() > 2 ? args.get(2) : null);

        // if startIndex was given then we may need to do additional work
        if (startIndex != null) {
        	if (startIndex instanceof ILiteral) {
        		startIndex = this.getStartIndexExpression((ILiteral)startIndex);
        	} else {
        		startIndex = this.getStartIndexExpression((IExpression)startIndex);
        	}
        }
		switch (this.parameterOrder) {
		case SOURCE_SEARCH_INDEX:
			args.set(0, sourceStr);
			args.set(1, searchStr);
			if (startIndex != null) args.set(2, startIndex);
			break;
		case INDEX_SOURCE_SEARCH:
			if (startIndex != null) args.set(0, startIndex);
			args.set(1, sourceStr);
			args.set(2, searchStr);
			break;
		case DEFAULT:
			args.set(0, searchStr);
			args.set(1, sourceStr);
			if (startIndex != null) args.set(2, startIndex);
			break;
		}
        return function;           
    }

	/**
	 * Return an expression that represents <code>startIndex</code> rewritten to 
	 * be consistent with the built-in system function's 
	 * <code>LOCATE(searchStr, sourceStr, startIndex)</code> <code>startIndex</code>
	 * parameter.
	 * <p>
	 * <code>startIndex</code> represents the unmodified parameter as passed to 
	 * the <code>LOCATE(searchStr, sourceStr, startIndex)</code> function.  The 
	 * returned value will represent a normalized version of the expression that 
	 * is consistent to the built-in system function.  For example, a value for  
	 * <code>startIndex</code> should not be less than <code>1</code>.  
	 * <p>
	 * If this method is not overriden, the result will be:
	 * <p>
	 * <ul>If <code>startIndex</code> is <code>null</code>, <code>startIndex</code></ul>
	 * <ul>If <code>startIndex</code> is not <code>null</code> and its value is 
	 * less than <code>1</code>, <code>1</code></ul>
	 *   
	 * @param startIndex an expression representing the <code>startIndex</code> 
	 *        parameter used in <code>LOCATE(searchStr, sourceStr, startIndex)</code>
	 * @return an expression that represents a normalized <code>startIndex</code>
	 */
    protected IExpression getStartIndexExpression(ILiteral startIndex) {
    	if (startIndex.getValue() != null) {
			if ((Integer)startIndex.getValue() < this.getSystemStringIndexBase()) {
				startIndex.setValue(this.getSystemStringIndexBase());
			}
    	}
    	return startIndex;
    }

	/**
	 * Return an expression that represents <code>startIndex</code> rewritten to 
	 * be consistent with the built-in system function's 
	 * <code>LOCATE(searchStr, sourceStr, startIndex)</code> <code>startIndex</code>
	 * parameter.
	 * <p>
	 * <code>startIndex</code> represents the unmodified parameter as passed to 
	 * the <code>LOCATE(searchStr, sourceStr, startIndex)</code> function.  The 
	 * returned value will represent a normalized version of the expression that 
	 * is consistent to the built-in system function.  For example, a value for  
	 * <code>startIndex</code> should not be less than <code>1</code>.  
	 * <p>
	 * If this method is not overriden, the result will be:
	 * <p>
	 * <ul><code>CASE WHEN &lt;startIndex&gt; &lt; 1; THEN 1; ELSE &lt;startIndex&gt; END</code></ul>
	 * <p>
	 * For the default searched case expression to work, the source must support 
	 * searched case.
	 *   
	 * @param startIndex an expression representing the <code>startIndex</code> 
	 *        parameter used in <code>LOCATE(searchStr, sourceStr, startIndex)</code>
	 * @return an expression that represents a normalized <code>startIndex</code>
	 */
    protected IExpression getStartIndexExpression(IExpression startIndex) {
    	ICompareCriteria[] whenExpr = {langFactory.createCompareCriteria(
    			Operator.LT, 
    			startIndex, 
    			langFactory.createLiteral(this.getSystemStringIndexBase(), Integer.class)
    		)};
    	ILiteral[] thenExpr = {langFactory.createLiteral(this.getSystemStringIndexBase(), Integer.class)};
    	return langFactory.createSearchedCaseExpression(Arrays.asList(whenExpr), Arrays.asList(thenExpr), startIndex, Integer.class);
    }

    /**
     * Get the string index base used by built-in system functions.  The value  
     * represents what is considered the first character of a string.
     * 
     * @return the data source's string index base
     */
    protected Integer getSystemStringIndexBase() {
    	return this.systemStringIndexBase;
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
