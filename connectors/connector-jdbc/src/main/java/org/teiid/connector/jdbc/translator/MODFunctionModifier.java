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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.teiid.connector.api.TypeFacility.RUNTIME_TYPES;
import org.teiid.connector.language.IExpression;
import org.teiid.connector.language.IFunction;
import org.teiid.connector.language.ILanguageFactory;

/**
 * A modifier class that can be used to translate the scalar function 
 * <code>mod(x, y)</code> to a function or expression that can be used at the 
 * data source.
 * <p>
 * If the default implementation is used, a function name of MOD will be used 
 * for the alias name and the expression will be unmodified if the data type 
 * of the <code>x</code> parameter is one of {@link RUNTIME_TYPES#BYTE}, 
 * {@link RUNTIME_TYPES#SHORT}, {@link RUNTIME_TYPES#INTEGER}, or 
 * {@link RUNTIME_TYPES#LONG}.  If the data type is not one of these types, the
 * expression will be modified to return: <code>(x - (TRUNC((x / y), 0) * y))</code>
 * 
 * @since 6.2
 */
public class MODFunctionModifier extends AliasModifier {

	private static List<Class<?>> DEFAULT_TYPELIST = new ArrayList<Class<?>>(4);
	static {
		DEFAULT_TYPELIST.add(RUNTIME_TYPES.BYTE);
		DEFAULT_TYPELIST.add(RUNTIME_TYPES.SHORT);
		DEFAULT_TYPELIST.add(RUNTIME_TYPES.INTEGER);
		DEFAULT_TYPELIST.add(RUNTIME_TYPES.LONG);
	}
	private static String DEFAULT_FUNCTIONNAME = "MOD"; //$NON-NLS-1$

	private ILanguageFactory langFactory;
	private List<Class<?>> supTypeList;
    
	/**
	 * Constructs a {@link AliasModifier} object that can be used to translate 
	 * the use of the scalar function MOD() to a source specific scalar function 
	 * or expression.
	 * <p>
	 * This constructor invokes {@link #MODFunctionModifier(ILanguageFactory, String, List)}
	 * passing it <code>langFactory</code>, {@link #DEFAULT_FUNCTIONNAME}, and 
	 * {@link #DEFAULT_TYPELIST}.
	 * 
	 * @param langFactory the language factory associated with translation
	 */
    public MODFunctionModifier(ILanguageFactory langFactory) {
    	this(langFactory, DEFAULT_FUNCTIONNAME, DEFAULT_TYPELIST);
    }
	
    /**
	 * Constructs a {@link AliasModifier} object that can be used to translate 
	 * the use of the scalar function MOD() to a source specific scalar function 
	 * or expression.
	 * <p>
	 * <code>functionName</code> is used to construct the parent {@link AliasModifier}
	 * and should represent the default function name or alias used by the data 
	 * source.
	 * <p>
	 * <code>supportedTypeList</code> should contain a list of <code>Class</code> 
	 * objects that represent the data types that the data source can support  
	 * with its implementation of the MOD() scalar function.  
     * 
     * @param langFactory the language factory associated with translation
     * @param functionName the function name or alias that should be used 
     *                     instead of MOD
     * @param supportedTypeList a list of type classes that is supported by the 
     *                          data source's MOD function
     */
    public MODFunctionModifier(ILanguageFactory langFactory, String functionName, List<Class<?>>supportedTypeList) {
    	super(functionName);
        this.langFactory = langFactory;
        if ( supportedTypeList != null ) {
        	this.supTypeList = supportedTypeList;
        } else {
        	this.supTypeList = MODFunctionModifier.DEFAULT_TYPELIST;
        }
    }
	
	/**
	 * Returns a version of <code>function</code> suitable for executing at the 
	 * data source.
	 * <p>
	 * If the data type of the parameters in <code>function</code> is in the 
	 * list of supported data types, this method simply returns <code>super.modify(function)</code>.
	 * <p>
	 * If the data type of the parameters in <code>function</code are not in the 
	 * list of supported data types, this method will return an expression that 
	 * is valid at the data source and will yield the same result as the original 
	 * MOD() scalar function.  To build the expression, a call is make to 
	 * {@link #getQuotientExpression(IExpression)} and its result is multiplied 
	 * by the second parameter of <code>function</code> and that result is then 
	 * subtracted from the first parameter of <code>function</code>.
	 * <p>
	 * For example:
	 * <code>mod(x, y)  --->  (x - (getQuotientExpression((x / y)) * y))</code>
	 * 
	 * @param function the MOD function that may need to be modified
	 * @see org.teiid.connector.jdbc.translator.AliasModifier#modify(org.teiid.connector.language.IFunction)
	 */
	@Override
	public IExpression modify(IFunction function) {
		List<IExpression> expressions = function.getParameters();
		IExpression dividend = expressions.get(0);
		IExpression divisor = expressions.get(1);

		// Check to see if parameters are supported by source MOD function
		if (this.supTypeList.contains(dividend.getType())) {
			return super.modify(function);
		}

		/* 
		 * Parameters are not supported by source MOD function so modify 
		 * MOD(<dividend>, <divisor>)  -->  (<dividend> - (<func_getQuotient((<dividend> / <divisor>))> * <divisor>))
		 */
		// -->   (<dividend> / <divisor>)
		IFunction divide = langFactory.createFunction("/", Arrays.asList(dividend, divisor), dividend.getType()); //$NON-NLS-1$
		// -->   <func_getQuotient(<divide>)>  -- i.e. TRUNC(<divide>, 0)
		IFunction quotient = (IFunction) this.getQuotientExpression(divide);
		// -->   (<quotient> * <divisor>)
		List<IExpression> multiplyArgs = Arrays.asList(quotient, divisor);
		IFunction multiply = langFactory.createFunction("*", multiplyArgs, divisor.getType()); //$NON-NLS-1$
		// -->   (<dividend> - <multiply>)
		List<IExpression> minusArgs = Arrays.asList(dividend, multiply);
		return langFactory.createFunction("-", minusArgs, dividend.getType()); //$NON-NLS-1$
	}

	/**
	 * Return an expression that will result in the quotient of </code>division</code>. 
	 * Quotient should always be represented as an integer (no remainder).
	 * <p>
	 * <code>division</code> will represent simple division that may result in a
	 * fraction.  <code>division</code> should be returned within a helper 
	 * function or expression that will result in an integer return value (no 
	 * decimal or fraction).
	 * <p>
	 * If this method is not overriden, the result will be:
	 * <p>
	 * <ul>TRUNC(<code>division</code>, 0)</ul>
	 * <p>
	 * For the default TRUNC() function to work, the source must support it.  
	 * TRUNC was used instead of FLOOR because FLOOR rounds to the nearest
	 * integer toward negative infinity.  This would result in incorrect values 
	 * when performing MOD on negative float, double, etc. values.
	 *   
	 * @param division an expression representing simple division
	 * @return an expression that will extract the quotient from the 
	 *         <code>division</code> expression
	 */
	protected IExpression getQuotientExpression(IExpression division) {
		// -->   TRUNC(<division>, 0)
		return langFactory.createFunction("TRUNC", Arrays.asList(division, langFactory.createLiteral(0, RUNTIME_TYPES.SHORT)), division.getType()); //$NON-NLS-1$
	}

	protected ILanguageFactory getLanguageFactory() {
		return this.langFactory;
	}
	
}
