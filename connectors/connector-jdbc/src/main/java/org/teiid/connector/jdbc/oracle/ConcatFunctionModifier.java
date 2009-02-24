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

package org.teiid.connector.jdbc.oracle;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.teiid.connector.jdbc.translator.BasicFunctionModifier;

import com.metamatrix.connector.api.SourceSystemFunctions;
import com.metamatrix.connector.api.TypeFacility;
import com.metamatrix.connector.language.ICriteria;
import com.metamatrix.connector.language.IExpression;
import com.metamatrix.connector.language.IFunction;
import com.metamatrix.connector.language.ILanguageFactory;
import com.metamatrix.connector.language.ILiteral;
import com.metamatrix.connector.language.ICompoundCriteria.Operator;


/**
 * This Function modifier used to support ANSI concat on Oracle 9i.
 * TODO: this is no longer necessary on Oracle 10g and later. 
 * <code>
 * CONCAT(a, b) ==> CASE WHEN (a is NULL OR b is NULL) THEN NULL ELSE CONCAT(a, b)
 * </code>   
 */
public class ConcatFunctionModifier extends BasicFunctionModifier {
    private ILanguageFactory langFactory;
    
    /** 
     * @param langFactory
     */
    public ConcatFunctionModifier(ILanguageFactory langFactory) {
        this.langFactory = langFactory;
    }

    /** 
     * @see org.teiid.connector.jdbc.translator.BasicFunctionModifier#modify(com.metamatrix.connector.language.IFunction)
     */
    public IExpression modify(IFunction function) {
        List when = new ArrayList();
        IExpression a = function.getParameters().get(0);
        IExpression b = function.getParameters().get(1);
        List crits = new ArrayList();
        
        ILiteral nullValue = langFactory.createLiteral(null, TypeFacility.RUNTIME_TYPES.STRING);
        if (isNull(a)) {
        	return nullValue;
        } else if (!isNotNull(a)) {
        	crits.add(langFactory.createIsNullCriteria(a, false));
        }
        if (isNull(b)) {
        	return nullValue;
        } else if (!isNotNull(b)) {
        	crits.add(langFactory.createIsNullCriteria(b, false));
        }
        
        ICriteria crit = null;
        
        if (crits.isEmpty()) {
        	return function;
        } else if (crits.size() == 1) {
        	crit = (ICriteria)crits.get(0);
        } else {
        	crit = langFactory.createCompoundCriteria(Operator.OR, crits);
        }
        when.add(crit);
        List then = Arrays.asList(new IExpression[] {nullValue}); 
        return langFactory.createSearchedCaseExpression(when, then, function, TypeFacility.RUNTIME_TYPES.STRING);
    }
    
    private boolean isNotNull(IExpression expr) {
    	if (expr instanceof ILiteral) {
    		ILiteral literal = (ILiteral)expr;
    		return literal.getValue() != null;
    	}
    	if (expr instanceof IFunction) {
    		IFunction function = (IFunction)expr;
    		if (function.getName().equalsIgnoreCase("NVL") || function.getName().equalsIgnoreCase(SourceSystemFunctions.IFNULL)) { //$NON-NLS-1$
    			return isNotNull(function.getParameters().get(0));
    		}
    	}
    	return false;
    }
    
    private boolean isNull(IExpression expr) {
    	if (expr instanceof ILiteral) {
    		ILiteral literal = (ILiteral)expr;
    		return literal.getValue() == null;
    	}
    	return false;
    }
        
    /** 
     * @see org.teiid.connector.jdbc.translator.BasicFunctionModifier#translate(com.metamatrix.connector.language.IFunction)
     */
    public List translate(IFunction function) {
        return null; //allow default translation
    }
}
