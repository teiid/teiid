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

import java.util.Arrays;
import java.util.List;

import org.teiid.connector.jdbc.translator.BasicFunctionModifier;
import org.teiid.connector.jdbc.translator.FunctionModifier;
import org.teiid.connector.language.IExpression;
import org.teiid.connector.language.IFunction;
import org.teiid.connector.language.ILanguageFactory;
import org.teiid.connector.language.ILiteral;


/**
 * Modify the locate function to use the Oracle instr function.
 * 
 * locate(sub, str) -> instr(str, sub)
 * 
 * locate(sub, str, start) -> instr(str, sub, start+1)
 */
public class LocateFunctionModifier extends BasicFunctionModifier implements FunctionModifier {

    private ILanguageFactory langFactory;
    
    public LocateFunctionModifier(ILanguageFactory langFactory) {
        this.langFactory = langFactory;
    }

    /* 
     * @see com.metamatrix.connector.jdbc.extension.FunctionModifier#modify(com.metamatrix.data.language.IFunction)
     */
    public IExpression modify(IFunction function) {
    	function.setName("instr"); //$NON-NLS-1$
        List<IExpression> args = function.getParameters();    
        IExpression expr = args.get(0);
        args.set(0, args.get(1));
        args.set(1, expr);
        if(args.size() == 3) {
            if(args.get(2) instanceof ILiteral) {
                ILiteral indexConst = (ILiteral)args.get(2);
                if(indexConst.getValue() != null) {
                    // Just modify the constant
                    Integer index = (Integer) indexConst.getValue();
                    args.set(2, langFactory.createLiteral(new Integer(index.intValue()+1), Integer.class));
                }
            } else {
                // Make plus function since this involves an element or function
                IFunction plusFunction = langFactory.createFunction("+",  //$NON-NLS-1$
                    Arrays.asList( args.get(2), langFactory.createLiteral(new Integer(1), Integer.class) ),
                    Integer.class);
                args.set(2, plusFunction);
            }   
        }        
        return function;           
    }

}
