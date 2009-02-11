/*
 * JBoss, Home of Professional Open Source.
 * Copyright (C) 2008 Red Hat, Inc.
 * Copyright (C) 2000-2007 MetaMatrix, Inc.
 * Licensed to Red Hat, Inc. under one or more contributor 
 * license agreements.  See the copyright.txt file in the
 * distribution for a full listing of individual contributors.
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

package com.metamatrix.connector.jdbc.oracle;

import com.metamatrix.connector.jdbc.extension.FunctionModifier;
import com.metamatrix.connector.jdbc.extension.impl.BasicFunctionModifier;
import com.metamatrix.connector.language.*;

/**
 * Convert the HOUR function into an equivalent Oracle function.  
 * HOUR(ts) --> TO_NUMBER(TO_CHAR(ts, 'HH24'))
 */
public class HourFunctionModifier extends BasicFunctionModifier implements FunctionModifier {

    private ILanguageFactory langFactory;
    
    public HourFunctionModifier(ILanguageFactory langFactory) {
        this.langFactory = langFactory;
    }
    
    /* 
     * @see com.metamatrix.connector.jdbc.extension.FunctionModifier#modify(com.metamatrix.data.language.IFunction)
     */
    public IExpression modify(IFunction function) {
        IExpression[] args = function.getParameters();
    
        IFunction innerFunction = langFactory.createFunction("TO_CHAR",  //$NON-NLS-1$
            new IExpression[] { 
                args[0],
                langFactory.createLiteral("HH24", String.class)},  //$NON-NLS-1$
            String.class); 

        IFunction outerFunction = langFactory.createFunction("TO_NUMBER",  //$NON-NLS-1$
            new IExpression[] { innerFunction },
            Integer.class); 
            
        return outerFunction;
    }

}
