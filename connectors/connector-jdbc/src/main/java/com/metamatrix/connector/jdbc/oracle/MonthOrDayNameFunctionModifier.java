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
 * Convert the MONTHNAME etc. function into an equivalent Oracle function.  
 * Format: to_char(timestampvalue/dayvalue, 'Month'/'Day') 
 */
public class MonthOrDayNameFunctionModifier extends BasicFunctionModifier implements FunctionModifier {
    private ILanguageFactory langFactory;
    private String format;
    
    public MonthOrDayNameFunctionModifier(ILanguageFactory langFactory, String format) {
        this.langFactory = langFactory;
        this.format = format;
    }
    
    /* 
     * @see com.metamatrix.connector.jdbc.extension.FunctionModifier#modify(com.metamatrix.data.language.IFunction)
     */
    public IExpression modify(IFunction function) {
        IExpression[] args = function.getParameters();
    
        IFunction func = langFactory.createFunction("TO_CHAR",  //$NON-NLS-1$
            new IExpression[] { 
                args[0], 
                langFactory.createLiteral(format, String.class)},  
            String.class);
        
        // For some reason, these values have trailing spaces
        IFunction trimFunc = langFactory.createFunction("RTRIM",  //$NON-NLS-1$
            new IExpression[] { func }, String.class);
        
        return trimFunc;    
    }
}
