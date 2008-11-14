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

package com.metamatrix.connector.jdbc.sybase;

import java.util.ArrayList;
import java.util.List;

import com.metamatrix.connector.jdbc.extension.FunctionModifier;
import com.metamatrix.data.language.IExpression;
import com.metamatrix.data.language.IFunction;

/**
 */
public class ModFunctionModifier implements FunctionModifier {

    /* 
     * @see com.metamatrix.connector.jdbc.extension.FunctionModifier#modify(com.metamatrix.data.language.IFunction)
     */
    public IExpression modify(IFunction function) {
        return function;
    }

    /* 
     * @see com.metamatrix.connector.jdbc.extension.FunctionModifier#translate(com.metamatrix.data.language.IFunction)
     */
    public List translate(IFunction function) {
        List parts = new ArrayList();
        parts.add("(");//$NON-NLS-1$
        
        IExpression[] args = function.getParameters();
        parts.add(args[0]);
        parts.add(" % "); //$NON-NLS-1$
        parts.add(args[1]);
        parts.add(")");             //$NON-NLS-1$
        
        return parts;
    }

}
