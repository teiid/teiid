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

package com.metamatrix.connector.jdbc.derby;

import java.util.ArrayList;
import java.util.List;

import com.metamatrix.connector.jdbc.extension.impl.BasicFunctionModifier;
import com.metamatrix.data.language.IExpression;
import com.metamatrix.data.language.IFunction;
import com.metamatrix.data.language.ILiteral;


public class DerbyTimestampFunctionModifier extends BasicFunctionModifier {

    public DerbyTimestampFunctionModifier() {
        super();
    }

/** 
     * @see com.metamatrix.connector.jdbc.extension.impl.BasicFunctionModifier#translate(com.metamatrix.data.language.IFunction)
     * @since 4.3
     */
    public List translate(IFunction function) {
        List objs = new ArrayList();
        objs.add("{fn ");         //$NON-NLS-1$
        objs.add(function.getName());
        objs.add("(");  //$NON-NLS-1$

        IExpression[] args = function.getParameters();
        if(args != null && args.length > 0) {
            objs.add(((ILiteral)args[0]).getValue());

            for(int i=1; i<args.length; i++) {
                objs.add(", ");  //$NON-NLS-1$
                objs.add(args[i]);
            }
        }
        objs.add(")}"); //$NON-NLS-1$
        return objs;
    }    
}
