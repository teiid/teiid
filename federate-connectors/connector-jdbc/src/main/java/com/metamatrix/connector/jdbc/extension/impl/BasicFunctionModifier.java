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

/*
 */
package com.metamatrix.connector.jdbc.extension.impl;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.HashMap;

import com.metamatrix.connector.jdbc.extension.FunctionModifier;
import com.metamatrix.data.api.TypeFacility;
import com.metamatrix.data.language.IExpression;
import com.metamatrix.data.language.IFunction;

/**
 */
public abstract class BasicFunctionModifier implements FunctionModifier {

    /*
     * Public sharing part for the mapping between class and type in format of Map<class->Integer>.
     */
    public static final int STRING = 0;
    public static final int CHAR = 1;
    public static final int BOOLEAN = 2;
    public static final int BYTE = 3;
    public static final int SHORT = 4;
    public static final int INTEGER = 5;
    public static final int LONG = 6;
    public static final int BIGINTEGER = 7;
    public static final int FLOAT = 8;
    public static final int DOUBLE = 9;
    public static final int BIGDECIMAL = 10;
    public static final int DATE = 11;
    public static final int TIME = 12;
    public static final int TIMESTAMP = 13;
    public static final int OBJECT = 14;
    public static final int BLOB = 15;
    public static final int CLOB = 16;
    public static final int XML = 17;

    public static final Map typeMap = new HashMap();
    
    static {
        typeMap.put(TypeFacility.RUNTIME_TYPES.STRING, new Integer(STRING));
        typeMap.put(TypeFacility.RUNTIME_TYPES.CHAR, new Integer(CHAR));
        typeMap.put(TypeFacility.RUNTIME_TYPES.BOOLEAN, new Integer(BOOLEAN));
        typeMap.put(TypeFacility.RUNTIME_TYPES.BYTE, new Integer(BYTE));
        typeMap.put(TypeFacility.RUNTIME_TYPES.SHORT, new Integer(SHORT));
        typeMap.put(TypeFacility.RUNTIME_TYPES.INTEGER, new Integer(INTEGER));
        typeMap.put(TypeFacility.RUNTIME_TYPES.LONG, new Integer(LONG));
        typeMap.put(TypeFacility.RUNTIME_TYPES.BIG_INTEGER, new Integer(BIGINTEGER));
        typeMap.put(TypeFacility.RUNTIME_TYPES.FLOAT, new Integer(FLOAT));
        typeMap.put(TypeFacility.RUNTIME_TYPES.DOUBLE, new Integer(DOUBLE));
        typeMap.put(TypeFacility.RUNTIME_TYPES.BIG_DECIMAL, new Integer(BIGDECIMAL));
        typeMap.put(TypeFacility.RUNTIME_TYPES.DATE, new Integer(DATE));
        typeMap.put(TypeFacility.RUNTIME_TYPES.TIME, new Integer(TIME));
        typeMap.put(TypeFacility.RUNTIME_TYPES.TIMESTAMP, new Integer(TIMESTAMP));
        typeMap.put(TypeFacility.RUNTIME_TYPES.OBJECT, new Integer(OBJECT));        
        typeMap.put(TypeFacility.RUNTIME_TYPES.BLOB, new Integer(BLOB));
        typeMap.put(TypeFacility.RUNTIME_TYPES.CLOB, new Integer(CLOB));
        typeMap.put(TypeFacility.RUNTIME_TYPES.XML, new Integer(XML));
    }    
    
    /**
     * Subclass should override this method as needed.
     * @see com.metamatrix.connector.jdbc.extension.FunctionModifier#modify(com.metamatrix.data.language.IFunction)
     */
    public IExpression modify(IFunction function) {
        return function;
    }

    /**
     * Subclass should override this method as needed.
     * @see com.metamatrix.connector.jdbc.extension.FunctionModifier#translate(com.metamatrix.data.language.IFunction)
     */
    public List translate(IFunction function) {
        List objs = new ArrayList();
        objs.add(function.getName());
        objs.add("("); //$NON-NLS-1$
        IExpression[] params = function.getParameters();
        if(params.length > 0) { 
            objs.add(params[0]);
            if(params.length > 1) {
                for(int i=1; i<params.length; i++) {
                    objs.add(", "); //$NON-NLS-1$
                    objs.add(params[i]);
                }
            }
        }
        objs.add(")"); //$NON-NLS-1$

        return objs;
    }

}
