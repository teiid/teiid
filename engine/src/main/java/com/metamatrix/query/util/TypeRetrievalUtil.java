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

package com.metamatrix.query.util;

import java.util.List;
import java.util.ListIterator;

import com.metamatrix.common.types.DataTypeManager;
import com.metamatrix.query.sql.symbol.Expression;


/**
 * Utility to provide type names to pass to the BufferManager.createTupleSource() method.
 * @since 4.2
 */
public class TypeRetrievalUtil {
    private TypeRetrievalUtil() {} // Uninstantiable
    
    /**
     * Gets the data type names for each of the input expressions, in order.
     * @param expressions List of Expressions
     * @return
     * @since 4.2
     */
    public static String[] getTypeNames(List expressions) {
        String[] types = new String[expressions.size()];
        ListIterator i = expressions.listIterator();
        Expression expr = null;
        for (int index = 0; i.hasNext(); index++) {
            expr = (Expression)i.next();
            types[index] = DataTypeManager.getDataTypeName(expr.getType());
        }
        return types;
    }

}
