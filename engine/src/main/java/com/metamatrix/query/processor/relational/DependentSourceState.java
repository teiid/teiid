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

package com.metamatrix.query.processor.relational;

import java.util.Collection;
import java.util.LinkedHashSet;
import java.util.List;

import com.metamatrix.api.exception.MetaMatrixComponentException;
import com.metamatrix.common.buffer.BlockedException;
import com.metamatrix.query.sql.symbol.Expression;
import com.metamatrix.query.sql.util.ValueIterator;

interface DependentSourceState {
    
    public static class SetState {

        Collection replacement = new LinkedHashSet();

        Expression valueExpression;

        ValueIterator valueIterator;

        Object nextValue;

        boolean isNull;
    }

    public void sort() throws BlockedException,
                      MetaMatrixComponentException;

    public void close() throws MetaMatrixComponentException;

    public ValueIterator getValueIterator(SetState setState);

    public void connectValueSource() throws MetaMatrixComponentException;
    
    public List getDepedentSetStates();
    
    public void setDependentSetStates(List states);

}