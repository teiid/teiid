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

package org.teiid.query.function.aggregate;

import java.util.Arrays;
import java.util.List;

import org.teiid.core.TeiidComponentException;
import org.teiid.core.TeiidProcessingException;
import org.teiid.query.util.CommandContext;

/**
 * Just a simple First/Last_value() implementation
 */
public class FirstLastValue extends SingleArgumentAggregateFunction {

    private Class<?> type;
    private Object value;
    private boolean first;
    private boolean set;

    public void reset() {
        value = null;
        set = false;
    }
    
    public FirstLastValue(Class<?> type, boolean first) {
        this.type = type;
        this.first = first;
    }
    
    @Override
    public void addInputDirect(Object input, List<?> tuple,
            CommandContext commandContext) throws TeiidProcessingException,
            TeiidComponentException {
        if (!first) {
            value = input;
        } else if (!set) {
            value = input;
            set = true;
        }
    }

    /**
     * @see org.teiid.query.function.aggregate.AggregateFunction#getResult(CommandContext)
     */
    public Object getResult(CommandContext commandContext) {
        return value;
    }
    
    @Override
    public void getState(List<Object> state) {
    	state.add(value);
    	state.add(set);
    }
    
    @Override
    public int setState(List<?> state, int index) {
    	value = state.get(index);
    	set = (Boolean)state.get(index++);
    	return index++;
    }
    
    @Override
    public List<? extends Class<?>> getStateTypes() {
    	return Arrays.asList(type, Boolean.class);
    }
    
    @Override
    public boolean respectsNull() {
        return true;
    }

}
