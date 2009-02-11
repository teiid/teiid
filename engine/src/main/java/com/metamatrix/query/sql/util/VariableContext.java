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

package com.metamatrix.query.sql.util;

import java.util.HashMap;
import java.util.Map;

import com.metamatrix.query.QueryPlugin;
import com.metamatrix.query.sql.symbol.ElementSymbol;
import com.metamatrix.query.util.ErrorMessageKeys;

/**
 * <p>This class holds a map of variables to their values, these variables and
 * their values are held in the context of a {@link Block} in the procedure language,
 * differrent {@link Statement}s populate the map by declaring variables and
 * assigning values. This class holds reference to a parent <code>VariableContext</code>
 * that holds variable info for parent {@link Block}. The variable declared at
 * the parent level is available to its immediate child context and any children,
 * down the heirarchy.</p>
 */
public class VariableContext {

    // map between variables and their values
    private Map variableMap = new HashMap();

    // reference to the parent variable context
    private VariableContext parentContext;

    /**
     * Constructor for VariableContext.
     */
    public VariableContext() {
        this.variableMap = new HashMap();
    }

    /**
     * Constructor for VariableContext.
     */
    public VariableContext(Map variableMap) {
        if(variableMap == null) {
            throw new IllegalArgumentException(QueryPlugin.Util.getString(ErrorMessageKeys.SQL_0019));
        }
        this.variableMap = variableMap;
    }

    /**
     * Set the value for the given, if the variable aready exits replaces its value
     * with the given value else adds a new variable to the map.
     * @param variable The <code>ElementSymbol</code> to be added as a variable.
     * @param value The value to be set for the given variable.
     */
    public void setValue(ElementSymbol variable, Object value) {
        variableMap.put(variable, value);
    }

    /**
     * Get the value for the given variable, if the variable exits in the current
     * context just return the value of the variable else lookup the parent context
     * and return the value of the variable.
     * @param variable The <code>ElementSymbol</code> whose value needs to be returned.
     * @return The value of the given variable
     */
    public Object getValue(ElementSymbol variable) {
        if (variableMap.containsKey(variable)) {
            return variableMap.get(variable);
        }
        // if the variable is not present in the current variablecontext
        // look up the parent context
        if(this.parentContext != null) {
            return this.parentContext.getValue(variable);
        }
        return null;
    }

    /**
     * Set the parent variable context for this variable context, when looking up
     * the variable's value, if the variable is not present in the current context,
     * the parent context is lookedup.
     * @param parent The parent <code>VariableContext</code>.
     */
    public void setParentContext(VariableContext parent) {
        this.parentContext = parent;
    }

    /**
     * Get the parent context for this variable context. When looking up
     * the variable's value, if the variable is not present in the current context,
     * the parent context is lookedup.
     * @return The parent <code>VariableContext</code>.
     */
    public VariableContext getParentContext() {
        return this.parentContext;
    }

    /** Helper Methods  */

    public void getFlattenedContextMap(Map values) {
        if(this.parentContext != null) {
            this.parentContext.getFlattenedContextMap(values);
        }
        values.putAll(this.variableMap);
    }

    /**
     * Check if this context or any of it's parent contexts contain this variable
     * @param variable The variable which may be present on this context
     * @return A boolean value indiating if the given variable is present on this context
     * or any of it's parent contexts.
     */
    public boolean containsVariable(ElementSymbol variable) {
        if(!variableMap.containsKey(variable)) {
            if(this.parentContext != null) {
                return this.parentContext.containsVariable(variable);
            }
            return false;
        }
        return true;
    }

    /**
     * Check if the current context and its parents contain any variables
     * @return A boolean bollean value indicating if this context is empty
     */
    public boolean isEmpty() {
        if(variableMap.isEmpty()) {
            if(this.parentContext != null) {
                return this.parentContext.isEmpty();
            }
        }
        return false;
    }
}
