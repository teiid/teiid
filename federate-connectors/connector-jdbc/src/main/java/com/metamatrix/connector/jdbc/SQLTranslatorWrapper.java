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

package com.metamatrix.connector.jdbc;

import java.util.*;
import java.util.HashMap;
import java.util.Map;

import com.metamatrix.connector.jdbc.extension.SQLConversionVisitor;
import com.metamatrix.connector.jdbc.extension.SQLTranslator;
import com.metamatrix.data.api.ConnectorEnvironment;
import com.metamatrix.data.api.ExecutionContext;
import com.metamatrix.data.exception.ConnectorException;
import com.metamatrix.data.language.ICommand;
import com.metamatrix.data.metadata.runtime.RuntimeMetadata;

/**
 * Wrapper for the actual SQLTranslator that provides caching and a
 * canonicalization of the function modifier map.
 */
public class SQLTranslatorWrapper implements SQLTranslator {

    // The underlying SQLTranslator we are wrapping
    private SQLTranslator delegate;
    
    // Cached function modifiers
    private Map functionModifiers;

    public SQLTranslatorWrapper(SQLTranslator translator) {
        this.delegate = translator;
    }

    public void initialize(ConnectorEnvironment env, RuntimeMetadata metadata) throws ConnectorException {
        this.delegate.initialize(env, metadata);
    }

    public ICommand modifyCommand(ICommand command, ExecutionContext context) throws ConnectorException {
        return this.delegate.modifyCommand(command, context);
    }

    public SQLConversionVisitor getTranslationVisitor() {
        return this.delegate.getTranslationVisitor();
    }

    public Map getFunctionModifiers() {
        // Note that this null check is NOT in a synchronized block.  This is ok because:
        // 1) functionModifiers is an object reference and either exists in a complete state 
        //    (due to the implementation of loadModifiers) or it does not - can't be half-constructed
        // 2) race condition to create map is not dangerous - worst case, the map will be created
        //    and swapped out more than once.  This is no worse than not having the cache in the first
        //    place.     
        if(functionModifiers == null) {
            loadModifiers();
        }
             
        return functionModifiers;
    }

    private synchronized void loadModifiers() {
        Map modifiers = this.delegate.getFunctionModifiers();
        
        // Build new map with all lower-case keys so we can do case-insensitive lookups
        Map cleanModifiers = new HashMap();
        Iterator modIter = modifiers.entrySet().iterator();
        while(modIter.hasNext()) {
            Map.Entry entry = (Map.Entry) modIter.next();
            String key = (String) entry.getKey();
            cleanModifiers.put(key.toLowerCase(), entry.getValue());
        }
        
        // Replace existing modifiers (usually null unless there was a race
        // condition to call this method) with clean modifiers
        this.functionModifiers = cleanModifiers;        
    }

}
