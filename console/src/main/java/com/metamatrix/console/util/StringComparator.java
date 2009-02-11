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

package com.metamatrix.console.util;

import java.util.Comparator;

import com.metamatrix.console.ConsolePlugin;


/**
 * Implementation of Comparator to compare two Strings.
 *  
 * @since 4.2
 */
public class StringComparator implements Comparator {
    private final static String EXCEPTION_MSG = ConsolePlugin.Util.getString(
        "StringComparator.badArgMsg"); //$NON-NLS-1$
    
    private boolean ignoreCase;
    
    public StringComparator(boolean ignoreCase) {
        super();
        this.ignoreCase = ignoreCase;
    }
    
    public int compare(Object o1, Object o2) {
        if (!((o1 instanceof String) && (o2 instanceof String))) {
            throw new RuntimeException(EXCEPTION_MSG);
        }
        String s1 = (String)o1;
        String s2 = (String)o2;
        int result;
        if (ignoreCase) {
            result = s1.compareToIgnoreCase(s2);
        } else {
            result = s1.compareTo(s2);
        }
        return result;
    }
}
