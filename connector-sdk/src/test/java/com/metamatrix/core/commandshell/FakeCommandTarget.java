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

package com.metamatrix.core.commandshell;

import java.util.Arrays;
import java.util.Date;
import java.util.HashSet;
import java.util.Set;

public class FakeCommandTarget extends CommandTarget {
    private String trace = ""; //$NON-NLS-1$
    
    public String getTrace() {
        return trace;
    }
    
    public FakeCommandTarget() {
        super();
    }

    public String checkin(String path, byte[] contents, Date lastModifiedDate) {
        trace += "checkin " + path + " <data> " + lastModifiedDate; //$NON-NLS-1$ //$NON-NLS-2$
        return null;
    }
    public String getLatest(String path) {
        trace += "getLatest " + path; //$NON-NLS-1$
        return null;
    }
    
    public void method0(String[] args) {
        trace += "method0 "; //$NON-NLS-1$
        for (int i=0; i<args.length; i++) {
            trace += args[i] + " "; //$NON-NLS-1$
        }
    }
    
    public void method1(String arg1, int[] args) {
        trace += "method1 " + arg1 + " "; //$NON-NLS-1$ //$NON-NLS-2$
        for (int i=0; i<args.length; i++) {
            trace += args[i] + " "; //$NON-NLS-1$
        }
    }
    
    public void methodToIgnore() {
    }
    
    /* 
     * @see com.metamatrix.core.commandshell.CommandTarget#getMethodsToIgnore()
     */
    protected Set getMethodsToIgnore() {
        Set result = new HashSet();
        result.addAll(Arrays.asList( new String[] { "methodToIgnore"} )); //$NON-NLS-1$
        return result;
    }

}
