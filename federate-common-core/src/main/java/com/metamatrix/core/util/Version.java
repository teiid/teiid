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

package com.metamatrix.core.util;


/**
 * Wraps a String version that represents a branch.version paradigm ( i.e. 1.3)
 */
public class Version {
    private String version;
    
    /**
     * Construct an instance of Version.
     * 
     */
    public Version(String version) {
        ArgCheck.isNotNull(version);
        this.version = version;
    }

    /**
     * @return
     */
    public String getVersion() {
        return this.version;
    }
    
    public static String incrementVersion(String version){
        ArgCheck.isNotNull(version);
        
        final int index = version.lastIndexOf("."); //$NON-NLS-1$
        if(index > -1 && index != (version.length() - 1) ){
            String result = version.substring(0, index + 1);
            String ver = version.substring(index + 1);
            int newVer = Integer.valueOf(ver).intValue();
            newVer++;
            
            return (result + newVer);
        }if(index > -1 && index == (version.length() - 1) ){
            return (version + 0);
        }
        int ver = Integer.valueOf(version).intValue();
        ver++;
        
        return new Integer(ver).toString();
    }

}
