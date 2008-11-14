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

import junit.framework.TestCase;

/**
 * @since 4.0
 */
public final class TestVersion extends TestCase {
    // =========================================================================
    //                        F R A M E W O R K
    // =========================================================================
    /**
     * Constructor for TestJDBCRepositoryWriter.
     * @param name
     */
    public TestVersion(String name) {
        super(name);
    }

    // =========================================================================
    //                         T E S T   C A S E S
    // =========================================================================
    /**
     * @since 4.0
     */
    public void testConstructor() {
        Version ver = null;
        try {
            ver = new Version(null);
        } catch (Exception e) {
            //expected, just return;
            return;
        }
        
        fail("Expected NPE for null arg but did not get one " + ver); //$NON-NLS-1$
    }
    
    public void testIncrementVersion1(){
        final String verStr = "1.0"; //$NON-NLS-1$
        final Version ver = new Version(verStr);
        
        final String postVer = Version.incrementVersion( ver.getVersion() );
        
        if("1.1".equals(postVer) ){ //$NON-NLS-1$
            return;
        }
        
        fail("Expected version to be \"1.1\", but it was \"" + postVer + "\"");   //$NON-NLS-1$//$NON-NLS-2$
    }
    
    public void testIncrementVersion2(){
        final String verStr = "1"; //$NON-NLS-1$
        final Version ver = new Version(verStr);
        
        final String postVer = Version.incrementVersion( ver.getVersion() );
        
        if("2".equals(postVer) ){ //$NON-NLS-1$
            return;
        }
        
        fail("Expected version to be \"2\", but it was \"" + postVer + "\"");  //$NON-NLS-1$ //$NON-NLS-2$
    }

    public void testIncrementVersion3(){
        final String verStr = "1."; //$NON-NLS-1$
        final Version ver = new Version(verStr);
        
        final String postVer = Version.incrementVersion( ver.getVersion() );
        
        if("1.0".equals(postVer) ){ //$NON-NLS-1$
            return;
        }
        
        fail("Expected version to be \"1.0\", but it was \"" + postVer + "\"");  //$NON-NLS-1$ //$NON-NLS-2$
    }

    public void testIncrementVersion4(){
        final String verStr = "1.1.0"; //$NON-NLS-1$
        final Version ver = new Version(verStr);
        
        final String postVer = Version.incrementVersion( ver.getVersion() );
        
        if("1.1.1".equals(postVer) ){ //$NON-NLS-1$
            return;
        }
        
        fail("Expected version to be \"1.1.1\", but it was \"" + postVer + "\"");  //$NON-NLS-1$ //$NON-NLS-2$
    }
    
}
