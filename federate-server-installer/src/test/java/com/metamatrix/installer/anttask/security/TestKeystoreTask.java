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
package com.metamatrix.installer.anttask.security;

import java.io.File;

import junit.framework.TestCase;

import org.apache.tools.ant.Project;

import com.metamatrix.core.util.UnitTestUtil;


/** 
 * TestKeystoreTask verifies task {@link KeystoreTask} will create the keystore file to the file system.
 */
public class TestKeystoreTask extends TestCase {
    
    /**
     * Constructor.
     * @param name
     */
    public TestKeystoreTask(String name) {
        super(name);
    }
    
    /**
     * This test the compatibility between the mm server jar and an upgrade jar
     * @throws Exception
     * @since 4.3
     */
    public void testExportDescriptor() throws Exception {
        String path = UnitTestUtil.getTestScratchFile("test.keystore").getAbsolutePath();
       
        Project p = new Project();
        p.setName("TestKeystoreTask");
        
        KeystoreTask cvc = new KeystoreTask();
        cvc.setProject(p);
        cvc.setKeystoreFile(path);
        
        cvc.init();
        
        cvc.perform();
        
        File newf = new File(path);
        if (!newf.exists()) {
            fail("Keystore file " + newf.getAbsolutePath() + " did not get written out");
        }
            
    }
         
    
}
