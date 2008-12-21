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

import java.util.Properties;

import junit.framework.TestCase;

import org.apache.tools.ant.Project;

import com.metamatrix.common.config.CurrentConfiguration;
import com.metamatrix.common.util.CommonPropertyNames;
import com.metamatrix.common.util.crypto.CryptoUtil;
import com.metamatrix.core.util.UnitTestUtil;


/** 
 * TestEncryptPasswordTask verifies that the {@link EncryptPasswordTask} task can encrypt a password
 * and place it into the global Ant project properties
 */
public class TestEncryptPasswordTask extends TestCase {
    private static final String PASSWORD_PROPERTY = "encrypted.password.property";
    private static final String PASSWORD_TO_ENCRYPT = "trythispassword";
    /**
     * Constructor.
     * @param name
     */
    public TestEncryptPasswordTask(String name) {
        super(name);
    }
    
    
    /**
     * Test the task of encrypting a password works
     */
    public void testEncrypt() throws Exception {
    	Properties props = System.getProperties();
    	
    	
    	props.remove(CommonPropertyNames.JCE_PROVIDER);
    	System.setProperties(props);
    	
     	CurrentConfiguration.reset();
		CryptoUtil.reinit();

        String path = UnitTestUtil.getTestScratchFile("test.keystore").getAbsolutePath();
            
        Project p = new Project();
        p.setName("TestEncryptPasswordTask");
        
        EncryptPasswordTask cvc = new EncryptPasswordTask();
        cvc.setProject(p);
        cvc.setPasswordProperty(PASSWORD_PROPERTY);
        cvc.setEncryptPassword(PASSWORD_TO_ENCRYPT);
        
        
        cvc.init();
        
        cvc.perform();
        
        String encryptedpassword = cvc.getProject().getProperty(PASSWORD_PROPERTY);
        System.out.println("Password " + PASSWORD_TO_ENCRYPT + " was encrypted to " + (encryptedpassword !=null ? encryptedpassword : "NULL"));
         if (encryptedpassword == null ||
        		encryptedpassword.equals(PASSWORD_TO_ENCRYPT) ) {

            fail("Password encryption failed");
        }
         
    }
         
    
}
