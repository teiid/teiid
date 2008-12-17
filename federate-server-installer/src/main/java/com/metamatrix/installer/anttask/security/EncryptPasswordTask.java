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

/**
 * Created on Jun 13, 2002
 *
 * Purpose: This task will encrypt the password
 *
 */

import org.apache.tools.ant.BuildException;
import org.apache.tools.ant.Project;
import org.apache.tools.ant.Task;

import com.metamatrix.common.util.crypto.CryptoException;
import com.metamatrix.common.util.crypto.CryptoUtil;
import com.metamatrix.core.util.ArgCheck;



public class EncryptPasswordTask extends Task { 
    private String passwordPropertyName;
    private String passwordToEncrypt;
    /**
     * Constructor 
     */
    public EncryptPasswordTask() {
        super();
        
    }

    public void setPasswordProperty(String propertyName) {
    	this.passwordPropertyName = propertyName;
    }
    public void setEncryptPassword(String password) {
    	this.passwordToEncrypt = password;

    }
   
    
    
    /*
     */
    public void execute() throws BuildException {
        ArgCheck.isNotNull(this.passwordPropertyName, "Unable to encrypt password, passwordPropertyname not set.");
        try {    
            String encrypted_password =CryptoUtil.getEncryptor().encrypt(this.passwordToEncrypt);
            this.getProject().setNewProperty(this.passwordPropertyName, encrypted_password);
           
            this.getProject().log("Successfully encryted password" + this.passwordToEncrypt + " to " + encrypted_password, Project.MSG_INFO);

        } catch (CryptoException e) {
        	   String msg = "Unable to encrypt the password " + passwordToEncrypt + ", error " + e.getMessage();
        	   this.getProject().log(msg, Project.MSG_ERR);
        	   throw new BuildException(e); //$NON-NLS-1$
        }           

    }


}
