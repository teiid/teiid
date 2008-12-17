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
 * Purpose: This task will create a new keystore file
 *
 */

import java.io.File;
import java.io.IOException;
import java.util.Properties;

import org.apache.tools.ant.BuildException;
import org.apache.tools.ant.Project;
import org.apache.tools.ant.Task;

import com.metamatrix.api.exception.MetaMatrixException;
import com.metamatrix.common.util.crypto.CryptoException;
import com.metamatrix.common.util.crypto.CryptoUtil;
import com.metamatrix.common.util.crypto.cipher.SymmetricCryptor;
import com.metamatrix.core.util.ArgCheck;



public class KeystoreTask extends Task { 
    private String keystoreFile;

    /**
     * Constructor 
     */
    public KeystoreTask() {
        super();
        
        

    }

    public void setKeystoreFile(String keystorefilename) {
    	this.keystoreFile = keystorefilename;
    }
   
    
    /*
     * 
     */
    public void execute() throws BuildException {
    	 ArgCheck.isNotNull(this.keystoreFile, "Unable to create keystore, keystoreFile not set.");
         try { 
        	 File f = new File(this.keystoreFile);
        	 f.delete();
        	 CryptoUtil.reinit();
            // Create the new Keystore
            SymmetricCryptor.generateAndSaveKey(this.keystoreFile);
            this.getProject().log("Keystore file " + this.keystoreFile + " was created", Project.MSG_DEBUG);
            
         } catch (Exception e) {
        	   String msg = "Error creating the keystore file " + keystoreFile + ", error " + e.getMessage();
        	   this.getProject().log(msg, Project.MSG_ERR);
           	   throw new BuildException(e); //$NON-NLS-1$

		}           

    }


}
