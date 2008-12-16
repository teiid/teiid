/*
 * Copyright � 2000-2005 MetaMatrix, Inc.  All rights reserved.
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
