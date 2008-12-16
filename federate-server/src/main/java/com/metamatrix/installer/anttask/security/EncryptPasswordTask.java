/*
 * Copyright � 2000-2005 MetaMatrix, Inc.  All rights reserved.
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
