/*
 * Copyright � 2000-2005 MetaMatrix, Inc.
 * All rights reserved.
 */
package com.metamatrix.installer.anttask.security;

import java.util.Properties;

import junit.framework.Test;
import junit.framework.TestCase;
import junit.framework.TestSuite;

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
    
    
    public static Test suite() {
          TestSuite suite = new TestSuite();
          suite.addTestSuite(TestEncryptPasswordTask.class);
          
      
          return suite;
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
       
        System.out.println("Keystore file: " + path);
        
       	try {
    		    	           
            
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
            
           
                		
     	} catch (Exception e) {
            e.printStackTrace();
    		fail(e.getMessage());
     	}
     	
         
    }
         
    
}
