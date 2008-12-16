/*
 * Copyright � 2000-2005 MetaMatrix, Inc.
 * All rights reserved.
 */
package com.metamatrix.installer.anttask.security;

import java.io.File;

import junit.framework.Test;
import junit.framework.TestCase;
import junit.framework.TestSuite;

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
    
    
    public static Test suite() {
          TestSuite suite = new TestSuite();
          suite.addTestSuite(TestKeystoreTask.class);
          
      
          return suite;
    }    
    
    
    /**
     * This test the compatibility between the mm server jar and an upgrade jar
     * @throws Exception
     * @since 4.3
     */
    public void testExportDescriptor() throws Exception {
        String path = UnitTestUtil.getTestScratchFile("test.keystore").getAbsolutePath();
       
        System.out.println("Keystore file: " + path);
        
       	try {
    		    	           
            
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
            
            System.out.println("Keystore file " + newf.getAbsolutePath() + " was created");
           
                		
     	} catch (Exception e) {
            e.printStackTrace();
    		fail(e.getMessage());
     	}
     	
         
    }
         
    
}
