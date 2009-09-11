/*
 * Copyright © 2000-2008 MetaMatrix, Inc.
 * All rights reserved.
 */
package org.teiid.test.testcases;

import junit.framework.Test;
import junit.framework.TestSuite;



/** 
 * @since 1.0
 */
public class Alltaests {
    
    public static void main(String[] args) {
        junit.textui.TestRunner.run(Alltaests.suite());
    }

    public static Test suite() {
        TestSuite suite = new TestSuite("All Integration Tests"); //$NON-NLS-1$
        //$JUnit-BEGIN$
 //       suite.addTestSuite(LocalTransactionTests.class);
        
        //$JUnit-END$
        return suite;
    }



}
