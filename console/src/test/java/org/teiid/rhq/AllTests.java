/*
 * Copyright © 2000-2008 MetaMatrix, Inc.
 * All rights reserved.
 */
package org.teiid.rhq;

import org.teiid.rhq.comm.impl.TestConnectionPool;

import junit.framework.Test;
import junit.framework.TestSuite;



/** 
 * @since 1.0
 */
public class AllTests {
    
    public static void main(String[] args) {
        junit.textui.TestRunner.runAndWait(AllTests.suite());
    }

    public static Test suite() {
        TestSuite suite = new TestSuite("Test for com.metamatrix.rhq"); //$NON-NLS-1$
        //$JUnit-BEGIN$
        suite.addTest(TestConnectionPool.suite());
        
        //$JUnit-END$
        return suite;
    }



}
