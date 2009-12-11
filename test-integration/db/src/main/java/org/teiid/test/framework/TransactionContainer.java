/*
 * Copyright (c) 2000-2007 MetaMatrix, Inc.
 * All rights reserved.
 */
package org.teiid.test.framework;

import org.teiid.test.framework.exception.TransactionRuntimeException;

import com.metamatrix.core.util.StringUtil;

public abstract class TransactionContainer {

    private String testClassName = null;


    protected TransactionContainer() {
	
    }


    protected void before(TransactionQueryTestCase test) {
    }

    protected void after(TransactionQueryTestCase test) {
    }

    public void runTransaction(TransactionQueryTestCase test) {

	this.testClassName = StringUtil.getLastToken(test.getClass().getName(),
		".");

	try {
	    detail("Start transaction test: " + test.getTestName());

	    try {

		test.setup();

	    } catch (TransactionRuntimeException tre) {  
			if (!test.exceptionExpected()) {
			    tre.printStackTrace();
			}
        		throw tre;
	    } catch (Throwable e) {
		if (!test.exceptionExpected()) {
		    e.printStackTrace();
		}
		throw new TransactionRuntimeException(e.getMessage());
	    }

	    runTest(test);

	    detail("Completed transaction test: " + test.getTestName());

	} finally {
	    debug("	test.cleanup");

		test.cleanup();

	}

    }

    protected void runTest(TransactionQueryTestCase test) {
	debug("Start runTest: " + test.getTestName());


	    debug("	before(test)");

	    before(test);
	    debug("	test.before");

	    test.before();

	    debug("	test.testcase");
	    
	try {


	    // run the test
	    test.testCase();
	    
	} catch (Throwable e) {

	    if (!test.exceptionExpected()) {
		e.printStackTrace();
		debug("Error: " + e.getMessage());
		test.setApplicationException(e);
 
	    }
	}
	
	if (test.exceptionExpected() && !test.exceptionOccurred()) {
	    TransactionRuntimeException t  = new TransactionRuntimeException(
		    "Expected exception, but one did not occur for test: "
			    + this.getClass().getName() + "."
			    + test.getTestName());
	    test.setApplicationException(t);
	}


	    debug("	test.after");

	    test.after();
	    debug("	after(test)");

	    after(test);

	    debug("End runTest: " + test.getTestName());




    }


    protected void debug(String message) {
	TestLogger.logDebug("[" + this.testClassName + "] " + message);
    }

    protected void detail(String message) {
	TestLogger.log("[" + this.testClassName + "] " + message);
    }

    protected boolean done() {
	return true;
    }

}
