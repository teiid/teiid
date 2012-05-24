/*
 * Copyright (c) 2000-2007 MetaMatrix, Inc.
 * All rights reserved.
 */
package org.teiid.test.framework;

import org.teiid.core.util.StringUtil;
import org.teiid.test.framework.exception.TransactionRuntimeException;

@SuppressWarnings("nls")
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
	    debug("Start transaction test: " + test.getTestName());

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

	    debug("Completed transaction test: " + test.getTestName());

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
	    // this catches the non-SQLExceptions that the AbstractQueryTest catches.
	    // And therefore, the exception needs to be set as an application exception,
	    // considered outside the bounds of the normal sqlexceptions.
	    test.setApplicationException(e);

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
