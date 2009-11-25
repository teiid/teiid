/*
 * Copyright (c) 2000-2007 MetaMatrix, Inc.
 * All rights reserved.
 */
package org.teiid.test.framework;

import java.util.Properties;

import org.teiid.test.framework.connection.ConnectionStrategy;
import org.teiid.test.framework.connection.ConnectionStrategyFactory;
import org.teiid.test.framework.exception.QueryTestFailedException;
import org.teiid.test.framework.exception.TransactionRuntimeException;

import com.metamatrix.core.util.StringUtil;

public abstract class TransactionContainer {

    private boolean debug = false;
   
 
    private String testClassName = null;

    protected ConnectionStrategy connStrategy;
    protected Properties props;

    protected TransactionContainer() {
	ConfigPropertyLoader config = ConfigPropertyLoader.getInstance();
	
	try {
	    this.connStrategy = ConnectionStrategyFactory
		    .createConnectionStrategy(config);
	} catch (QueryTestFailedException e) {
	    // TODO Auto-generated catch block
	    throw new TransactionRuntimeException(e);
	}
	this.props = new Properties();
	this.props.putAll(this.connStrategy.getEnvironment());

    }


    public ConnectionStrategy getConnectionStrategy() {
	return this.connStrategy;
    }
    
    
    public void setEnvironmentProperty(String key, String value) {
	this.getConnectionStrategy().getEnvironment().setProperty(key, value);
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
		test.setConnectionStrategy(connStrategy);

		test.setup();

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

	    try {
		test.cleanup();
	    } finally {

		// cleanup all connections created for this test.
		if (connStrategy != null) {
		    connStrategy.shutdown();
		}
	    }
	}

    }

    protected void runTest(TransactionQueryTestCase test) {
	debug("Start runTest: " + test.getTestName());

	try {

	    debug("	before(test)");

	    before(test);
	    debug("	test.before");

	    test.before();

	    debug("	test.testcase");

	    // run the test
	    test.testCase();

	    debug("	test.after");

	    test.after();
	    debug("	after(test)");

	    after(test);

	    debug("End runTest: " + test.getTestName());

	} catch (Throwable e) {

	    if (!test.exceptionExpected()) {
		e.printStackTrace();
	    }
	    debug("Error: " + e.getMessage());
	    throw new TransactionRuntimeException(e.getMessage());
	}

	if (test.exceptionExpected() && !test.exceptionOccurred()) {
	    throw new TransactionRuntimeException(
		    "Expected exception, but one did not occur for test: "
			    + this.getClass().getName() + "."
			    + test.getTestName());
	}

    }


    protected void debug(String message) {
	if (debug) {
	    System.out.println("[" + this.testClassName + "] " + message);
	}

    }

    protected void detail(String message) {
	System.out.println("[" + this.testClassName + "] " + message);
    }

    protected boolean done() {
	return true;
    }

}
