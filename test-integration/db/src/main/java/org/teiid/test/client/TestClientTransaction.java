/*
 * JBoss, Home of Professional Open Source.
 * See the COPYRIGHT.txt file distributed with this work for information
 * regarding copyright ownership.  Some portions may be licensed
 * to Red Hat, Inc. under one or more contributor license agreements.
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

package org.teiid.test.client;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import org.teiid.test.client.results.TestResultStat;
import org.teiid.test.framework.TestLogger;
import org.teiid.test.framework.exception.QueryTestFailedException;
import org.teiid.test.framework.exception.TransactionRuntimeException;
import org.teiid.test.framework.query.AbstractQueryTransactionTest;

/**
 * TestClientTransaction
 * 
 * @author vanhalbert
 * 
 */
public class TestClientTransaction extends AbstractQueryTransactionTest {

    private QueryScenario querySet = null;
    private ExpectedResults expectedResults = null;

    // the current querySet info
    private String querySetID = null;
    private String queryIdentifier = null;
    private Object queryObject = null;

    private long endTS = 0;
    private long beginTS = 0;

    private int testStatus = TestResult.RESULT_STATE.TEST_SUCCESS;

    private TestResult rs = null;

    private boolean errorExpected = false;
    
    private String sql = null;

    public TestClientTransaction(QueryScenario querySet) {
	super(querySet.getQueryScenarioIdentifier());
	this.querySet = querySet;

    }

    public void init(String querySetID, String queryIdentifier, Object query) {
	this.querySetID = querySetID;
	this.queryIdentifier = queryIdentifier;
	this.queryObject = query;

	this.expectedResults = null;
	endTS = 0;
	beginTS = 0;

	testStatus = TestResult.RESULT_STATE.TEST_SUCCESS;

	rs = null;

	errorExpected = false;

    }

    @Override
    public void before() {
	// TODO Auto-generated method stub
	super.before();

	this.expectedResults = this.querySet
		.getExpectedResults(this.querySetID);

	try {
	    this.errorExpected = expectedResults
		    .isExceptionExpected(this.queryIdentifier);
	} catch (QueryTestFailedException e) {
	    // TODO Auto-generated catch block
	    throw new TransactionRuntimeException("ProgramError: "
		    + e.getMessage());
	}

    }

    @Override
    public void testCase() throws Exception {
	if (this.queryObject instanceof String) {
	    this.sql = (String) this.queryObject;
	    executeTest(this.querySetID, this.queryIdentifier, sql);
	}
	
	// TODO:  support object types for queries (i.e., arguments for prepared statements, etc)

    }

    protected void executeTest(String querySetID, String queryidentifier,
	    String sql) throws Exception {

        
	TestLogger.logDebug("execute: " + sql);

	// int expectedRowCount=-1;
	TestLogger.logDebug("expected error: " + this.errorExpected);

	try {
	    System.out.println(this.querySet.getQueryScenarioIdentifier()  + ":" + this.querySetID + ":"
		    + this.queryIdentifier);
	    // need to set this so the underlying query execution handles an
	    // error properly.

	    beginTS = System.currentTimeMillis();
	    execute(sql);

	} catch (Throwable t) {
	    this.setApplicationException(t);

	} finally {
	    // Capture resp time
	    endTS = System.currentTimeMillis();
	}

    }

    @Override
    public void after() {
	// TODO Auto-generated method stub
	super.after();

	String errorfile = null;

	// TODO: uncomment
	ResultSet erResultSet = null;
	// this.internalResultSet;

	ResultsGenerator genResults = this.querySet.getResultsGenerator();
	
	Throwable resultException = null;

	resultException = (this.getLastException() != null ? this
		    .getLastException() : this.getApplicationException());

	    

	if (resultException != null) {
		    if (this.exceptionExpected()) {
			testStatus = TestResult.RESULT_STATE.TEST_EXPECTED_EXCEPTION;
		    } else {
			testStatus = TestResult.RESULT_STATE.TEST_EXCEPTION;
		    }

	}




	if (this.querySet.getResultsMode().equalsIgnoreCase(
		TestProperties.RESULT_MODES.COMPARE)) {
	    if (testStatus != TestResult.RESULT_STATE.TEST_EXCEPTION) {
		try {
		    this.expectedResults.compareResults(this.queryIdentifier,
			    sql, this.internalResultSet, resultException,
			    testStatus, isOrdered(sql), this.fetchSize - 1);
		} catch (QueryTestFailedException qtf) {
		    resultException = (resultException != null ? resultException
			    : qtf);
		    testStatus = TestResult.RESULT_STATE.TEST_EXCEPTION;

		}
	    }

	    if (testStatus == TestResult.RESULT_STATE.TEST_EXCEPTION) {
		try {
		    genResults.generateErrorFile(querySetID,
			    this.queryIdentifier, sql, this.internalResultSet,
			    resultException, expectedResults
				    .getResultsFile(this.queryIdentifier));

		} catch (QueryTestFailedException qtfe) {
		    throw new TransactionRuntimeException(qtfe.getMessage());
		}
	    }

	} else if (this.querySet.getResultsMode().equalsIgnoreCase(
		TestProperties.RESULT_MODES.GENERATE)) { //$NON-NLS-1$

	    try {
		genResults.generateQueryResultFile(querySetID,
			this.queryIdentifier, sql, erResultSet,
			resultException, testStatus);
	    } catch (QueryTestFailedException qtfe) {
		throw new TransactionRuntimeException(qtfe.getMessage());
	    }

	} else {
	    // just create the error file for any failures
	    if (testStatus == TestResult.RESULT_STATE.TEST_EXCEPTION) {
		try {
		    genResults.generateErrorFile(querySetID,
			    this.queryIdentifier, sql, this.internalResultSet,
			    resultException, expectedResults
				    .getResultsFile(this.queryIdentifier));

		} catch (QueryTestFailedException qtfe) {
		    throw new TransactionRuntimeException(qtfe.getMessage());
		}
	    }
	}

	rs = new TestResultStat(querySetID, this.queryIdentifier, sql,
		testStatus, beginTS, endTS, resultException, errorfile);

    }

    public TestResult getTestResult() {
	return this.rs;

    }

    @Override
    protected Statement createStatement() throws SQLException {
	return this.internalConnection.createStatement(
		ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY);
    }

    // need to override this method because the abstract logic for throwing
    // exceptions depends on this
    @Override
    public boolean exceptionExpected() {
	// TODO Auto-generated method stub
	return this.errorExpected;
    }

    private boolean isOrdered(String sql) {

	if (sql.toLowerCase().indexOf(" order by ") > 0) {
	    return true;
	}
	return false;

    }

    /**
     * Override the super cleanup() so that the connection to Teiid is not
     * cleaned up at this time.
     * 
     * This will be handled after all queries in the set have been executed.
     * 
     * @see TestClient#runTest();
     * 
     */
    @Override
    public void cleanup() {
	//	
	// NOTE: do not cleanup TestResults because {@link #getTestResult} is called
	// after cleanup

    }


    

}
