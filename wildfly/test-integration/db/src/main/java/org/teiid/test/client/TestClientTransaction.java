/*
 * Copyright Red Hat, Inc. and/or its affiliates
 * and other contributors as indicated by the @author tags and
 * the COPYRIGHT.txt file distributed with this work.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
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
@SuppressWarnings("nls")
public class TestClientTransaction extends AbstractQueryTransactionTest {

    private QueryScenario querySet = null;
    private ExpectedResults expectedResults = null;
    private QueryTest query = null;

    private long endTS = 0;
    private long beginTS = 0;

    private int testStatus = TestResult.RESULT_STATE.TEST_SUCCESS;

    private boolean errorExpected = false;
    
    private String sql = null;
    private boolean resultFromQuery = false;
    
    private TestResultsSummary testResultsSummary;

    public TestClientTransaction(QueryScenario querySet) {
	super(querySet.getQueryScenarioIdentifier());
	this.querySet = querySet;

    }
    
    
    

    public void init(TestResultsSummary testResultsSummary, ExpectedResults expectedResults, QueryTest query) {
	this.query = query;
	this.testResultsSummary = testResultsSummary;
	this.expectedResults = expectedResults;
	
	endTS = 0;
	beginTS = 0;

	testStatus = TestResult.RESULT_STATE.TEST_SUCCESS;

	errorExpected = false;
	resultFromQuery = false;

    }
    
    public String getTestName() {
	return query.getQueryScenarioID() + ":" + (query.getQueryID()!=null?query.getQueryID():"NA");
	
    }

    @Override
    public void before() {
	// TODO Auto-generated method stub
	super.before();


	try {
	    this.errorExpected = expectedResults
		    .isExceptionExpected(query.getQueryID());
	} catch (QueryTestFailedException e) {
	    // TODO Auto-generated catch block
	    throw new TransactionRuntimeException("ProgramError: "
		    + e.getMessage());
	}

    }


    @Override
    public void testCase() throws Exception {
	TestLogger.logDebug("expected error: " + this.errorExpected);
	TestLogger.logInfo("ID: " + query.geQuerySetID() + "  -  " + query.getQueryID());
        
	QuerySQL[] queries = query.getQueries();
	int l = queries.length;

	try {
	    // need to set this so the underlying query execution handles an
	    // error properly.

	    beginTS = System.currentTimeMillis();
	    
	    for (int i= 0; i < l; i++) {
		QuerySQL qsql = queries[i];
		this.sql = qsql.getSql();
		resultFromQuery = execute(sql, qsql.getParms());
		if (qsql.getUpdateCnt() >= 0) {	    
		    this.assertUpdateCount(qsql.getUpdateCnt());

		} else if (qsql.getRowCnt() >= 0) {
		    this.assertRowCount(qsql.getRowCnt());

		}
		
	    }

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
	TestResult rs = null;
	
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

	rs = new TestResultStat(query.geQuerySetID(), query.getQueryID(), sql,
		testStatus, beginTS, endTS, resultException, null);
	
	this.testResultsSummary.addTestResult(query.geQuerySetID(), rs);

	this.querySet.handleTestResult(rs, this.internalResultSet, this.updateCount, resultFromQuery, sql);

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
	return this.errorExpected;
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
