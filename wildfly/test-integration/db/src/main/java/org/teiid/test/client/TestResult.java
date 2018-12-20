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

/**
 * The TestResult represents the results from a single test.  A single test can include 1 or more sql commands that
 * are considered 1 inclusive test.
 * 
 * @author vanhalbert
 * 
 */
@SuppressWarnings("nls")
public interface TestResult {

    /**
     * The RESULT_STATE is the value assigned based the result of the executed
     * query test
     * 
     * @author vanhalbert
     * 
     */
    public interface RESULT_STATE {

	/**
	 * TEST_SUCCESS - indicates the executed query performed as expected
	 */
	public static final int TEST_SUCCESS = 0;
	/**
	 * TEST_EXCEPTION - indicates an unexpected exception occurred during
	 * the execution of the query
	 */
	public static final int TEST_EXCEPTION = 1;
	/**
	 * TEST_EXPECTED_EXCEPTION - indicates the expected result was suppose
	 * to an exception, how, the query executed without error
	 */
	public static final int TEST_EXPECTED_EXCEPTION = 4;
    }

    public interface RESULT_STATE_STRING {
	/**
	 * The string value for when a
	 * {@link TestResult.RESULT_STATE#TEST_SUCCESS occurs
	 */
	public static final String PASS = "pass";
	/**
	 * The string value for when a
	 * {@link TestResult.RESULT_STATE#TEST_EXCEPTION occurs
	 */
	public static final String FAIL = "fail";
	/**
	 * The string value for when a
	 * {@link TestResult.RESULT_STATE#TEST_EXPECTED_EXCEPTION occurs
	 */
	public static final String FAIL_EXPECTED_EXCEPTION = "fail-expected_exception";

	/**
	 * The string value for when a status occurs that hasn't been defined
	 */
	public static final String UNKNOWN = "unknown";

    }

    /**
     * Return the id the uniquely identifies the query set.
     * 
     * @return String is the query set id
     */
    String getQuerySetID();

    /**
     * Return the id that uniquely identifies the query within the query set
     * {@link #getQuerySetID()}.
     * 
     * @return
     * 
     * @since
     */
    String getQueryID();

    /**
     * Return the query that was executed in order to produce this result.
     * 
     * @return
     * 
     * @since
     */
    String getQuery();

    /**
    * Return the status of the execution of this query {@link #getQuery();
    * @see TestResult.RESULT_STATE
    * @return
    *
    * @since
    */
    int getStatus();
    
    
    /**
     * Call to set the status for this test result.
     * @see TestResult.RESULT_STATE
     * @param status
     */
    void setStatus(int status);
    
    /**
     * Return the result status in string format.
     * @return String
     */

    String getResultStatusString();

    /**
     * If the result from this query produced an exception, then this method
     * should return the <code>String</code> representation of the exception.
     * 
     * @return
     * 
     * @since
     */
    String getExceptionMsg();
    
    Throwable getException();
    
    /**
     * Set the exception that indicates the reason why there is a problem
     * with the results.   Call {@link #setExceptionMessage(String)} to display
     * a different message in the summary file.
     * @param error
     */
    void setException(Throwable error);
    
    /**
     * Set the error message relating to the reason why there is a problem
     * with the results.
     * @param errorMsg
     */
    void setExceptionMessage(String errorMsg);

    /**
     * Return the time (in a long value) that this query started.
     * 
     * @return long representation of begin time
     * 
     * @since
     */
    long getBeginTS();
    
    void setBeginTS(long beginTS);

    /**
     * Return the time (in a long value) that this query ended
     * 
     * @return long representation of end time
     * 
     * @since
     */
    long getEndTS();
    
    void setEndTS(long endTS);

    /**
     * @return Returns the name of errorfile where the error results were
     *         written.
     */
    String getErrorfile();
    
    void setErrorFile(String errorFile);
    
}
