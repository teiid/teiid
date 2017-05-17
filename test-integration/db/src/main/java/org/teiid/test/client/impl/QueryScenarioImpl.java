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
package org.teiid.test.client.impl;

import java.sql.ResultSet;
import java.util.Properties;

import org.teiid.test.client.QueryScenario;
import org.teiid.test.client.TestProperties;
import org.teiid.test.client.TestResult;
import org.teiid.test.framework.exception.QueryTestFailedException;
import org.teiid.test.framework.exception.TransactionRuntimeException;

/**
 * The QueryScenarioImpl extends the QueryScenerio handle the testresults for defaults settings.
 * 
 * @author vanhalbert
 *
 */
@SuppressWarnings("nls")
public class QueryScenarioImpl extends QueryScenario {
    
    
    public QueryScenarioImpl(String scenarioName, Properties queryProperties) {
	super(scenarioName, queryProperties);

    }
    

    /* (non-Javadoc)
     * @see org.teiid.test.client.QueryScenario#handleTestResult(org.teiid.test.client.TestResult, java.lang.String)
     */
    @Override
    public void handleTestResult(TestResult tr, ResultSet resultSet, int updateCnt, boolean resultFromQuery, String sql) {
 	Throwable resultException = tr.getException();
	if (getResultsMode().equalsIgnoreCase(
		TestProperties.RESULT_MODES.COMPARE)) {
	    
		Object results = null;
		try {
		    results = this.getExpectedResults(tr.getQuerySetID()).compareResults(tr.getQueryID(), 
			    sql, 
			    resultSet, 
			    resultException, 
			    tr.getStatus(), isOrdered(sql), updateCnt, resultFromQuery);
		    
		    if (results == null) {
			tr.setStatus(TestResult.RESULT_STATE.TEST_SUCCESS);
		    } else {
			tr.setStatus(TestResult.RESULT_STATE.TEST_EXCEPTION);
			tr.setExceptionMessage("Results did not compare to expected results");
		    }
		    
		    
		} catch (QueryTestFailedException qtf) {
		    resultException = (resultException != null ? resultException
			    : qtf);
		    tr.setException(resultException);
		    tr.setStatus(TestResult.RESULT_STATE.TEST_EXCEPTION);

		}
		
		if (tr.getStatus() == TestResult.RESULT_STATE.TEST_EXCEPTION) {
		    try {
    		    	this.getResultsGenerator().generateErrorFile(tr.getQuerySetID(),
    			    tr.getQueryID(), sql, resultSet, resultException,
    			    results );	
    		    	
		    } catch (QueryTestFailedException qtfe) {
			    throw new TransactionRuntimeException(qtfe.getMessage());
		    }
		}


	} else if (getResultsMode().equalsIgnoreCase(
		TestProperties.RESULT_MODES.GENERATE)) { //$NON-NLS-1$

	    try {
		
		this.getResultsGenerator().generateQueryResultFile(tr.getQuerySetID(),
			tr.getQueryID(), sql, resultSet, resultException, tr.getStatus());
			
	    } catch (QueryTestFailedException qtfe) {
		throw new TransactionRuntimeException(qtfe.getMessage());
	    }

	} else {
	    // just create the error file for any failures
	    if (tr.getException() != null) {
		tr.setStatus(TestResult.RESULT_STATE.TEST_EXCEPTION);
		try {
		    this.getResultsGenerator().generateErrorFile(tr.getQuerySetID(),
			    tr.getQueryID(), sql, resultSet, resultException, null);

		} catch (QueryTestFailedException qtfe) {
		    throw new TransactionRuntimeException(qtfe.getMessage());
		}
	    }
	}


	
    }
    

    private boolean isOrdered(String sql) {

	if (sql.toLowerCase().indexOf(" order by ") > 0) {
	    return true;
	}
	return false;

    }
  
}
