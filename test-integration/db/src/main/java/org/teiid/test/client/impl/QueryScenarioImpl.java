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
