/* JBoss, Home of Professional Open Source.
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
package org.teiid.test.client.ctc;

import java.sql.ResultSet;
import java.util.Properties;

import org.teiid.test.client.ExpectedResults;
import org.teiid.test.client.QueryScenario;
import org.teiid.test.client.TestProperties;
import org.teiid.test.client.TestResult;
import org.teiid.test.framework.exception.QueryTestFailedException;
import org.teiid.test.framework.exception.TransactionRuntimeException;

/**
 * The CTCQueryScenario represents the tests that were created using the old xml file formats.
 *  
 * @author vanhalbert
 *
 */
@SuppressWarnings("nls")
public class CTCQueryScenario extends QueryScenario {
    
     
    public CTCQueryScenario(String scenarioName, Properties querySetProperties) {
	super(scenarioName, querySetProperties);
    }
    
    protected void setUp() {

	try {
	    reader = new XMLQueryReader(this.getQueryScenarioIdentifier(), this.getProperties());
	} catch (QueryTestFailedException e1) {
    		throw new TransactionRuntimeException(e1);
	}

	resultsGen = new XMLGenerateResults(this.getQueryScenarioIdentifier(), this.getProperties());

	if (reader.getQuerySetIDs() == null
		|| reader.getQuerySetIDs().isEmpty()) {
	    throw new TransactionRuntimeException(
		    "The queryreader did not return any queryset ID's to process");
	}

	validateResultsMode(this.getProperties());

    }

 
    @Override 
    public ExpectedResults getExpectedResults(String querySetID) {    
	return new XMLExpectedResults( querySetID, this.getProperties());
    }    



    /* (non-Javadoc)
     * @see org.teiid.test.client.QueryScenario#handleTestResult(org.teiid.test.client.TestResult, java.lang.String)
     */
    @Override
    public void handleTestResult(TestResult tr, ResultSet resultSet, int updatecnt, boolean resultFromQuery, String sql) {

	Throwable resultException = tr.getException();
	if (getResultsMode().equalsIgnoreCase(TestProperties.RESULT_MODES.COMPARE)) {
	    if (tr.getStatus() != TestResult.RESULT_STATE.TEST_EXCEPTION) {
			try {
			    this.getExpectedResults(tr.getQuerySetID()).compareResults(tr.getQueryID(), 
				    sql, 
				    resultSet, 
				    resultException, 
				    tr.getStatus(), isOrdered(sql), -1,  resultFromQuery);
	
			} catch (QueryTestFailedException qtf) {
			    resultException = (resultException != null ? resultException
				    : qtf);
			    tr.setException(resultException);
			    tr.setStatus(TestResult.RESULT_STATE.TEST_EXCEPTION);
	
			}
	    }

	    if (tr.getStatus() == TestResult.RESULT_STATE.TEST_EXCEPTION) {
			try {
			    
			    this.getResultsGenerator().generateErrorFile(tr.getQuerySetID(),
				    tr.getQueryID(), sql, resultSet, resultException,
				    this.getExpectedResults(tr.getQuerySetID()).getResultsFile(tr.getQueryID()) );
			    
	
			} catch (QueryTestFailedException qtfe) {
			    throw new TransactionRuntimeException(qtfe.getMessage());
			}
	    }

	} else if (getResultsMode().equalsIgnoreCase(TestProperties.RESULT_MODES.GENERATE)) { //$NON-NLS-1$

	    try {
		
			this.getResultsGenerator().generateQueryResultFile(tr.getQuerySetID(),
				tr.getQueryID(), sql, resultSet, resultException, tr.getStatus());
		
	    } catch (QueryTestFailedException qtfe) {
		throw new TransactionRuntimeException(qtfe.getMessage());
	    }

	} else {
	    // just create the error file for any failures
	    if (tr.getStatus() == TestResult.RESULT_STATE.TEST_EXCEPTION && !getResultsMode().equalsIgnoreCase(TestProperties.RESULT_MODES.NONE)) {
		try {
		    this.getResultsGenerator().generateErrorFile(tr.getQuerySetID(),
			    tr.getQueryID(), sql, resultSet, resultException,
			    this.getExpectedResults(tr.getQuerySetID()).getResultsFile(tr.getQueryID()) );

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
