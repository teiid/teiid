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

import java.io.File;
import java.sql.ResultSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.teiid.core.util.SqlUtil;
import org.teiid.test.client.ExpectedResults;
import org.teiid.test.client.ctc.ResultsHolder;
import org.teiid.test.framework.TestLogger;
import org.teiid.test.framework.exception.QueryTestFailedException;
import org.teiid.test.util.TestResultSetUtil;

@SuppressWarnings("nls")
public class ExpectedResultsImpl implements ExpectedResults {

    private static final int MAX_COL_WIDTH = 65;

    protected Properties props;
    protected int resultMode = -1;
    protected String generateDir = null;
    protected String querySetIdentifier = null;
    protected String results_dir_loc = null;

    protected Map<String, ResultsHolder> loadedResults = new HashMap<String, ResultsHolder>();

    public ExpectedResultsImpl(String querySetIdentifier, Properties properties) {
	this.props = properties;
	this.querySetIdentifier = querySetIdentifier;

	this.results_dir_loc = props.getProperty(PROP_EXPECTED_RESULTS_DIR_LOC,
		"");

	String expected_root_loc = this.props
		.getProperty(PROP_EXPECTED_RESULTS_ROOT_DIR);

	if (expected_root_loc != null) {
	    File dir = new File(expected_root_loc, results_dir_loc);
	    this.results_dir_loc = dir.getAbsolutePath();
	}

	TestLogger.logInfo("Expected results loc: " + this.results_dir_loc);
    }

    @Override
    public boolean isExceptionExpected(String queryidentifier)
	    throws QueryTestFailedException {
	return false;
    }
    
	public boolean isExpectedResultsNeeded() {
    	return true;

	}


    @Override
    public String getQuerySetID() {
	return this.querySetIdentifier;
    }

    @Override
    public synchronized File getResultsFile(String queryidentifier)
	    throws QueryTestFailedException {
	return findExpectedResultsFile(queryidentifier, this.querySetIdentifier);

    }

    /**
     * Compare the results of a query with those that were expected.
     * 
     * @param expectedResults
     *            The expected results.
     * @param results
     *            The actual results - may be null if
     *            <code>actualException</code>.
     * @param actualException
     *            The actual exception recieved durring query execution - may be
     *            null if <code>results</code>.
     * @param isOrdered
     *            Are the actual results ordered?
     * @param batchSize
     *            Size of the batch(es) used in determining when the first batch
     *            of results were read.
     * @return The response time for comparing the first batch (sizes) of
     *         resutls.
     * @throws QueryTestFailedException
     *             If comparison fails.
     */
    public Object compareResults(final String queryIdentifier, final String sql,
	    final ResultSet resultSet, final Throwable actualException,
	    final int testStatus, final boolean isOrdered, final int updateCnt,
	    final boolean resultFromQuery) throws QueryTestFailedException {

	File expectedResultsFile = getResultsFile(queryIdentifier);

	List<?> results = null;
	if (actualException != null) {
	    try {
		results = TestResultSetUtil.compareThrowable(
			actualException, sql, expectedResultsFile, false);

	    } catch (Throwable e) {
		QueryTestFailedException t = new QueryTestFailedException(
			e.getMessage());
		t.initCause(e);
		throw t;
	    }
	    
	    if (results != null && results.size() > 0) {
		return results;
	    }
	    
	    return null;

	}

	// update sql or procedure(with no results) has no results set
	if (!resultFromQuery) {

	    if (SqlUtil.isUpdateSql(sql)) {
		if (updateCnt == 0 && expectedResultsFile.length() > 0) {
		    throw new QueryTestFailedException("Update cnt was zero: " + expectedResultsFile.getName());
		}
		if (updateCnt > 0 && expectedResultsFile.length() == 0) {
		    throw new QueryTestFailedException(
			    "Update cnt was greater than zero, but didnt expected any updates");
		}

	    } else {
		if (expectedResultsFile.length() > 0) {
		    throw new QueryTestFailedException("No results from query, but expected results");
		}
	    }
	    
	    
	} else {

	    try {
		if (expectedResultsFile.length() == 0) {
		    // if the expectedResult file is empty
		    // and the result doesnt have a first row-meaning its empty
		    // then this is good
		    if (!resultSet.first()) {
			throw new QueryTestFailedException(
				"Expected results is empty, but query produced results");
		    }
		    return results;
		} 
		
		resultSet.beforeFirst();

		results = TestResultSetUtil.writeAndCompareResultSet(resultSet, sql,
			MAX_COL_WIDTH, false, null, expectedResultsFile, false);

	    } catch (QueryTestFailedException qe) {
		throw qe;
	    } catch (Throwable e) {
		QueryTestFailedException t = new QueryTestFailedException(
			e.getMessage());
		t.initCause(e);
		throw t;
	    }

	    
	    if (results != null && results.size() > 0) {
		return results;
	    }
	    
	    return null;

	}
	
	return results;

    }

    @Override
    public Object getMetaData(String queryidentifier) {
	// TODO Auto-generated method stub
	return null;
    }

    private File findExpectedResultsFile(String queryIdentifier,
	    String querySetIdentifier) throws QueryTestFailedException {
	String resultFileName = queryIdentifier + ".txt"; //$NON-NLS-1$
	File file = new File(results_dir_loc + "/" + querySetIdentifier,
		resultFileName);
	if (!file.exists()) {
	    throw new QueryTestFailedException("Query results file "
		    + file.getAbsolutePath() + " cannot be found");
	}

	return file;

    }

}
