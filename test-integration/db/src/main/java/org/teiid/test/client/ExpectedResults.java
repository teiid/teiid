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

import java.io.File;
import java.sql.ResultSet;

import org.teiid.test.framework.exception.QueryTestFailedException;

/**
 * An ExpectedResults represents one set of expected results (referred to as the queryset) identified by the {@link  #getQuerySetID}.   
 * The <code>queryidentifier</code> identify a unique query and corresponds to the expected results file.   
 * 
 * 
 * @author vanhalbert
 *
 */
public interface ExpectedResults {
	
    /**
     * The results location is where expected result files can be found
     */
    public static final String PROP_EXPECTED_RESULTS_DIR_LOC = "results.loc";
    
    /**
     * {@link #PROP_EXPECTED_RESULTS_ROOT_DIR}, if specified, indicates the root directory 
     * to be prepended to the {@link #PROP_EXPECTED_RESULTS_DIR_LOC} to create the full
     * directory to find the expected results files.  
     * 
     * This property is normally used during the nightly builds so that the query files
     * will coming from other projects.
     */

    public static final String PROP_EXPECTED_RESULTS_ROOT_DIR = "results.root.dir";


    
    /**
     * Return the unique identifier for this query set.
     * @return
     */
    String getQuerySetID();


	/**
	 * Returns the <code>File</code> location for the actual results for the specified
	 * query identifier. 
	 * @param queryidentifier
	 * @return File location for actual results for the specified query
	 * @throws QueryTestFailedException
	 *
	 * @since
	 */
	File getResultsFile(String queryidentifier) throws QueryTestFailedException;
	
	
	/**
	 * @see TestProperties#RESULT_MODES
	 * 
	 * Return true if the expected results file is needed in the test.  Either
	 * for comparison or generation.   It will return false when
	 * the option <code>TestProperties.RESULT_MODES.NONE</code>
	 * @return
	 */
	boolean isExpectedResultsNeeded();
	
	
	/**
	 * Indicates if a query expects to have an <code>Exception</code> to be thrown when the
	 * query is executed.
	 * @param queryidentifier
	 * @return boolean true if the query expects an exception to be thrown
	 * @throws QueryTestFailedException
	 */
	boolean isExceptionExpected(String queryidentifier) throws QueryTestFailedException;
	
	
	Object getMetaData(String queryidentifier);
	
	
	/**
	 * Called to compare the <code>ResultSet</code> from the executed query to the expected results
	 * and return the errors.
	 * @param queryIdentifier
	 * @param sql
	 * @param resultSet
	 * @param actualException
	 * @param testStatus
	 * @param isOrdered
	 * @param updateCnt
	 * @return Object identifying the errors in the comparison
	 * @throws QueryTestFailedException
	 */
	Object compareResults(final String queryIdentifier,
			   final String sql,
               final ResultSet resultSet,
               final Throwable actualException,
               final int testStatus,
               final boolean isOrdered,
               final int updateCnt,
               final boolean resultFromQuery) throws QueryTestFailedException;

}
