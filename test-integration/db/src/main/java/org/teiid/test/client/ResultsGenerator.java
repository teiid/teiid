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

import org.teiid.test.framework.exception.QueryTestFailedException;

/**
 * The ResultsGenerator represents how a new set of results will be written for a given <code>querySetID</code> and <code>queryIdentifier</code>.  The implementor should write out 
 * one result file for each call that is made to {@link #generateQueryResultFile(String, String, String, ResultSet, Throwable, int)  },
 * however, it will control the format of the content.
 * 
 * The testing process will only generate a new result file when the result mode is {@link TestClientTransaction.RESULT_MODES#GENERATE}
 * 
 * @author vanhalbert
 *
 */
public interface ResultsGenerator {
    /**
     * The {@link #PROP_GENERATE_DIR} property indicates where newly generated results
     * files should be written to.   The newly generated files should be written to
     * a different location than the existing expected results.
     */
    public static final String PROP_GENERATE_DIR = "generatedir"; //$NON-NLS-1$

    /**
     * Return the location that output files are written to.
     * 
     * @return
     * 
     * @since
     */
    String getOutputDir();

    /**
     * Return the location that results will be generated to.
     * 
     * @return
     * 
     * @since
     */
    String getGenerateDir();

    /**
     * Call to generate the results file from an executed query.
     * If an exception occurred, it is considered the result from 
     * the query.   The file created based on the result should
     * be able to be used as the expected result when query
     * tests are run with in the resultmode of "compare".
     * @param querySetID
     * @param queryIdentfier
     * @param query
     * @param result
     * @param queryError
     * @param testStatus
     * @throws QueryTestFailedException
     */
    void generateQueryResultFile(String querySetID, String queryIdentfier,
	    String query, ResultSet result, Throwable queryError, int testStatus)
	    throws QueryTestFailedException;

    /**
     * Call to generate an error file as the result of incompatibilities in the
     * comparison of the expected results to the actual results.
     * @param querySetID
     * @param queryIdentifier
     * @param sql
     * @param resultSet
     * @param queryError
     * @param results
     * @return
     * @throws QueryTestFailedException
     */
    String generateErrorFile(final String querySetID,
	    final String queryIdentifier, final String sql,
	    final ResultSet resultSet, final Throwable queryError,
	    final Object results) throws QueryTestFailedException;

}
