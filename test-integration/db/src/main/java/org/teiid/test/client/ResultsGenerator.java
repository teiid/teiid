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
