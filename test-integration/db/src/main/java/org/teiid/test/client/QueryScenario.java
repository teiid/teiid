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

import java.util.Collection;
import java.util.Map;
import java.util.Properties;

import org.teiid.test.client.TestProperties.RESULT_MODES;

/**
 * The QueryScenario manages all the information required to run one scenario of tests.
 * This includes the following:
 * <li>The queryreader and its query sets to be executed as a scenario </li>
 * <li>Provides the expected results that correspond to a query set </li>
 * <li>The results generator that would be used when {@link RESULT_MODES#GENERATE} is specified</li>
 * 
 * @author vanhalbert
 *
 */
public interface QueryScenario {
    
 
    /**
     * Return the identifier for the current scenario
     * @return String name of scenario
     */
    String getQueryScenarioIdentifier() ;
    
    /**
     * Return the properties defined for this scenario
     * @return Properties
     */
    Properties getProperties();
    
    /**
     * Return a <code>Map</code> containing the query identifier as the key, and
     * the value is the query.  In most simple cases, the query will be a <code>String</code>
     * However, complex types (i.e., to execute prepared statements or other arguments), it maybe
     * some other type.
     * @param querySetID identifies a set of queries
     * @return Map<String, Object>
     */
    Map<String, Object>  getQueries(String querySetID);
    
    /**
     * Return a <code>Collection</code> of <code>querySetID</code>s that the {@link QueryReader} will be
     * providing.  The <code>querySetID</code> can be used to obtain it associated set of queries by
     * call {@link #getQueries(String)}
     * @return Collection of querySetIDs
     */
    Collection<String> getQuerySetIDs();
    
    /**
     * Return the result mode that was defined by the property {@link TestProperties#PROP_RESULT_MODE}
     * @return String result mode
     */
    String getResultsMode();
    
    /**
     * Return the {@link ExpectedResults} for the specified <code>querySetID</code>.  These expected
     * results will be used to compare with the actual results in order to determine success or failure.
     * @param querySetID
     * @return ExpectedResults
     */
    ExpectedResults getExpectedResults(String querySetID);
    
    /**
     * Return the {@link ResultsGenerator} that is to be used to create new sets of expected results.
     * @return
     */
    ResultsGenerator getResultsGenerator() ;
    
    /**
     * Return the root output directory where comparison reports or newly generated expected results should be located.
     */
    String getOutputDirectory();



}
