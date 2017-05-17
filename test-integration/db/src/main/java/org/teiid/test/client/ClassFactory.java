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

import java.util.ArrayList;
import java.util.Collection;

import org.teiid.core.util.ReflectionHelper;
import org.teiid.test.framework.ConfigPropertyLoader;
import org.teiid.test.framework.exception.TransactionRuntimeException;


/**
 * @author vanhalbert
 *
 */
@SuppressWarnings("nls")
public class ClassFactory {
    
    /**
     * The {@link #QUERY_SCENARIO_CLASSNAME} property indicates the implementation of
     * {@link QueryScenario} to use.
     */
    public static final String QUERY_SCENARIO_CLASSNAME = "query.scenario.classname"; //$NON-NLS-1$
    
    /**
     * The default scenario class to use when {@link #QUERY_SCENARIO_CLASSNAME} is not defined.
     */
    
    public static final String QUERY_SCENARIO_DEFAULT_CLASSNAME = "org.teiid.test.client.impl.QueryScenarioImpl"; //$NON-NLS-1$
   

    /**
     * The {@link #QUERY_READER_CLASSNAME} property indicates the implementation of
     * {@link QueryReader} to use.
     */
    public static final String QUERY_READER_CLASSNAME = "query.reader.classname"; //$NON-NLS-1$
    
    /**
     * The default query reader class to use when {@link #QUERY_READER_CLASSNAME} is not defined.
     */
    
    public static final String QUERY_READER_DEFAULT_CLASSNAME = "org.teiid.test.client.ctc.XMLQueryReader"; //$NON-NLS-1$
 

    
    /**
     * The {@link #RESULTS_GENERATOR_CLASSNAME} property indicates the implementation of
     * {@link ResultsGenerator} to use.
     */
    public static final String RESULTS_GENERATOR_CLASSNAME = "results.generator.classname"; //$NON-NLS-1$
    
    /**
     * The default query reader class to use when {@link #QUERY_READER_CLASSNAME} is not defined.
     */
    
    public static final String RESULTS_GENERATOR_DEFAULT_CLASSNAME = "org.teiid.test.client.impl.ResultsGeneratorImpl"; //$NON-NLS-1$
 
    
    
    /**
     * The {@link #EXPECTED_RESULTS_CLASSNAME} property indicates the implementation of
     * {@link ExpectedResults} to use.
     */
    public static final String EXPECTED_RESULTS_CLASSNAME = "expected.results.classname"; //$NON-NLS-1$
    
    /**
     * The default query reader class to use when {@link #EXPECTED_RESULTS_CLASSNAME} is not defined.
     */
    
    public static final String EXPECTED_RESULTS_DEFAULT_CLASSNAME = "org.teiid.test.client.impl.ExpectedResultsImpl"; //$NON-NLS-1$



    
    
    public static QueryScenario createQueryScenario(String scenarioName) {
    
	String clzzname = ConfigPropertyLoader.getInstance().getProperty(QUERY_SCENARIO_CLASSNAME);
		if (clzzname == null || clzzname.startsWith("${")) {
	    clzzname = QUERY_SCENARIO_DEFAULT_CLASSNAME;
	}
	
	    Collection<Object> args = new ArrayList<Object>(2);
	    args.add(scenarioName);
	    args.add(ConfigPropertyLoader.getInstance().getProperties());

	QueryScenario scenario;
	try {
	    scenario = (QueryScenario) ReflectionHelper.create(clzzname, args, null);
	} catch (Exception e) {
	    e.printStackTrace();
	    throw new TransactionRuntimeException(e);
	}
	return scenario;
    }
    
    public static QueryReader createQueryReader(Collection<?> args) {
	String clzzname = ConfigPropertyLoader.getInstance().getProperty(QUERY_READER_CLASSNAME);
	if (clzzname == null) {
	    clzzname = QUERY_READER_DEFAULT_CLASSNAME;
	}
	
	QueryReader reader;
	try {
	    reader = (QueryReader) ReflectionHelper.create(clzzname, args, null);
	} catch (Exception e) {
	    e.printStackTrace();
	    throw new TransactionRuntimeException(e);
	}	

	return reader;
    }
    
    public static ResultsGenerator createResultsGenerator(Collection<?> args) {
	String clzzname = ConfigPropertyLoader.getInstance().getProperty(RESULTS_GENERATOR_CLASSNAME);
	if (clzzname == null) {
	    clzzname = RESULTS_GENERATOR_DEFAULT_CLASSNAME;
	}
	
	ResultsGenerator resultsgen;
	try {
	    resultsgen = (ResultsGenerator) ReflectionHelper.create(clzzname, args, null);
	} catch (Exception e) {
	    throw new TransactionRuntimeException(e.getMessage());
	}	

	return resultsgen;
    }
    
    public static ExpectedResults createExpectedResults(Collection<?> args)  {
	String clzzname = ConfigPropertyLoader.getInstance().getProperty(EXPECTED_RESULTS_CLASSNAME);
	if (clzzname == null) {
	    clzzname = EXPECTED_RESULTS_DEFAULT_CLASSNAME;
	}
	
	ExpectedResults expResults;
	try {
	    expResults = (ExpectedResults) ReflectionHelper.create(clzzname, args, null);
	} catch (Exception e) {
	    throw new TransactionRuntimeException(e.getMessage());
	}	

	return expResults;
    }
}
