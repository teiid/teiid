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
package org.teiid.test.client.ctc;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.junit.Assert;
import org.teiid.test.client.ExpectedResults;
import org.teiid.test.client.QueryReader;
import org.teiid.test.client.QueryScenario;
import org.teiid.test.client.ResultsGenerator;
import org.teiid.test.client.TestProperties;
import org.teiid.test.client.TestResult;
import org.teiid.test.client.TestProperties.RESULT_MODES;
import org.teiid.test.framework.ConfigPropertyLoader;
import org.teiid.test.framework.TestLogger;
import org.teiid.test.framework.exception.QueryTestFailedException;
import org.teiid.test.framework.exception.TransactionRuntimeException;

import com.metamatrix.common.util.PropertiesUtils;
import com.metamatrix.core.util.FileUtils;

/**
 * The TestQuerySet contains all the information required to run one set of tests.
 * This includes the following:
 * <li>The queryreader and its query sets to be executed as a scenario </li>
 * <li>Provides the expected results that correspond to a query set </li>
 * <li>The results generator that would be used when {@link RESULT_MODES#GENERATE} is specified</li>
 * 
 * @author vanhalbert
 *
 */
public class CTCQueryScenario implements QueryScenario {
    
    private String resultMode = TestProperties.RESULT_MODES.NONE;

    private QueryReader reader = null;
    private ResultsGenerator genResults = null;    
    private Map<String, Collection<TestResult>> testResults = Collections.synchronizedMap(new HashMap<String, Collection<TestResult>>());


    private Properties props;
    private String outputDir = null;
    
    private String scenario_test_name;
    
    public CTCQueryScenario(String scenarioName, Properties querySetProperties) {
	this.props = querySetProperties;
	this.scenario_test_name = scenarioName;
	setup();
    }
    


    public String getQueryScenarioIdentifier() {
	return this.scenario_test_name;
    }


    private void setup() {
	
	TestLogger.logDebug("Perform TestClient Setup");
	Properties props = ConfigPropertyLoader.getInstance().getProperties();
	
	outputDir = props.getProperty(TestProperties.PROP_OUTPUT_DIR, ".");

	Assert.assertNotNull("Property " + TestProperties.PROP_OUTPUT_DIR
		+ " was not specified", outputDir);
	
	outputDir = outputDir + "/" + scenario_test_name;

	
	try {
	    
	    
	    validateResultsMode(props);
	    
	    setupVDBs(props);
	    
	    reader = new XMLQueryReader(props);
	} catch (Exception e) {
	    // TODO Auto-generated catch block
	    throw new TransactionRuntimeException(e.getMessage());
	}
	
       genResults = new XMLGenerateResults(this.props, scenario_test_name, outputDir);


	if (reader.getQuerySetIDs() == null || reader.getQuerySetIDs().isEmpty()) {
	    throw new TransactionRuntimeException("No query set ID's were returned");
	}

    }
    
    private void setupVDBs(Properties props) throws IOException {
	// NOTE: this is probably a hack, because the only way I could get all
	// the vdb's available when running multiple scenarions
	// was to update the deploy.properties by setting the vdb.definition
	// property containing the location of
	// all the vdbs

	String deployPropLoc = props.getProperty("deployprops.loc");
	Properties deployProperties = PropertiesUtils.load(deployPropLoc);

	// set the vdb.definition property that contains all the vdbs
	String vdb_loc = props.getProperty("vdb.loc");
	File vdbfiles[] = FileUtils.findAllFilesInDirectoryHavingExtension(vdb_loc, ".vdb");
	if (vdbfiles == null || vdbfiles.length == 0) {
	    throw new TransactionRuntimeException((new StringBuilder()).append(
		    "No vdbs found in directory ").append(vdb_loc).toString());
	}
	StringBuffer vdbdef = new StringBuffer();

	for (int i = 0; i < vdbfiles.length; i++) {
	    vdbdef.append(vdbfiles[i].getAbsolutePath() + ";");
	}

	deployProperties.setProperty("vdb.definition", vdbdef.toString());
	PropertiesUtils.print(deployPropLoc, deployProperties,"Updated for vdb.definition");

    }
    


    private void validateResultsMode(Properties props) {
	// Determine from property what to do with query results
	String resultModeStr = props.getProperty(TestProperties.PROP_RESULT_MODE, "");
	// No need to check for null prop here since we've just checked for this
	// required property

	if (resultModeStr.equalsIgnoreCase(TestProperties.RESULT_MODES.NONE) ||
		resultModeStr.equalsIgnoreCase(TestProperties.RESULT_MODES.COMPARE) ||
		resultModeStr.equalsIgnoreCase(TestProperties.RESULT_MODES.GENERATE)) { //$NON-NLS-1$
	    resultMode = resultModeStr;
	} 
	// otherwise use default of NONE

	TestLogger.log("\nResults mode: " + resultMode); //$NON-NLS-1$

    }
    
    public Properties getProperties() {
	return this.props;
    }
    
    public Map<String, Object>  getQueries(String querySetID) {
	try {
	    return reader.getQueries(querySetID);
	} catch (QueryTestFailedException e) {
		throw new TransactionRuntimeException(e);
	}
    }
    
    public Collection<String> getQuerySetIDs() {
	return reader.getQuerySetIDs();
    }
    
    public String getResultsMode() {
	return this.resultMode;
    }
    
    public ExpectedResults getExpectedResults(String querySetID) {    
	XMLExpectedResults expectedResults = new XMLExpectedResults(this.props, querySetID);
	return expectedResults;
    }
    
    
    public ResultsGenerator getResultsGenerator() {
	return this.genResults;
    }



    @Override
    public String getOutputDirectory() {
	return outputDir;
    }



    @Override
    public synchronized void addTestResult(String querySetID, TestResult result) {
	Collection<TestResult> results = null;
	if (this.testResults.containsKey(querySetID)) {
	    results = this.testResults.get(querySetID);
	} else {
	    results = new ArrayList<TestResult>();
	    this.testResults.put(querySetID, results);
	}
	results.add(result);
	
    }



    @Override
    public Collection<TestResult> getTestResults(String querySetID) {
	return this.testResults.get(querySetID);
    }



}
