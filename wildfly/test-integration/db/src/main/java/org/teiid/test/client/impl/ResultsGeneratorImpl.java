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

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.sql.ResultSet;
import java.text.SimpleDateFormat;
import java.util.List;
import java.util.Properties;

import org.junit.Assert;
import org.teiid.core.util.FileUtils;
import org.teiid.test.client.ResultsGenerator;
import org.teiid.test.client.TestProperties;
import org.teiid.test.framework.exception.QueryTestFailedException;
import org.teiid.test.util.TestResultSetUtil;

@SuppressWarnings("nls")
public class ResultsGeneratorImpl implements ResultsGenerator {
    private static final SimpleDateFormat FILE_NAME_DATE_FORMATER = new SimpleDateFormat(
	    "yyyyMMdd_HHmmss"); //$NON-NLS-1$
    private String outputDir = "";
    private String generateDir = "";
    
    private static final int MAX_COL_WIDTH = 65;
	

    public ResultsGeneratorImpl( String testname, Properties props) {

	outputDir = props.getProperty(TestProperties.PROP_OUTPUT_DIR, ".");

	Assert.assertNotNull("Property " + TestProperties.PROP_OUTPUT_DIR
		+ " was not specified", outputDir);

	outputDir = outputDir + "/" + testname;

	
	generateDir = props.getProperty(PROP_GENERATE_DIR, ".");
	Assert.assertNotNull("Property " + PROP_GENERATE_DIR
		+ " was not specified", this.generateDir);
	

	File d = new File(this.outputDir);
	this.outputDir = d.getAbsolutePath();
	d = new File(this.outputDir);
	if (d.exists()) {
	    FileUtils.removeDirectoryAndChildren(d);

	}
	if (!d.exists()) {
	    d.mkdirs();
	}


	d = new File(generateDir, testname);
	generateDir = d.getAbsolutePath();
	d = new File(generateDir);
	if (d.exists()) {
	    FileUtils.removeDirectoryAndChildren(d);
	}
	if (!d.exists()) {
	    d.mkdirs();
	}

    }
    

    @Override
    public String getGenerateDir() {
	// TODO Auto-generated method stub
	return this.generateDir;
    }

    @Override
    public String getOutputDir() {
	// TODO Auto-generated method stub
	return outputDir;
    }

    /**
     * Generate query results. These are actual results from the server and may
     * be used for comparing to results from a later test run.
     * 
     * @param queryID
     * @param resultsFile
     * @param result
     * @param ex
     * @throws QueryTestFailedException
     */

    public void generateQueryResultFile(String querySetID, String queryID,
	    String query, ResultSet result, Throwable ex, int testStatus)
	    throws QueryTestFailedException {
	
	File fos = createNewResultsFile(queryID, querySetID,
		getGenerateDir());
	
	
	FileOutputStream actualOut = null;
	try {
	    actualOut = new FileOutputStream(fos);
	    PrintStream filePrintStream = new PrintStream(actualOut);
	    if (ex != null) {
		TestResultSetUtil.printThrowable(ex, query, filePrintStream);
	    } else if (result != null ){
		result.beforeFirst();
		TestResultSetUtil.printResultSet(result, query, MAX_COL_WIDTH, true, filePrintStream);
	    }

	} catch (Exception e) {
	    e.printStackTrace();
	    throw new QueryTestFailedException(e);
	} finally {
	    if (actualOut != null) {
		try {
		    actualOut.close();
		} catch (IOException e) {
		    // TODO Auto-generated catch block
		    e.printStackTrace();
		}
	    }
	}
        
    }

    public String generateErrorFile(final String querySetID,
	    final String queryID, final String sql, final ResultSet resultSet,
	    final Throwable queryError, final Object results)
	    throws QueryTestFailedException {

	String errorFileName = null;
	try {
	    // write actual results to error file
	    errorFileName = generateErrorFileName(queryID, querySetID);
	    // configID, queryID, Integer.toString(clientID));
	    //           CombinedTestClient.log("\t" + this.clientID + ": Writing error file with actual results: " + errorFileName); //$NON-NLS-1$ //$NON-NLS-2$
	    File errorFile = new File(getOutputDir(), errorFileName);
	    
	    // the resultset will be passed in as null when
	    // the error was due to a thrown exception, and not based comparison issues 
	    if (resultSet == null) {
		FileOutputStream actualOut = null;
		try {
		    actualOut = new FileOutputStream(errorFile);
		    PrintStream filePrintStream = new PrintStream(actualOut);
		    

		    TestResultSetUtil.printThrowable(queryError, sql, filePrintStream);
		    
		    filePrintStream.flush();

		} catch (Exception e) {
			    e.printStackTrace();
			    throw new QueryTestFailedException(e);
		} finally {
		    	if (actualOut != null) {
				try {
				    actualOut.close();
				} catch (IOException e) {
				    // TODO Auto-generated catch block
				    e.printStackTrace();
				}
			}
		}
		return errorFileName;

	    }

	    // rewind resultset

	    resultSet.beforeFirst();

	    generateErrorResults(querySetID, queryID, sql, errorFile,
		    resultSet, (results != null ? (List) results : null));

	} catch (Throwable e) {
	    throw new QueryTestFailedException(e.getMessage());
	    //           CombinedTestClient.logError("Error writing error file \"" + outputDir + "\"/" + errorFileName + ": " + e); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
	}
	return errorFileName;
    }

    private File createNewResultsFile(String queryID, String querySetID,
	    String genDir) {
	String resultFileName = queryID + ".txt"; //$NON-NLS-1$

	String targetDirname = genDir + File.separator + querySetID; //$NON-NLS-1$
	File targetDir = new File(targetDirname);
	targetDir.mkdirs();

	return new File(targetDir, resultFileName);
    }


    private String generateErrorFileName(String queryID, String querySetID) {
//	String errorFileName = "ERROR_"
//		// configID + "_" //$NON-NLS-1$ //$NON-NLS-2$
//		//                               + querySetID + "_" //$NON-NLS-1$
//		+ queryID
//		+ "_" //$NON-NLS-1$
//		+ FILE_NAME_DATE_FORMATER.format(new Date(System
//			.currentTimeMillis())) + ".txt"; //$NON-NLS-1$
//	return errorFileName;
	
	return  queryID + ".txt";

    }

    /**
     * Generate an error file for a query that failed comparison. File should
     * have the SQL, the actual results returned from the server and the results
     * that were expected.
     * 
     * @param queryID
     * @param sql
     * @param resultsFile
     * @param actualResult
     * @param expectedResultFile
     * @param ex
     * @throws QueryTestFailedException
     */
    private void generateErrorResults(String querySetID, String queryID,
	    String sql, File resultsFile, ResultSet actualResult,
	    List<String> results)
	    throws QueryTestFailedException {
	
	FileOutputStream actualOut = null;
	try {
	    actualOut = new FileOutputStream(resultsFile);
	    PrintStream filePrintStream = new PrintStream(actualOut);
	    
	    TestResultSetUtil.printResultSet(actualResult, sql, MAX_COL_WIDTH, true, filePrintStream);

//	    if (results != null) {
//        	    for (Iterator<String> it=results.iterator(); it.hasNext();) {
//        		String line = it.next();
//        		filePrintStream.print(line);
//        	    }
//	    } else {
//	    
//		ResultSetUtil.printResultSet(actualResult, MAX_COL_WIDTH, true, filePrintStream);
//	    }
	    	    

	} catch (Exception e) {
	    e.printStackTrace();
	    throw new QueryTestFailedException(e);
	} finally {
	    if (actualOut != null) {
		try {
		    actualOut.close();
		} catch (IOException e) {
		    // TODO Auto-generated catch block
		    e.printStackTrace();
		}
	    }
	}
    }

}
