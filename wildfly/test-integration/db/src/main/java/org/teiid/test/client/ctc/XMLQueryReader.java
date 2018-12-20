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

package org.teiid.test.client.ctc;

import java.io.File;
import java.io.FileFilter;
import java.io.IOException;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.teiid.core.util.StringUtil;
import org.teiid.test.client.QueryReader;
import org.teiid.test.client.QueryTest;
import org.teiid.test.framework.ConfigPropertyLoader;
import org.teiid.test.framework.ConfigPropertyNames;
import org.teiid.test.framework.TestLogger;
import org.teiid.test.framework.exception.QueryTestFailedException;
import org.teiid.test.framework.exception.TransactionRuntimeException;

@SuppressWarnings("nls")
public class XMLQueryReader implements QueryReader {
 
    private Properties props = null;
    private String queryScenarioIdentifier;

    private Map<String, String> querySetIDToFileMap = new HashMap<String, String>();

    public XMLQueryReader(String queryScenarioID, Properties properties)
	    throws QueryTestFailedException {
	this.props = properties;
	this.queryScenarioIdentifier = queryScenarioID;
	loadQuerySets();
    }

    @Override
    public List<QueryTest> getQueries(String querySetID)
	    throws QueryTestFailedException {
	String queryFile = querySetIDToFileMap.get(querySetID);

	try {
	    return loadQueries(querySetID, queryFile);
	} catch (IOException e) {
	    throw new QueryTestFailedException((new StringBuilder()).append(
		    "Failed to load queries from file: ").append(queryFile).append(" error:").append(e.getMessage())
		    .toString());
	}

    }

    @Override
    public Collection<String> getQuerySetIDs() {
	return new HashSet<String>(querySetIDToFileMap.keySet());
    }


    private void loadQuerySets() throws QueryTestFailedException {
	String query_dir_loc = this.props.getProperty(PROP_QUERY_FILES_DIR_LOC);
	if (query_dir_loc == null)
	    throw new QueryTestFailedException(
		    "queryfiles.loc property was not specified ");

	String query_root_loc = this.props
		.getProperty(PROP_QUERY_FILES_ROOT_DIR);

	String loc = query_dir_loc;

	if (query_root_loc != null) {
	    File dir = new File(query_root_loc, query_dir_loc);
	    loc = dir.getAbsolutePath();
	}

	TestLogger.log("Loading queries from " + loc);

	File files[] = findAllFilesInDirectoryHavingExtension(loc,
		".xml");
	if (files == null || files.length == 0)
	    throw new QueryTestFailedException((new StringBuilder()).append(
		    "No query files found in directory ").append(loc)
		    .toString());
	// List<String> queryFiles = new ArrayList<String>(files.length);
	for (int i = 0; i < files.length; i++) {
	    String queryfile = files[i].getAbsolutePath();
	    // Get query set name
	    String querySet = getQuerySetName(queryfile); //$NON-NLS-1$
	    querySetIDToFileMap.put(querySet, queryfile);
	    // queryFiles.add(files[i].getAbsolutePath());
	}

    }

    private List <QueryTest> loadQueries(String querySetID, String queryFileName)
	    throws IOException {

//	Map<String, Object> queries = new HashMap<String, Object>();
	File queryFile = new File(queryFileName);
	if (!queryFile.exists() || !queryFile.canRead()) {
	    String msg = "Query file doesn't exist or cannot be read: " + queryFileName + ", ignoring and continuing";
	    TestLogger.log(msg);
	    throw new TransactionRuntimeException(msg); //$NON-NLS-1$ //$NON-NLS-2$
	} 
	    // Get query set name
	    //			String querySet = getQuerySetName(queryFileName) ; //$NON-NLS-1$

	    XMLQueryVisitationStrategy jstrat = new XMLQueryVisitationStrategy();
	    try {
		return jstrat.parseXMLQueryFile(this.queryScenarioIdentifier, queryFile, querySetID);
//		Iterator iter = queryMap.keySet().iterator();
//		while (iter.hasNext()) {
//		    String queryID = (String) iter.next();
//		    String query = (String) queryMap.get(queryID);
//
//		    String uniqueID = querySetID + "_" + queryID;
//		    queries.put(uniqueID, query);
//		}

	    } catch (Exception e) {
		String msg = "Error reading query file: " + queryFileName + ", " + e.getMessage(); //$NON-NLS-1$ //$NON-NLS-2$
		TestLogger.log(msg);
		throw new IOException(msg); //$NON-NLS-1$ //$NON-NLS-2$
	    }

//	return queries;
    }

    private static String getQuerySetName(String queryFileName) {
	// Get query set name
	String querySet = queryFileName;
	List<String> nameParts = StringUtil.split(querySet, "./\\"); //$NON-NLS-1$
	if (nameParts.size() > 1) {
	    querySet = nameParts.get(nameParts.size() - 2);
	}
	return querySet;
    }

    public static void main(String[] args) {
	System.setProperty(ConfigPropertyNames.CONFIG_FILE,
		"ctc-bqt-test.properties");

	ConfigPropertyLoader _instance = ConfigPropertyLoader.getInstance();
	Properties p = _instance.getProperties();
	if (p == null || p.isEmpty()) {
	    throw new RuntimeException("Failed to load config properties file");

	}

	_instance.setProperty(PROP_QUERY_FILES_ROOT_DIR, new File(
		"src/main/resources/").getAbsolutePath());

	try {
	    XMLQueryReader reader = new XMLQueryReader("scenario_id",  _instance.getProperties());
	    Iterator<String> it = reader.getQuerySetIDs().iterator();
	    while (it.hasNext()) {
		String querySetID = it.next();

		List<QueryTest> queries = reader.getQueries(querySetID);
		
		if (queries.size() == 0l) {
		    System.out.println("Failed, didn't load any queries ");
		}
	    }
	} catch (QueryTestFailedException e) {
	    // TODO Auto-generated catch block
	    e.printStackTrace();
	}

    }
    
    /**
     * Returns a <code>File</code> array that will contain all the files that
     * exist in the directory that have the specified extension.
     * @return File[] of files having a certain extension
     */
    public static File[] findAllFilesInDirectoryHavingExtension(String dir, final String extension) {

      // Find all files in that directory that end in XML and attempt to
      // load them into the runtime metadata database.
      File modelsDirFile = new File(dir);
      FileFilter fileFilter = new FileFilter() {
          public boolean accept(File file) {
              if(file.isDirectory()) {
                  return false;
              }


              String fileName = file.getName();

              if (fileName==null || fileName.length()==0) {
                  return false;
              }

              // here we check to see if the file is an .xml file...
              int index = fileName.lastIndexOf("."); //$NON-NLS-1$

              if (index<0 || index==fileName.length()) {
                  return false;
              }

              if (fileName.substring(index, fileName.length()).equalsIgnoreCase(extension)) {
                  return true;
              }
              return false;
          }
      };

      File[] modelFiles = modelsDirFile.listFiles(fileFilter);

      return modelFiles;

  }

}
