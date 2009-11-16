/*
 * Copyright (c) 2000-2007 MetaMatrix, Inc.
 * All rights reserved.
 */
package org.teiid.test.framework.datasource;

import java.io.File;
import java.io.FileFilter;
import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import org.teiid.test.framework.exception.QueryTestFailedException;
import org.teiid.test.framework.exception.TransactionRuntimeException;

/**
 * The DataSourceMgr is responsible for loading and managing the datasources
 * defined by the datasource connection properties file. There's only a need to
 * load the set of available datasources once for the duration of the entire
 * test suite. And it will maintain one {@link DataSource} for each
 * connection.properties file that it finds.
 * 
 * @author vanhalbert
 * 
 */
public class DataSourceMgr {

    private static DataSourceMgr _instance = null;

    private Map<String, DataSource> allDatasourcesMap = new HashMap<String, DataSource>(); // key
											   // =
											   // datasource
											   // name

    // map of the datasources assigned to with model
    // because only one VDB (Transactions) is used, then this mapping can live
    // for the
    // duration of all tests
    private Map<String, DataSource> modelToDatasourceMap = new HashMap<String, DataSource>(); // key
											      // =
											      // modelname

    private DataSourceMgr() {
    }

    public static synchronized DataSourceMgr getInstance() {
	if (_instance == null) {
	    _instance = new DataSourceMgr();
	    try {
		_instance.loadDataSourceMappings();
	    } catch (QueryTestFailedException e) {
		throw new TransactionRuntimeException(e);
	    } catch (TransactionRuntimeException e) {
		throw e;
	    }

	}
	return _instance;
    }

    public Map<String, DataSource> getDataSources() {
	Map<String, DataSource> ds = new HashMap<String, DataSource>(
		allDatasourcesMap.size());
	ds.putAll(allDatasourcesMap);
	return ds;
    }

    public int numberOfAvailDataSources() {
	return allDatasourcesMap.size();
    }

    public DataSource getDataSource(String modelname) {
	if (modelToDatasourceMap.containsKey(modelname)) {
	    return modelToDatasourceMap.get(modelname);
	}
	return null;
    }

    public void setDataSource(String modelName, DataSource ds) {
	modelToDatasourceMap.put(modelName, ds);
    }

    private void loadDataSourceMappings() throws QueryTestFailedException {

	File[] dirs = findAllChildDirectories("./target/classes/datasources/");
	if (dirs == null || dirs.length == 0) {
	    throw new TransactionRuntimeException(
		    "No datasource directories found at location "
			    + "./target/classes/datasources/");
	}
	for (int i = 0; i < dirs.length; i++) {
	    File d = dirs[i];

	    String dname = d.getName();

	    addDataSource(dname, d.getName(), allDatasourcesMap);

	}

	if (allDatasourcesMap == null || allDatasourcesMap.isEmpty()) {
	    throw new TransactionRuntimeException(
		    "Error: No Datasources were loaded.");
	}

	System.out.println("Number of total datasource mappings loaded "
		+ allDatasourcesMap.size());

    }

    /**
     * Returns a <code>File</code> array that will contain all the directories
     * that exist in the directory
     * 
     * @return File[] of directories in the directory
     */
    private static File[] findAllChildDirectories(String dir) {

	// Find all files in the specified directory
	File mfile = new File(dir);

	File modelsDirFile = null;
	try {
	    modelsDirFile = new File(mfile.getCanonicalPath());
	} catch (IOException e) {
	    // TODO Auto-generated catch block
	    e.printStackTrace();
	}
	if (!modelsDirFile.exists()) {
	    return null;
	}
	FileFilter fileFilter = new FileFilter() {

	    public boolean accept(File file) {
		if (file.isDirectory()) {
		    return true;
		}

		return false;
	    }
	};

	File[] modelFiles = modelsDirFile.listFiles(fileFilter);

	return modelFiles;

    }

    private void addDataSource(String dirname, String dirloc,
	    Map<String, DataSource> datasources) {

	String dsfile = "/datasources/" + dirloc + "/connection.properties";
	Properties dsprops = loadProperties(dsfile);

	if (dsprops != null) {

	    DataSource ds = new DataSource(dirname, "dsgroup", dsprops);
	    datasources.put(ds.getName(), ds);
	    System.out.println("Loaded datasource " + ds.getName());

	}

    }

    private static Properties loadProperties(String filename) {
	Properties props = null;

	try {
	    InputStream in = DataSourceMgr.class.getResourceAsStream(filename);
	    if (in != null) {
		props = new Properties();
		props.load(in);
		return props;
	    }
	    return null;
	} catch (IOException e) {
	    throw new TransactionRuntimeException(
		    "Error loading properties from file '" + filename + "'"
			    + e.getMessage());
	}
    }

}
